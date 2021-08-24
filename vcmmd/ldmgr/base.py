# Copyright (c) 2016-2017, Parallels International GmbH
# Copyright (c) 2017-2021, Virtuozzo International GmbH, All rights reserved
#
# This file is part of OpenVZ. OpenVZ is free software; you can redistribute
# it and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the License,
# or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#
# Our contact details: Virtuozzo International GmbH, Vordergasse 59, 8200
# Schaffhausen, Switzerland.

import importlib
import itertools
import functools
import logging
import os
import threading
import types
import psutil
import xml.etree.ElementTree as ET

from vcmmd.error import (VCMMDError,
                         VCMMD_ERROR_VE_NAME_ALREADY_IN_USE,
                         VCMMD_ERROR_VE_NOT_REGISTERED,
                         VCMMD_ERROR_UNABLE_APPLY_VE_GUARANTEE,
                         VCMMD_ERROR_VE_NOT_ACTIVE,
                         VCMMD_ERROR_POLICY_SET_ACTIVE_VES,
                         VCMMD_ERROR_POLICY_SET_INVALID_NAME)
from vcmmd.ve_config import DefaultVEConfig, VEConfig, VCMMD_MEMGUARANTEE_AUTO
from vcmmd.ve_type import (VE_TYPE_CT, VE_TYPE_VM, VE_TYPE_VM_LINUX,
                           VE_TYPE_VM_WINDOWS, VE_TYPE_SERVICE)
from vcmmd.ldmgr.policy import clamp
from vcmmd.util.libvirt import get_qemu_proxy, get_vzct_proxy
from vcmmd.config import VCMMDConfig
from vcmmd.ve import VE
from vcmmd.ve.ct import lookup_cgroup
from vcmmd.host import Host
from vcmmd.cgroup.memory import MemoryCgroup


# Dummy policy may be used in purpose of debugging other
# Virtuzzo components. While running this policy, VCMMD returns
# sucessful result codes for any requests and doesn't keep state of VE's
# on the node.
DUMMY_POLICY = types.SimpleNamespace(name='dummy')


def _dummy_pass(func=None, *, return_value=None):
    def decorator_dummy_pass(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if args[0]._policy == DUMMY_POLICY:
                return return_value
            return func(*args, **kwargs)
        return wrapper
    return decorator_dummy_pass if not func else decorator_dummy_pass(func)


def _fix_vstorage_memory_issues(service_path):
    # PSBM-64263
    # PSBM-89802
    fixes = {
        os.path.join(service_path, 'memory.disable_cleancache'): '1',
        os.path.join(service_path, 'memory.swappiness'): '0',
    }
    for path, data in fixes.items():
        with open(path, 'w') as fp:
            fp.write(data)


DEFAULT_GUARANTEE = {
    VE_TYPE_CT: 0,
    VE_TYPE_VM: 40
}


class LoadManager:

    FALLBACK_POLICY = 'NoOpPolicy'

    def __init__(self):
        self.logger = logging.getLogger('vcmmd.ldmgr')
        self._registered_ves = {}  # str -> VE
        self._registered_ves_lock = threading.Lock()
        self.alias = None
        self.cfg = VCMMDConfig()
        self._host = Host()
        policy_name = self.cfg.get_str(
                'LoadManager.Policy', self.FALLBACK_POLICY)
        policy_name = self._load_alias(policy_name)
        self._load_policy(policy_name)
        self._initialize_services()
        self._initialize_ves()

    def _set_user_cache_limit(self):
        total_mem = psutil.virtual_memory().total
        cache_limit = self.cfg.get_num(
                'LoadManager.UserCacheLimitTotal',
                min(total_mem // 10, 10 * (1 << 30)))
        if not self.cfg.get_bool('EnableUserCacheLimits', True):
            cache_limit = total_mem
        try:
            MemoryCgroup('user.slice').write_cache_limit_in_bytes(cache_limit)
        except IOError as e:
            self.logger.error('Can\'t update user.slice: %s', e)

    def switch_policy(self, policy_name):
        if self.alias is not None and policy_name not in self.alias:
            raise VCMMDError(VCMMD_ERROR_POLICY_SET_INVALID_NAME)
        self.cfg.dump('LoadManager.Policy', policy_name)
        policy_name = self._load_alias(policy_name)
        with self._registered_ves_lock:
            for ve_name in self._registered_ves:
                ve = self._registered_ves.get(ve_name)
                if ve.VE_TYPE != VE_TYPE_SERVICE:
                    raise VCMMDError(VCMMD_ERROR_POLICY_SET_ACTIVE_VES)
            self.shutdown()
            # Load a policy
            self._load_policy(policy_name)
            for ve_name in self._registered_ves:
                ve = self._registered_ves.get(ve_name)
                self._policy.ve_registered(ve)
                if ve.active:
                    self._policy.ve_activated(ve)

    def _load_alias(self, policy_name):
        try:
            alias = importlib.import_module('vcmmd.ldmgr.policies.alias')
        except ImportError as err:
            return policy_name

        self.alias = alias.alias
        return self.alias.get(policy_name, policy_name)

    def _load_policy(self, policy_name):
        if policy_name == DUMMY_POLICY.name:
            self._policy = DUMMY_POLICY
            self.logger.info("Loaded %s policy", DUMMY_POLICY.name)
            return
        try:
            policy_module = importlib.import_module(
                'vcmmd.ldmgr.policies.' + policy_name)
        except ImportError as err:
            self.logger.error("Failed to load policy '%s': %s",
                              policy_name, err)
            # fallback on default policy
            policy_name = self.FALLBACK_POLICY
            policy_module = importlib.import_module(
                'vcmmd.ldmgr.policies.' + policy_name)
        self._policy = getattr(policy_module, policy_name)()
        self._policy.load()
        self._set_user_cache_limit()
        self.logger.info("Loaded policy '%s'", policy_name)

    @_dummy_pass
    def shutdown(self):
        self._policy.shutdown()

    def _check_guarantees(self, delta):
        mem_min = sum(ve.mem_min for ve in self._registered_ves.values())
        mem_min += delta
        if mem_min > self._host.ve_mem:
            raise VCMMDError(VCMMD_ERROR_UNABLE_APPLY_VE_GUARANTEE)

    @_dummy_pass
    def register_ve(self, ve_name, ve_type, ve_config):
        with self._registered_ves_lock:
            if ve_name in self._registered_ves:
                raise VCMMDError(VCMMD_ERROR_VE_NAME_ALREADY_IN_USE)

            ve_config.complete(DefaultVEConfig)
            if ve_type not in (VE_TYPE_CT, VE_TYPE_SERVICE) and \
               ve_config.guarantee_type == VCMMD_MEMGUARANTEE_AUTO:
                ve_config.update(guarantee=int(
                    ve_config.limit * self._policy.DEFAULT_VM_AUTO_GUARANTEE))
            ve = VE(ve_type, ve_name, ve_config)
            self._check_guarantees(ve.mem_min)
            ve.effective_limit = min(ve.config.limit, self._host.ve_mem)

            self._registered_ves[ve_name] = ve
            self._policy.ve_registered(ve)

            self.logger.info('Registered %s (%s)', ve, ve.config)

    @_dummy_pass
    def activate_ve(self, ve_name):
        with self._registered_ves_lock:
            ve = self._registered_ves.get(ve_name)
            if ve is None:
                raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

            ve.activate()
            self._policy.ve_activated(ve)

    @_dummy_pass
    def update_ve_config(self, ve_name, ve_config):
        with self._registered_ves_lock:
            ve = self._registered_ves.get(ve_name)
            if ve is None:
                raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)
            if not ve.active:
                raise VCMMDError(VCMMD_ERROR_VE_NOT_ACTIVE)

            if ve.VE_TYPE in (
                    VE_TYPE_VM, VE_TYPE_VM_LINUX, VE_TYPE_VM_WINDOWS):
                ve._get_obj()._update_cgroups()

            ve_config.complete(ve.config)
            if ve.VE_TYPE not in (VE_TYPE_CT, VE_TYPE_SERVICE) and \
               ve_config.guarantee_type == VCMMD_MEMGUARANTEE_AUTO:
                ve_config.update(guarantee=int(
                    ve_config.limit * self._policy.DEFAULT_VM_AUTO_GUARANTEE))
            self._check_guarantees(ve_config.mem_min - ve.config.mem_min)

            try:
                ve.set_config(ve_config)
            except VCMMDError:
                pass
            ve.effective_limit = min(ve.config.limit, self._host.ve_mem)

            self._policy.ve_config_updated(ve)

    @_dummy_pass
    def deactivate_ve(self, ve_name):
        with self._registered_ves_lock:
            ve = self._registered_ves.get(ve_name)
            if ve is None:
                raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

            ve.deactivate()
            self._policy.ve_deactivated(ve)

    @_dummy_pass
    def unregister_ve(self, ve_name):
        with self._registered_ves_lock:
            ve = self._registered_ves.get(ve_name)
            if ve is None:
                raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

            del self._registered_ves[ve.name]

            self._policy.ve_unregistered(ve)
            if ve.active:
                self._policy.ve_deactivated(ve)
                self.logger.info('Unregistered %s', ve)

    @_dummy_pass(return_value=True)
    def is_ve_active(self, ve_name):
        try:
            with self._registered_ves_lock:
                return self._registered_ves[ve_name].active
        except KeyError:
            raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

    @_dummy_pass(return_value=[])
    def get_ve_config(self, ve_name):
        with self._registered_ves_lock:
            try:
                return self._registered_ves[ve_name].config.as_array()
            except KeyError:
                raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

    @_dummy_pass(return_value=[])
    def get_all_registered_ves(self):
        result = []
        with self._registered_ves_lock:
            for ve in self._registered_ves.values():
                result.append((ve.name, ve.VE_TYPE, ve.active,
                               ve.config.as_array()))
        return result

    @_dummy_pass(return_value=DUMMY_POLICY.name)
    def get_current_policy(self):
        cfg = self.cfg.get_str('LoadManager.Policy')
        cur = self._policy.get_name()
        if self._load_alias(cfg) == cur:
            return cfg
        else:
            return cur

    def get_policy_from_file(self):
        cfg = self.cfg.read()
        return cfg and cfg.get('LoadManager', {}).get('Policy', '') or ''

    @_dummy_pass(return_value=[])
    def get_stats(self, ve_name):
        with self._registered_ves_lock:
            ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)
        res = list(ve.stats.report().items())
        return res

    @_dummy_pass(return_value={})
    def get_free(self):
        with self._registered_ves_lock:
            qemu_vram_overhead = 0
            guarantee = 0
            for ve in self._registered_ves.values():
                qemu_vram_overhead += ve.mem_overhead
                if ve.protection:
                    guarantee += ve.protection
        swap = self._host.get_slice_swap('machine')
        if swap is None:
            swap = 0
        available = max(
                self._host.total_mem - qemu_vram_overhead - guarantee, 0)
        return {'total': self._host.total_mem,
                'qemu overhead+vram': qemu_vram_overhead,
                'guarantee': guarantee,
                'swap': swap,
                'available': available}

    def get_config(self, full_config=False):
        return VCMMDConfig().report(full_config)

    @_dummy_pass(return_value='{}')
    def get_policy_counts(self):
        return self._policy.report()

    def _initialize_services(self):
        for name, config in VCMMDConfig().get('Limits', default={}).items():
            self._initialize_service(name, config)

    def _initialize_service(self, name, config):
        known_params = {'Limit', 'Guarantee', 'Swap', 'Path'}
        total_mem = psutil.virtual_memory().total
        if 'Path' not in config and name == 'VStorage':
            config['Path'] = 'vstorage.slice/vstorage-services.slice'
            self.logger.info('Assuming that VStorage is located at %s',
                             config['Path'])
        service_name = config['Path']
        service_path = '/sys/fs/cgroup/memory/{}'.format(service_name)
        if not os.path.isdir(service_path):
            self.logger.error('Memory cgroup %s not found', service_path)
            return
        if name == 'VStorage':
            _fix_vstorage_memory_issues(service_path)
        # read config for limit, guarantee and swap
        ve_config = {}
        unknown_params = set(config.keys()) - known_params
        if unknown_params:
            raise VCMMDError('Unknown fields in {}: {}'.format(
                service_name, unknown_params))
        for Param in known_params - {'Path'}:
            param = Param.lower()
            try:
                unknown_params = (set(config[Param].keys())
                                  - {'Share', 'Min', 'Max'})
                if unknown_params:
                    raise VCMMDError('Unknown fields in {}: {}'.format(
                        service_name, unknown_params))
                ve_config[param] = clamp(int(config[Param]['Share'] * total_mem),
                                         int(config[Param].get('Min', 0)),
                                         int(config[Param].get('Max', -1)))
            except (KeyError, TypeError, ValueError):
                raise VCMMDError('Error parsing {}.{}'.format(service_name, Param))
        self.register_ve(service_name, VE_TYPE_SERVICE, VEConfig(**ve_config))
        self.activate_ve(service_name)

    def _initialize_ves(self):
        cts = get_vzct_proxy().listAllDomains(0)
        vms = get_qemu_proxy().listAllDomains(0)
        for domain in itertools.chain(cts, vms):
            if domain.isActive():
                self._initialize_ve(domain)

    def _initialize_ve(self, domain):
        ve_type = VE_TYPE_CT if domain.OSType() == 'exe' else VE_TYPE_VM
        uuid = domain.UUIDString()
        ve_config = {}
        dom_xml = ET.fromstring(domain.XMLDesc())
        if ve_type == VE_TYPE_VM:
            ve_config['limit'] = domain.maxMemory() << 10
            video = dom_xml.findall('./devices/video/model')
            vram = sum(int(v.attrib.get('vram', 0)) for v in video) << 10
            ve_config['vram'] = vram
        else:
            memcg = lookup_cgroup(MemoryCgroup, uuid)
            ve_config['limit'] = memcg.read_mem_max()
            ve_config['swap'] = memcg.read_swap_max()
        vcpu_dom = dom_xml.find('./vcpu')
        if vcpu_dom:
            ve_config['cpulist'] = vcpu_dom.attrib.get('cpuset', '')
        numa_memory_dom = dom_xml.find('./numatune/memory')
        if numa_memory_dom:
            ve_config['nodelist'] = numa_memory_dom.attrib.get('nodeset', '')
        guarantee_pct = DEFAULT_GUARANTEE[ve_type]
        guarantee_dom = dom_xml.find('./memtune/min_guarantee')
        guarantee_auto = False
        if guarantee_dom:
            guarantee_auto = guarantee_dom.attrib.get('vz-auto', False)
            if guarantee_auto != 'yes':
                guarantee_pct = int(guarantee_dom.text)
        ve_config['guarantee'] = ve_config['limit'] * guarantee_pct // 100
        ve_config['guarantee_type'] = guarantee_auto != 'yes'
        self.register_ve(uuid, ve_type, VEConfig(**ve_config))
        self.activate_ve(uuid)
