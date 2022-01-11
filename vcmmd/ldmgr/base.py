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
                         VCMMD_ERROR_VE_OPERATION_FAILED)
from vcmmd.ve_config import DefaultVEConfig, VEConfig, VCMMD_MEMGUARANTEE_AUTO
from vcmmd.ve_type import (VE_TYPE_CT, VE_TYPE_VM, VE_TYPE_VM_LINUX,
                           VE_TYPE_VM_WINDOWS, VE_TYPE_SERVICE)
from vcmmd.ldmgr.policy import clamp
from vcmmd.util.libvirt import get_qemu_proxy, get_vzct_proxy
from vcmmd.config import VCMMDConfig
from vcmmd.ve import VE
from vcmmd.ve.base import Error
from vcmmd.ve.ct import lookup_cgroup
from vcmmd.host import Host
from vcmmd.cgroup.memory import MemoryCgroup
from vcmmd.cgroup.cpu import CpuCgroup
from vcmmd.util.limits import UINT64_MAX


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
        self.cfg = VCMMDConfig()
        self._host = Host()
        self._load_policy(self.cfg.get_str('LoadManager.Policy', self.FALLBACK_POLICY))
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
        self.cfg.dump('LoadManager.Policy', policy_name)
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

    @staticmethod
    def _get_aliases():
        try:
            return importlib.import_module('vzpolicies.alias').policies
        except ImportError:
            return {}

    @_dummy_pass(return_value=DUMMY_POLICY.name)
    def get_current_policy(self):
        aliases = self._get_aliases()
        reversed_aliases = dict(zip(aliases.values(), aliases.keys()))
        policy_name = self._policy.get_name()
        return reversed_aliases.get(policy_name, policy_name)

    def _load_policy_object(self, policy_name):
        real_policy_name = self._get_aliases().get(policy_name, policy_name)
        policy_module = None
        for namespace in 'vzpolicies', 'vcmmd.ldmgr.policies':
            try:
                policy_module = importlib.import_module(namespace + '.' + real_policy_name)
                break
            except ModuleNotFoundError:
                pass  # try next namespace
            except ImportError as err:
                self.logger.error('Failed to load policy \'%s\': %s', policy_name, err)
                break
        else:
            self.logger.error('Failed to load policy \'%s\': Policy not found', policy_name)

        if policy_module is None or not self._host.check_numa_complete():
            policy_name = real_policy_name = self.FALLBACK_POLICY
            policy_module = importlib.import_module('vcmmd.ldmgr.policies.' + policy_name)

        self.logger.info("Loaded policy '%s'", policy_name)
        return getattr(policy_module, real_policy_name)()

    def _load_policy(self, policy_name):
        if policy_name == DUMMY_POLICY.name:
            self._policy = DUMMY_POLICY
            self.logger.info("Loaded %s policy", DUMMY_POLICY.name)
            return
        self._policy = self._load_policy_object(policy_name)
        self._policy.load()
        self._set_user_cache_limit()

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
            try:
                ve = VE(ve_type, ve_name, ve_config)
            except Error as err:
                if ve_type == VE_TYPE_SERVICE:
                    self.logger.info('Skip registering %s: %s', ve_name, err)
                    return
                else:
                    self.logger.error('Can\'t register %s: %s', ve_name, err)
                    raise VCMMDError(VCMMD_ERROR_VE_OPERATION_FAILED)

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

            if ve.VE_TYPE in (VE_TYPE_VM, VE_TYPE_VM_LINUX, VE_TYPE_VM_WINDOWS):
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
                self.logger.warning(f'VE \'{ve_name}\' is not registered, skipping')
                return

            ve.deactivate()
            self._policy.ve_deactivated(ve)

    @_dummy_pass
    def unregister_ve(self, ve_name):
        with self._registered_ves_lock:
            ve = self._registered_ves.get(ve_name)
            if ve is None:
                self.logger.warning(f'VE \'{ve_name}\' is not registered, skipping')
                return

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
                return self._registered_ves[ve_name].get_config().as_array()
            except KeyError:
                raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

    @_dummy_pass(return_value=[])
    def get_all_registered_ves(self):
        result = []
        with self._registered_ves_lock:
            for ve in self._registered_ves.values():
                result.append((ve.name, ve.VE_TYPE, ve.active,
                               ve.get_config().as_array()))
        return result

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
            reserved = 0
            cpu_reserved = 0
            file_cache = 0
            for ve in self._registered_ves.values():
                qemu_vram_overhead += ve.mem_overhead
                guarantee += max(ve.mem_min, ve.protection)
                if ve.VE_TYPE == VE_TYPE_SERVICE:
                    reserved += ve.mem_min
                    if ve.config.cpunum > 0:
                        cpu_reserved += ve.config.cpunum
                    if ve.config.cache < UINT64_MAX:
                        file_cache += ve.config.cache
        swap = self._host.get_slice_swap('machine')
        if swap is None:
            swap = 0
        available = max(
                self._host.total_mem - qemu_vram_overhead - guarantee, 0)
        file_cache = min(file_cache, MemoryCgroup('/').read_mem_stat()['total_active_file'])
        reserved = min(reserved + file_cache, self._host.total_mem)
        cpu_reserved = min(cpu_reserved, self._host.get_cpu_count())
        return {'total': self._host.total_mem,
                'overhead': qemu_vram_overhead,
                'host_reserved': reserved,
                'guarantee': guarantee,
                'swap_usage': swap,
                'swap_total': self._host.get_swap_total(),
                'available': available,
                'cpu_reserved': cpu_reserved}

    def get_config(self, full_config=False):
        return VCMMDConfig().report(full_config)

    @_dummy_pass(return_value='{}')
    def get_policy_counts(self):
        return self._policy.report()

    def _initialize_services(self):
        for name, config in VCMMDConfig().get('Limits', default={}).items():
            self._initialize_service(name, config)
        self._initialize_hci_services()

    def _initialize_hci_services(self):
        memory_cgroup_path = '/sys/fs/cgroup/memory'
        vstorage_cgroup_path = f'{memory_cgroup_path}/vstorage.slice'
        mem_cgroups = []
        for service_slice in (
                'vstorage-compute.slice',
                'vstorage-compute.slice/vstorage-compute-storage.slice',
                'vstorage-services.slice',
                'vstorage-target.slice',
                'vstorage-ui.slice'):
            full_path = os.path.join(vstorage_cgroup_path, service_slice)
            if not os.path.isdir(full_path):
                continue
            mem_cgroups.append(f'vstorage.slice/{service_slice}')
        for service_slice in (
                'system.slice/postgresql.service',
                'system.slice/nginx.service'):
            full_path = os.path.join(memory_cgroup_path, service_slice)
            if not os.path.isdir(full_path):
                continue
            mem_cgroups.append(service_slice)
        for cgroup_name in mem_cgroups:
            memcg = MemoryCgroup(cgroup_name)
            kv = {
                'guarantee': memcg.read_mem_low(),
                'limit': memcg.read_mem_max(),
                'cache': memcg.read_cache_limit_in_bytes()
            }
            if not memcg.read_swappiness():
                kv['swap'] = 0
            try:
                self.register_ve(cgroup_name, VE_TYPE_SERVICE, VEConfig(**kv))
                self.activate_ve(cgroup_name)
            except VCMMDError as e:
                if e.errno == VCMMD_ERROR_VE_NAME_ALREADY_IN_USE:
                    continue
                self.logger.error('Can\'t register %s: %s', cgroup_name, e)

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
        try:
            self.register_ve(service_name, VE_TYPE_SERVICE, VEConfig(**ve_config))
            self.activate_ve(service_name)
        except VCMMDError as e:
            self.logger.error('Can\'t register %s: %s', service_name, e)

    def _initialize_ves(self):
        domains = []
        for getter in get_vzct_proxy, get_qemu_proxy:
            try:
                domains += getter().listAllDomains(0)
            except LookupError as e:
                self.logger.warning(f'{getter.__name__} failed: {e}')
        for domain in domains:
            if domain.isActive():
                self._initialize_ve(domain)

    def _initialize_ve(self, domain):
        ve_type = VE_TYPE_CT if domain.OSType() == 'exe' else VE_TYPE_VM
        uuid = domain.UUIDString()
        ve_config = {'cpunum': 0}
        dom_xml = ET.fromstring(domain.XMLDesc())
        if ve_type == VE_TYPE_VM:
            ve_config['limit'] = domain.maxMemory() << 10
            ve_config['cpunum'] = domain.maxVcpus()
            video = dom_xml.findall('./devices/video/model')
            vram = sum(int(v.attrib.get('vram', 0)) for v in video) << 10
            ve_config['vram'] = vram
        else:
            memcg = lookup_cgroup(MemoryCgroup, uuid)
            cpucg = lookup_cgroup(CpuCgroup, uuid)
            ve_config['limit'] = memcg.read_mem_max()
            ve_config['swap'] = memcg.read_swap_max()
            ve_config['cpunum'] = cpucg.get_nr_cpus()
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
