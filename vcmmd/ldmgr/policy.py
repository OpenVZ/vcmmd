# Copyright (c) 2016-2017, Parallels International GmbH
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
# Our contact details: Parallels International GmbH, Vordergasse 59, 8200
# Schaffhausen, Switzerland.

import logging
import types
import os
from select import poll, POLLIN, error as  poll_error
from abc import ABCMeta, abstractmethod
from threading import Lock, Thread, Event
import ctypes
import psutil
import json

from vcmmd.host import Host
from vcmmd.config import VCMMDConfig
from vcmmd.util.misc import print_dict
from vcmmd.util.cpu_features import get_cpuinfo_features
from vcmmd.ve_type import VE_TYPE_CT, VE_TYPE_SERVICE
from vcmmd.ve_config import VEConfig
from vcmmd.cgroup import MemoryCgroup
from vcmmd.rpc.dbus.client import RPCProxy
from vcmmd.error import (VCMMDError,
                         VCMMD_ERROR_VE_NAME_ALREADY_IN_USE,
                         VCMMD_ERROR_VE_ALREADY_ACTIVE)
from vcmmd.util.limits import INT64_MAX

def clamp(v, l, h):
    if h == -1:
        h = INT64_MAX
    return max(l, min(v, h))

def eventfd(init_val, flags):
    libc = ctypes.cdll.LoadLibrary("libc.so.6")
    fd = libc.eventfd(ctypes.c_uint(init_val), ctypes.c_int(flags))
    if fd < 0:
        err = ctypes.get_errno()
        msg = os.strerror(err)
        raise OSError(err, msg)
    return fd


class Policy(object):
    '''Load manager policy interface.
    '''

    __metaclass__ = ABCMeta

    MEM_PRES_PATH = '/sys/fs/cgroup/memory/memory.pressure_level'
    EVENT_CONTR_PATH = '/sys/fs/cgroup/memory/cgroup.event_control'
    PRESSURE_LEVEL = 'medium'
    DEFAULT_VM_AUTO_GUARANTEE = 0.4

    def __init__(self):
        self.logger = logging.getLogger('vcmmd.ldmgr.policy')
        self.host = Host() # Singleton object with host related data
        self.controllers = set()
        self.low_memory_callbacks = set()
        self.__ve_data = {}  # Dictionary of all managed VEs to their policy data
        self.__ve_data_lock = Lock()
        self.counts = {}
        self.stop = Event()

    @staticmethod
    def controller(f):
        def wrapper(self, *args, **kwargs):
            sleep_timeout = 0
            while not self.stop.wait(sleep_timeout):
                sleep_timeout = f(self, *args, **kwargs)
                assert isinstance(sleep_timeout, int)
        return wrapper

    def get_name(self):
        return self.__class__.__name__

    def get_ves(self):
        with self.__ve_data_lock:
            return self.__ve_data.keys()

    def get_policy_data(self, t):
        with self.__ve_data_lock:
            return filter(lambda x: x is not None,
                          (pdata.get(t, None) for pdata in self.__ve_data.itervalues()))

    def rm_policy_data(self, t, ve):
        with self.__ve_data_lock:
            self.__ve_data.get(t, {}).pop(ve, None)

    def set_policy_data(self, ve, data):
        with self.__ve_data_lock:
            t = type(data)
            self.__ve_data[ve][t] = data

    def ve_activated(self, ve):
        '''Called right after a VE gets activated.
        '''
        with self.__ve_data_lock:
            if ve not in self.__ve_data:
                self.__ve_data[ve] = {}
        ve.set_mem(ve.config.limit, ve.mem_min)

    def ve_deactivated(self, ve):
        '''Called right after a VE gets deactivated.
        '''
        pass

    def ve_registered(self, ve):
        '''Called right after a VE gets activated.
        '''
        if ve.VE_TYPE == VE_TYPE_CT:
            ve.set_mem(ve.config.limit, ve.mem_min)

    def ve_unregistered(self, ve):
        '''Called right after a VE gets deactivated.
        '''
        pass

    def ve_config_updated(self, ve):
        '''Called right after a VE's configuration update.
        '''
        ve.set_mem(ve.config.limit, ve.mem_min)

    def load(self):
        self.controllers_threads = []
        for ctrl in self.controllers:
            self.controllers_threads.append(Thread(target=ctrl))
        if self.low_memory_callbacks:
            self.controllers_threads.append(Thread(target=self.low_memory_watchdog))

        for thread in self.controllers_threads:
            thread.start()

    def report(self, j=False):
        return print_dict(self.counts, j)

    def shutdown(self):
        self.stop.set()
        for thread in self.controllers_threads:
            thread.join()

    def low_memory_watchdog(self):
        self.counts['low_mem_events'] = 0
        efd = eventfd(0, 0)
        mp = open(self.MEM_PRES_PATH)
        with open(self.EVENT_CONTR_PATH, 'w') as cgc:
            cgc.write("%d %d %s" % (efd, mp.fileno(), self.PRESSURE_LEVEL))

        p = poll()
        p.register(efd, POLLIN)

        self.host.log_info('"Low memory" watchdog started(pressure level=%r).' % self.PRESSURE_LEVEL)
        err = 'shutdown event'
        while not self.stop.wait(1):
            try:
                if not p.poll(1):
                    continue
                # In an eventfd, there are always 8 bytes
                ret = os.read(efd, 8)
            except poll_error as err:
                break
            self.host.log_debug('"Low memory" notification received.')
            for callback in self.low_memory_callbacks:
                callback()
            self.counts['low_mem_events'] += 1

        os.close(efd)
        self.host.log_info('"Low memory" watchdog stopped(msg="%s").' % err)

class BalloonPolicy(Policy):
    '''Manages balloons in VEs.
    '''
    def __init__(self):
        super(BalloonPolicy, self).__init__()
        self.__apply_changes_lock = Lock()

        bc = VCMMDConfig().get_bool("LoadManager.Controllers.Balloon", True)
        self.counts['Balloon'] = {}
        if not bc:
            return
        self.controllers.add(self.balloon_controller)
        self.low_memory_callbacks.add(self.balloon_controller)
        self.balloon_timeout = 5

    def update_balloon_stats(self):
        pass

    @Policy.controller
    def balloon_controller(self):
        '''Set VE memory quotas

        Expects that self is an appropriate BalloonPolicy with overwritten
        calculate_balloon_size.
        '''
        with self.__apply_changes_lock:
            self.update_balloon_stats()

            ve_quotas = self.calculate_balloon_size()

            # Apply the quotas.
            for ve, (target, protection) in ve_quotas.iteritems():
                if ve.target != target or ve.protection != protection:
                    ve.set_mem(target=target, protection=protection)

        return self.balloon_timeout

    def calculate_balloon_size(self):
        '''Calculate VE memory quotas

        Returns a mapping VE -> (target, protection), where 'target'
        is the memory consumption that should be set for a VE and 'protection'
        is the amount memory that should be protected from host pressure.
        '''
        return {ve: (ve.config.limit, ve.mem_min) for ve in self.get_ves()}


class NumaPolicy(Policy):
    '''Manages NUMA nodes' load by VEs.
    '''

    def __init__(self):
        super(NumaPolicy, self).__init__()
        self.__apply_changes_lock = Lock()

        host_has_numa = len(self.host.numa.nodes_ids) > 1
        default = host_has_numa

        nc = VCMMDConfig().get_bool("LoadManager.Controllers.NUMA", default)

        if not nc:
            if not host_has_numa:
                self.logger.info("Found < 2 NUMA nodes, no need balance")
            return

        self.controllers.add(self.numa_controller)
        self.low_memory_callbacks.add(self.numa_low_memory_callback)
        self.numa_timeout = 60 * 5
        self.__prev_numa_migrations = {}
        self.counts['NUMA'] = {}
        self.counts['NUMA']['ve'] = {}
        self.counts['NUMA']['node'] = {i: 0 for i in self.host.numa.nodes_ids}

    def ve_activated(self, ve):
        super(NumaPolicy, self).ve_activated(ve)
        if 'NUMA' in self.counts:
            self.counts['NUMA']['ve'][ve.name] = 0

    def ve_deactivated(self, ve):
        super(NumaPolicy, self).ve_deactivated(ve)
        if 'NUMA' in self.counts:
            try:
                del self.counts['NUMA']['ve'][ve.name]
                del self.__prev_numa_migrations[ve.name]
            except KeyError:
                pass

    @abstractmethod
    def update_numa_stats(self):
        pass

    def apply_changes(self, changes):
        if changes is None:
            return
        for ve, nodes in tuple(changes.iteritems()):
            if not isinstance(nodes, (list, tuple, types.NoneType)):
                self.logger.error("Invalid nodes list: %r for ve: %s" % (nodes, ve))
                del changes[ve]
                continue
            if nodes is not None:
                ve.set_node_list(nodes)
            if nodes != self.__prev_numa_migrations.get(ve.name, None):
                try:
                    self.counts['NUMA']['ve'][ve.name] += 1
                    self.__prev_numa_migrations[ve.name] = nodes
                except KeyError:
                    pass
                for node in nodes:
                    self.counts['NUMA']['node'][node] += 1
        self.logger.debug(repr(self.__prev_numa_migrations))

    @Policy.controller
    def numa_controller(self):
        '''Reapply_policy VEs between NUMA nodes.

        Expects that self is an appropriate NumaPolicy with overwritten
        get_numa_migrations.
        '''
        with self.__apply_changes_lock:
            self.update_numa_stats()

            changes = self.get_numa_migrations()
            self.apply_changes(changes)

        return self.numa_timeout

    @abstractmethod
    def get_numa_migrations(self):
        '''Suggest VE numa node migrations.

        Returns a mapping VE -> new node list, or None to preserve old list.

        This function must be overridden in sub-class.
        '''
        pass

    def numa_low_memory_callback(self):
        with self.__apply_changes_lock:
            self.update_numa_stats()

            changes = self.get_low_memory_param()
            if changes is not None:
                self.apply_changes(changes)

    def get_low_memory_param(self):
        pass


class KSMPolicy(Policy):
    '''Manages ksm parametrs on host
    '''
    def __init__(self):
        super(KSMPolicy, self).__init__()
        nested_v = 'hypervisor' in get_cpuinfo_features()
        default = not nested_v

        kc = VCMMDConfig().get_bool("LoadManager.Controllers.KSM", default)

        if not kc:
            if nested_v:
                self.host.thptune({"khugepaged/defrag": "0"})
                self.host.thptune({"enabled": "never", "defrag": "never"})
                self.logger.info("Running in hypervisor, no need for ksm")
            return

        self.controllers.add(self.ksm_controller)
        self.ksm_timeout = 60
        self.counts['KSM'] = {'run': 0}

    @abstractmethod
    def update_ksm_stats(self):
        pass

    @Policy.controller
    def ksm_controller(self):
        self.update_ksm_stats()
        params = self.get_ksm_params()

        run = params.get('run', None)
        if run is not None and self.host.stats.ksm_run != run:
            self.counts['KSM']['run'] += 1
            self.host.log_info("Switch KSM run: %s" % run)

        self.host.ksmtune(params)

        params = self.get_thp_params()
        if params is not None:
            self.host.thptune(params)

        return self.ksm_timeout

    @abstractmethod
    def get_ksm_params(self):
        pass

    def get_thp_params(self):
        pass


class StoragePolicy(Policy):
    '''Manages cgroup slice parametrs in sysfs
    '''

    STORAGE_CONFIG = '/etc/vz/vstorage-limits.conf'
    SELF_NAME = 'VStorage'
    SLICE_NAME = 'vstorage.slice/vstorage-services.slice'

    def __init__(self):
        super(StoragePolicy, self).__init__()

        self.controllers.add(self.storage_controller)
        self.cgroup_timeout = 6
        self.storage_config = {'Path': self.SLICE_NAME}
        try:
            self.storage_config.update(self.__read_config())
        except Exception as e:
            self.logger.error("Failed to read vstorage config(): %s" % str(e))
        self.__service_path = '/sys/fs/cgroup/memory/%s' % self.storage_config['Path']
        os.system("echo 1 > %s/memory.disable_cleancache" % self.__service_path)

    def __read_config(self):
        with open(self.STORAGE_CONFIG) as f:
            j = json.loads(f.read())

        for name, service in j.iteritems():
            if str(name) == self.SELF_NAME:
                return service
        return {}

    def init_storage_ct(self):
        known_params = {'Limit', 'Guarantee', 'Swap', 'Path'}
        unknown_params = set(self.storage_config.keys()) - known_params
        if unknown_params:
            raise Exception("Unknown fields in %s: %s" % (self.__service_path, unknown_params))
        kv = {}
        total_mem = psutil.virtual_memory().total
        for Param in known_params - {'Path'}:
            param = Param.lower()
            try:
                unknown_params = (set(self.storage_config[Param].iterkeys()) -
                                  {'Share', 'Min', 'Max'})
                if unknown_params:
                    self.logger.error("Unknown fields in %s: %s" % (self.__service_path, unknown_params))
                    return
                kv[param] = clamp(int(self.storage_config[Param]['Share'] * total_mem),
                                  int(self.storage_config[Param].get('Min', 0)),
                                  int(self.storage_config[Param].get('Max', -1)))
            except (KeyError, TypeError, ValueError):
                self.logger.error("Error parsing %s %r %s" %
                                     (self.SLICE_NAME, self.storage_config, Param))
                return
        proxy = RPCProxy()
        try:
            proxy.register_ve(self.SLICE_NAME, VE_TYPE_SERVICE, VEConfig(**kv), 0)
            proxy.activate_ve(self.SLICE_NAME, 0)
        except VCMMDError as e:
            if e.errno in (VCMMD_ERROR_VE_NAME_ALREADY_IN_USE, VCMMD_ERROR_VE_ALREADY_ACTIVE):
                return
            raise


    def get_cgroup_params(self, ves):
        '''The limit should be set as follows:
        1. no VMs/CTs - no limit (consider as all RAM for #3), but no limit should be applied
        2. limit should be not less than 2 GB under any circumstances
        3. VM/CT started - the limit should be decreased by VM/CT RAM size * 1.1
        '''
        if not ves:
            return {}
        used_by_ves = int(1.1 * sum(ve.effective_limit for ve in self.get_ves() if ve.VE_TYPE != VE_TYPE_SERVICE))
        return {"cache.limit_in_bytes": max(2 << 30, self.host.ve_mem - used_by_ves)}

    @Policy.controller
    def storage_controller(self):
        if not os.path.isdir(self.__service_path):
            self.cgroup_timeout = 6
            return self.cgroup_timeout
        ves_uuids = [ve.name for ve in self.get_ves()]
        # TODO
        #if self.SLICE_NAME not in ves_uuids:
        #    self.init_storage_ct()
        params = self.get_cgroup_params(self.get_ves())
        if params is not None:
            cache_limit = params.get("cache.limit_in_bytes", -1)
            self.logger.error("Set cache.limit_in_bytes to %s" % cache_limit)
            if not os.path.exists(os.path.join(self.__service_path, "memory.cache.limit_in_bytes")):
                return self.cgroup_timeout
            try:
                MemoryCgroup(self.SLICE_NAME).write_cache_limit_in_bytes(cache_limit)
            except Exception as e:
                self.cgroup_timeout = 10
                self.logger.error("Failed to set cache.limit_in_bytes for vstorage: %s" % str(e))

        return self.cgroup_timeout
