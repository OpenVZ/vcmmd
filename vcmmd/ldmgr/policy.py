# Copyright (c) 2016-2017, Parallels International GmbH
# Copyright (c) 2017-2019, Virtuozzo International GmbH, All rights reserved
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

import logging
import types
import os
from select import poll, POLLIN, error as  poll_error
from abc import ABCMeta, abstractmethod
from threading import Lock, Thread, Event
import ctypes
import json

from vcmmd.host import Host
from vcmmd.config import VCMMDConfig
from vcmmd.util.misc import print_dict
from vcmmd.util.cpu_features import get_cpuinfo_features
from vcmmd.ve_type import VE_TYPE_CT, VE_TYPE_SERVICE
from vcmmd.cgroup import MemoryCgroup
from vcmmd.util.limits import INT64_MAX
from vcmmd.util.misc import get_cs_num


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
    """Load manager policy interface."""

    __metaclass__ = ABCMeta

    MEM_PRES_PATH = '/sys/fs/cgroup/memory/memory.pressure_level'
    EVENT_CONTROL_PATH = '/sys/fs/cgroup/memory/cgroup.event_control'
    PRESSURE_LEVEL = 'medium'
    DEFAULT_VM_AUTO_GUARANTEE = 0.4

    def __init__(self):
        self.logger = logging.getLogger('vcmmd.ldmgr.policy')
        self.host = Host()  # Singleton object with host related data
        self.controllers = set()
        self.low_memory_callbacks = set()
        self.__ve_data = {}  # Dictionary of all managed VEs to their policy data
        self.__ve_data_lock = Lock()
        self.counts = {}
        self.stop = Event()
        self.controllers_threads = []

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
            data = [data.get(t, None) for data in self.__ve_data.values()]
            return list(filter(None, data))

    def rm_policy_data(self, t, ve):
        with self.__ve_data_lock:
            self.__ve_data.get(ve, {}).pop(t, None)

    def set_policy_data(self, ve, data):
        with self.__ve_data_lock:
            t = type(data)
            self.__ve_data[ve][t] = data

    def ve_registered(self, ve):
        """Called before a VE gets activated."""
        if ve.VE_TYPE == VE_TYPE_CT:
            ve.set_mem(ve.config.limit, ve.mem_min)

    def ve_activated(self, ve):
        """Called right after a VE gets registered."""
        with self.__ve_data_lock:
            if ve not in self.__ve_data:
                self.__ve_data[ve] = {}
        ve.set_mem(ve.config.limit, ve.mem_min)

    def ve_deactivated(self, ve):
        """Called before a VE gets unregistered."""
        with self.__ve_data_lock:
            if ve in self.__ve_data:
                del self.__ve_data[ve]

    def ve_unregistered(self, ve):
        """Called right after a VE gets deactivated."""
        pass

    def ve_config_updated(self, ve):
        """Called right after a VE's configuration update."""
        ve.set_mem(ve.config.limit, ve.mem_min)

    def load(self):
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
        with open(self.EVENT_CONTROL_PATH, 'w') as cgc:
            cgc.write('%d %d %s' % (efd, mp.fileno(), self.PRESSURE_LEVEL))

        p = poll()
        p.register(efd, POLLIN)

        self.host.log_info('"Low memory" watchdog started(pressure level=%r).' % self.PRESSURE_LEVEL)
        err = 'shutdown event'
        while not self.stop.wait(1):
            try:
                if not p.poll(1):
                    continue
                # In an eventfd, there are always 8 bytes
                _ = os.read(efd, 8)
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
            for ve, (target, protection) in ve_quotas.items():
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
        if not VCMMDConfig().get_bool("LoadManager.Controllers.KSM", default):
            return
        if nested_v:
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
    """Manages cgroup slice parameters."""

    STORAGE_CONFIG = '/etc/vz/vstorage-limits.conf'
    SELF_NAME = 'VStorage'
    SLICE_NAME = 'vstorage.slice/vstorage-services.slice'

    def __init__(self):
        super(StoragePolicy, self).__init__()
        if not VCMMDConfig().get_bool('LoadManager.Controllers.StoragePolicy', True):
            return
        self.controllers.add(self._storage_controller)
        self.storage_config = {'Path': self.SLICE_NAME}
        try:
            self.storage_config.update(self._read_config())
        except Exception as e:
            self.logger.error('Failed to read vstorage config(): %s' % e)
        self._service_path = os.path.join('/sys/fs/cgroup/memory', self.storage_config['Path'])
        self._memcgp = MemoryCgroup(self.SLICE_NAME)

    def _read_config(self):
        with open(self.STORAGE_CONFIG) as f:
            return json.load(f).get(self.SELF_NAME, {})

    def _get_cache_size(self):
        max_cache_size = max(int(2 * self.host.ve_mem / 3), self.host.ve_mem - (32 << 30))
        all_ves = self.get_ves()
        service_ves = [ve for ve in all_ves if ve.VE_TYPE == VE_TYPE_SERVICE]
        if len(all_ves) - len(service_ves) > 0:
            max_cache_size = (512 * max(2, get_cs_num())) << 20
        return max_cache_size

    @Policy.controller
    def _storage_controller(self):
        controller_timeout = 60
        if not os.path.isdir(self._service_path):
            return controller_timeout
        if not any(ve.name == self.SLICE_NAME for ve in self.get_ves()):
            self.logger.info('Storage is not registered')
            return controller_timeout
        cache_limit = VCMMDConfig().get_num('LoadManager.Controllers.StorageCacheLimitTotal', None)
        if cache_limit is None:
            cache_limit = self._get_cache_size()
        if not self._update_cgroup(cache_limit):
            controller_timeout = 10
        return controller_timeout

    def _update_cgroup(self, cache_limit):
        update_cgroup_files = [
            (self._memcgp.write_cache_limit_in_bytes, cache_limit, 'cache_limit_in_bytes'),
            (self._memcgp.write_cleancache, False, 'cleancache'),
            (self._memcgp.write_swappiness, 0, 'swappiness'),
            (self._memcgp.write_oom_control, 1, 'oom_control'),
        ]
        self.logger.debug('Set cache.limit_in_bytes to %s' % cache_limit)
        for fn, value, name in update_cgroup_files:
            try:
                fn(value)
            except Exception as e:
                self.logger.error('Failed to set %r for vstorage: %s' % (name, e))
                return False
        return True
