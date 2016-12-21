# Copyright (c) 2016 Parallels IP Holdings GmbH
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
# Our contact details: Parallels IP Holdings GmbH, Vordergasse 59, 8200
# Schaffhausen, Switzerland.
import logging
import types
import os
from time import sleep
from select import epoll, EPOLLIN
import struct
from abc import ABCMeta, abstractmethod
from threading import Lock

from vcmmd.host import Host
from vcmmd.ldmgr.base import Request
from vcmmd.config import VCMMDConfig
from vcmmd.util.misc import print_dict
from vcmmd.util.cpu_features import get_cpuinfo_features
from vcmmd.ve_type import VE_TYPE_CT

def eventfd(init_val, flags):
    from ctypes import cdll
    import ctypes
    libc = cdll.LoadLibrary("libc.so.6")
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

    def get_name(self):
        return self.__class__.__name__

    def get_policy_data(self, t):
        with self.__ve_data_lock:
            return list(self.__ve_data.get(t, {}).itervalues())

    def rm_policy_data(self, t, ve):
        with self.__ve_data_lock:
            self.__ve_data.get(t, {}).pop(ve, None)

    def set_policy_data(self, ve, data):
        with self.__ve_data_lock:
            t = type(data)
            if t not in self.__ve_data:
                self.__ve_data[t] = {}
            self.__ve_data[t][ve] = data

    def ve_activated(self, ve):
        '''Called right after a VE gets activated.
        '''
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

    def sched_req(self):
        ret = []
        for ctrl in self.controllers:
            ret.append(ctrl())
        if self.low_memory_callbacks:
            ret.append(Request(self.low_memory_watchdog, blocker=True))
        return ret

    def report(self, j=False):
        return print_dict(self.counts, j)

    def shutdown(self):
        self.__watchdog__run = False

    def low_memory_watchdog(self):
        self.counts['low_mem_events'] = 0
        self.__watchdog__run = True
        efd = eventfd(0, 0)
        mp = open(self.MEM_PRES_PATH)
        with open(self.EVENT_CONTR_PATH, 'w') as cgc:
            cgc.write("%d %d %s" % (efd, mp.fileno(), self.PRESSURE_LEVEL))

        p = epoll()
        p.register(efd, EPOLLIN)

        self.host.log_info('"Low memory" watchdog started(pressure level=%r).' % self.PRESSURE_LEVEL)
        err = 'shutdown event'
        while self.__watchdog__run:
            try:
                events = p.poll(2)
                for fd,event in events:
                    if event & EPOLLIN:
                        # In an eventfd, there are always 8 bytes
                        ret = os.read(efd, 64/8)
                        num = struct.unpack('Q', ret)
                        break
                else:
                    continue
            except (ValueError, OSError, IOError) as err:
                break
            self.host.log_debug('"Low memory" notification received: %d' % num)
            for callback in self.low_memory_callbacks:
                callback()
            self.counts['low_mem_events'] += 1

        p.close()
        os.close(efd)
        self.host.log_info('"Low memory" watchdog stopped(msg="%s").' % err)


class BalloonPolicy(Policy):
    '''Manages balloons in VEs.
    '''
    def __init__(self):
        super(BalloonPolicy, self).__init__()
        self.__apply_changes_lock = Lock()

        bc = VCMMDConfig().get_bool("LoadManager.Controllers.Balloon", False)
        self.counts['Balloon'] = {}
        if not bc:
            return
        self.controllers.add(self.balloon_controller)
        self.low_memory_callbacks.add(self.balloon_controller)
        self.balloon_timeout = 5

    @abstractmethod
    def update_balloon_stats(self):
        pass

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
                ve.set_mem(target=target, protection=protection)

            return Request(self.balloon_controller, timeout=self.balloon_timeout, blocker=True)

    @abstractmethod
    def calculate_balloon_size(self):
        '''Calculate VE memory quotas

        Returns a mapping VE -> (target, protection), where 'target'
        is the memory consumption that should be set for a VE and 'protection'
        is the amount memory that should be protected from host pressure.

        This function must be overridden in sub-class.
        '''
        pass


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

    def numa_controller(self):
        '''Reapply_policy VEs between NUMA nodes.

        Expects that self is an appropriate NumaPolicy with overwritten
        get_numa_migrations.
        '''
        with self.__apply_changes_lock:
            self.update_numa_stats()

            changes = self.get_numa_migrations()
            self.apply_changes(changes)

            return Request(self.numa_controller, timeout=self.numa_timeout, blocker=True)

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
            self.apply_changes(changes)
            sleep(1)

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

        return Request(self.ksm_controller, timeout=self.ksm_timeout, blocker=True)

    @abstractmethod
    def get_ksm_params(self):
        pass

    def get_thp_params(self):
        pass
