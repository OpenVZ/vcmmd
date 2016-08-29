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

from __future__ import absolute_import

import logging

from vcmmd.error import (VCMMDError,
                         VCMMD_ERROR_INVALID_VE_NAME,
                         VCMMD_ERROR_INVALID_VE_TYPE,
                         VCMMD_ERROR_INVALID_VE_CONFIG,
                         VCMMD_ERROR_VE_ALREADY_ACTIVE,
                         VCMMD_ERROR_VE_NOT_ACTIVE,
                         VCMMD_ERROR_VE_OPERATION_FAILED)
from vcmmd.ve_type import get_ve_type_name
from vcmmd.util.stats import Stats
from vcmmd.numa import Numa
from vcmmd.util.threading import update_stats_single


class Error(Exception):
    pass


class VEStats(Stats):

    ABSOLUTE_STATS = [
        'rss',          # resident set size
        'actual',       # actual amount of memory committed to the guest
                        # (RAM size - balloon size for VM, memory limit for CT)
        'host_mem',     # size of host memory used
                        # (note it can be less than rss due to shared pages)
        'host_swap',    # size of host swap used
        'memfree',      # amount of memory left completely unused by guest OS
        'memavail',     # an estimate of how much memory is available for
                        # starting new applications, without swapping
        'last_update',  # timestemp when was memory stats updated
    ]

    CUMULATIVE_STATS = [
        'swapin',       # amount of memory read in from swap space
        'swapout',      # amount of memory written out to swap space
        'minflt',       # minor page fault count
        'majflt',       # major page fault count
        'rd_req',       # number of read requests
        'rd_bytes',     # number of read bytes
        'wr_req',       # number of write requests
        'wr_bytes',     # number of written bytes
    ]

    ALL_STATS = ABSOLUTE_STATS + CUMULATIVE_STATS


class VENumaNodeStats(Stats):

    ABSOLUTE_STATS = [
        'memtotal',        # sum of the following 3 stats:
        'memfile',         # number of file pages
        'memanon',         # number of anonymous pages
        'memunevictable',  # number of unevictable pages
    ]

    CUMULATIVE_STATS = [
        'cpuuser',         # Percentage of CPU utilization at the user level
        'cpunice',         # Percentage of CPU utilization with nice priority
        'cpusystem',       # Percentage of CPU utilization at the system level
        'cpuidle',         # Percentage of time that the CPU or CPUs were idle
    ]

    ALL_STATS = ABSOLUTE_STATS + CUMULATIVE_STATS


class VEImpl(object):
    '''VE implementation.

    This class defines the interface to an underlying VE implementation
    (such as libvirt or cgroup).

    Any of the functions defined by this interface may raise Error.
    '''

    def __init__(self, name):
        pass

    @staticmethod
    def mem_overhead(config_limit):
        '''Return an estimate of memory overhead.

        This function is supposed to return the amount of memory beyond the
        configured limit which is required to run the VE smoothly. E.g. for
        VMs this should equal expected RSS of the emulator process.
        '''
        return 0

    def get_stats(self):
        '''Return stats dict {name: value}.
        '''
        pass

    def set_mem_protection(self, value):
        '''Set memory best-effort protection.

        If memory usage of a VE is below this value, the VE's memory shouldn't
        be reclaimed on host pressure if memory can be reclaimed from
        unprotected VEs.
        '''
        pass

    def set_mem_target(self, value):
        '''Set memory allocation target.

        This function sets memory consumption target for a VE. Note, it does
        not necessarily mean that the VE memory usage will reach the target
        instantly or even any time soon - in fact, it may not reach it at all
        in case allocation is reduced. However, reducing the value will put the
        VE under heavy local memory pressure forcing it to release its memory
        to the host.
        '''
        pass

    def set_config(self, config):
        '''Set new config.
        '''
        pass

    def set_node_list(self,nodes):
        '''
        Should be expanded when memory migration "knob" will be implemented
        in kernel.
        '''
        pass


_VE_IMPL_MAP = {}  # VE type -> VE implementation class


def register_ve_impl(ve_impl):
    assert ve_impl.VE_TYPE not in _VE_IMPL_MAP
    _VE_IMPL_MAP[ve_impl.VE_TYPE] = ve_impl


def _lookup_ve_impl(ve_type):
    try:
        return _VE_IMPL_MAP[ve_type]
    except KeyError:
        raise VCMMDError(VCMMD_ERROR_INVALID_VE_TYPE)


def _check_ve_name(name):
    if not name or '/' in name:
        raise VCMMDError(VCMMD_ERROR_INVALID_VE_NAME)


def _check_ve_config(config):
    if not config.is_valid():
        raise VCMMDError(VCMMD_ERROR_INVALID_VE_CONFIG)


class VE(object):

    def __init__(self, ve_type, name, config):
        _check_ve_name(name)
        _check_ve_config(config)

        self._impl = _lookup_ve_impl(ve_type)
        self._obj = None

        self._logger = logging.getLogger('vcmmd.ve')

        self.name = name
        self.config = config
        self.stats = VEStats()
        self.numa_stats = {i: VENumaNodeStats() for i in Numa().get_nodes_ids()}
        self.active = False
        self._overhead = self._impl.mem_overhead(config.limit)

    def __str__(self):
        return "%s '%s'" % (get_ve_type_name(self.VE_TYPE), self.name)

    def _log(self, lvl, msg, *args, **kwargs):
        self._logger.log(lvl, str(self) + ': ' + msg, *args, **kwargs)

    def _log_err(self, *args, **kwargs):
        self._log(logging.ERROR, *args, **kwargs)

    def _log_info(self, *args, **kwargs):
        self._log(logging.INFO, *args, **kwargs)

    def _log_debug(self, *args, **kwargs):
        # Debugging is unlikely to be enabled.
        # Avoid evaluating args if it is not.
        if self._logger.isEnabledFor(logging.DEBUG):
            self._log(logging.DEBUG, *args, **kwargs)

    @property
    def VE_TYPE(self):
        return self._impl.VE_TYPE

    def _get_obj(self):
        if self._obj is None:
            obj = self._impl(self.name)
            obj.set_config(self.config)
            self._obj = obj
        return self._obj

    def activate(self):
        '''Mark VE active.

        This function is supposed to be called after a VE switched to a state,
        in which its memory allocation can be tuned.
        '''
        if self.active:
            raise VCMMDError(VCMMD_ERROR_VE_ALREADY_ACTIVE)

        self.active = True
        self._log_info('Activated')

    def deactivate(self):
        '''Mark VE inactive.

        This function is supposed to be called before switching a VE to a state
        in which its runtime memory parameters cannot be changed any more (e.g.
        suspended or paused).
        '''
        if not self.active:
            raise VCMMDError(VCMMD_ERROR_VE_NOT_ACTIVE)

        # We need uptodate rss for inactive VEs - see VE.mem_min
        self.update_stats()

        self.active = False
        self._log_info('Deactivated')

    @update_stats_single
    def update_numa_stats(self):
        assert self.active

        def sum_values_in_dicts(dicts):
            ret = dicts[0]
            for k in ret:
                ret[k] += sum([d[k] for d in dicts[1:]])
            return ret

        try:
            obj = self._get_obj()
            cpucg_stats = obj._cpucg.get_cpu_stats()
            numa_stats = [
                {n: sum_values_in_dicts([cpucg_stats[cpu] for cpu in Numa().nodes[n].cpu_list])
                    for n in Numa().nodes},
                obj._memcg.get_numa_stats(),
            ]
            all = numa_stats[0]
            for stats in numa_stats[1:]:
                for node in stats:
                    all[node].update(stats[node])
            for node, data in all.iteritems():
                self.numa_stats[node]._update(**data)
        except Error as err:
            self._log_err('Failed to update numa stats: %s', err)
        else:
            self._log_debug('update_numa_stats: %s', self.stats)

    @update_stats_single
    def update_stats(self):
        '''Update VE stats.
        '''
        assert self.active

        try:
            obj = self._get_obj()
            self.stats._update(**obj.get_stats())
        except Error as err:
            self._log_err('Failed to update stats: %s', err)
        else:
            self._log_debug('update_stats: %s', self.stats)

    @property
    def mem_overhead(self):
        return self._overhead + self.config.vram

    @property
    def mem_min(self):
        '''Return min memory size required by this VE.

        Normally, it simply returns configured guarantee plus overhead.
        However, for an inactive VE the result will never be less than RSS,
        because its allocation cannot be tuned any more.
        '''
        val = self.config.mem_min + self._overhead
        if not self.active:
            val = max(val, self.stats.rss)
        return val

    @property
    def mem_shared(self):
        return max(0, self.stats.rss - self.stats.host_mem)

    def set_mem(self, target = None, protection = None):
        '''Set VE memory consumption target.
        '''
        assert self.active

        msg = ''
        try:
            obj = self._get_obj()
            if target is not None:
                obj.set_mem_target(target)
                msg = 'target:%d ' % target
            if protection is not None:
                obj.set_mem_protection(protection)
                msg += 'protection:%d' % protection
        except Error as err:
            self._log_err('Failed to tune allocation: %s', err)
        else:
            if msg:
                self._log_debug('set_mem: %s' % msg)
            self.target = target
            self.protection = protection

    def set_config(self, config):
        '''Update VE config.
        '''
        _check_ve_config(config)

        if not self.active:
            raise VCMMDError(VCMMD_ERROR_VE_NOT_ACTIVE)

        try:
            obj = self._get_obj()
            obj.set_config(config)
        except Error as err:
            self._log_err('Failed to set config: %s', err)
            raise VCMMDError(VCMMD_ERROR_VE_OPERATION_FAILED)

        self.config = config
        self._log_info('Config updated: %s', config)

    def set_node_list(self, nodes):
        '''Set VE NUMA binding
        '''
        try:
            obj = self._get_obj()
            obj.set_node_list(nodes)
        except Error as err:
            self._log_err('Failed to bind NUMA nodes: %s' % err)
        else:
            self._log_debug('set_node_list: %s' % str([n.id for n in nodes]))
