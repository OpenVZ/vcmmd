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

import time
from multiprocessing.pool import ThreadPool

from vcmmd.cgroup import MemoryCgroup, BlkIOCgroup, CpuSetCgroup, CpuCgroup
from vcmmd.ve.base import Error, VEImpl, register_ve_impl
from vcmmd.ve_type import VE_TYPE_CT, VE_TYPE_SERVICE
from vcmmd.util.limits import PAGE_SIZE, UINT64_MAX


# The thread pool is used in order not to block the main thread while
# performing costly operations, like memory.high adjustment.
#
# XXX: Note, using threads should not really hurt parallelism, because real
# work is done from system calls, with GIL released.
_thread_pool = ThreadPool(3)


def _lookup_cgroup(klass, name):
    # A container's cgroup is located either at the top level of the cgroup
    # hierarchy or under machine.slice

    cg = klass(name)
    if cg.exists():
        return cg

    cg = klass('/machine.slice/' + name)
    if cg.exists():
        return cg

    raise Error("cgroup not found: '{}'".format(cg.abs_path))


class ABSVEImpl(VEImpl):

    VE_TYPE = VE_TYPE_CT

    def __init__(self, name):
        self._memcg = _lookup_cgroup(MemoryCgroup, name)

        self.mem_limit = UINT64_MAX

    def get_rss(self):
        try:
            return self._memcg.read_mem_current()
        except (ValueError, IOError) as err:
            raise Error('Cgroup read failed: {}'.format(err))

    def set_mem_protection(self, value):
        # Use memcg/memory.low to protect the CT from host pressure.
        try:
            self._memcg.write_mem_low(value)
        except IOError as err:
            raise Error('Cgroup write failed: {}'.format(err))

    def set_mem_target(self, value):
        # Decreasing memory.high might take long as it implies memory reclaim,
        # so do it asynchronously.
        #
        # XXX: For the sake of simplicity, we don't care about failures here.
        # It is acceptable, because adjusting memory.high may fail only if
        # the cgroup gets destroyed, which we will see and report anyway from
        # get_stats().
        _thread_pool.apply_async(self._memcg.write_mem_high, (value,))

        self.mem_limit = value

    def set_config(self, config):
        try:
            self._memcg.write_oom_guarantee(config.guarantee)
            self._memcg.write_mem_config(config.limit, config.swap)
            self._memcg.write_tcp_mem_limit(config.limit / 8)
            self._memcg.write_udp_mem_limit(config.limit / 8)
        except IOError as err:
            raise Error('Cgroup write failed: {}'.format(err))

        self.mem_limit = min(self.mem_limit, config.limit)


class CTImpl(ABSVEImpl):

    def __init__(self, name):
        super(CTImpl, self).__init__(name)
        self._cpusetcg = _lookup_cgroup(CpuSetCgroup, name)
        self._blkcg = _lookup_cgroup(BlkIOCgroup, name)
        self._cpucg = _lookup_cgroup(CpuCgroup, name)

    def get_stats(self):
        try:
            current = self._memcg.read_mem_current()
            stat = self._memcg.read_mem_stat()
            io_serviced = self._blkcg.get_io_serviced()
            io_service_bytes = self._blkcg.get_io_service_bytes()
        except IOError as err:
            raise Error('Cgroup read failed: {}'.format(err))

        memtotal = max(self.mem_limit, current)
        memfree = memtotal - current
        memavail = (memfree +
                    stat.get('active_file', 0) +
                    stat.get('inactive_file', 0) +
                    stat.get('slab_reclaimable', 0))

        return {'rss': current,
                'host_mem': current,
                'host_swap': stat.get('swap', -1),
                'actual': memtotal,
                'memfree': memfree,
                'memavail': memavail,
                'swapin': stat.get('pswpin', -1),
                'swapout': stat.get('pswpout', -1),
                'minflt': stat.get('pgfault', -1),
                'majflt': stat.get('pgmajfault', -1),
                'rd_req': io_serviced[0],
                'rd_bytes': io_service_bytes[0],
                'wr_req': io_serviced[1],
                'wr_bytes': io_service_bytes[1],
                'last_update': int(time.time())}

    @property
    def nr_cpus(self):
        try:
            self._nr_cpus = self._cpucg.get_nr_cpus()
            return self._nr_cpus
        except IOError:
            return getattr(self, "_nr_cpus", -1)

    def get_node_list(self):
        '''Get list of nodes where CT is running
        '''
        try:
            node_list = self._cpusetcg.get_node_list()
        except IOError as err:
            raise Error('Cgroup read failed: {}'.format(err))
        return node_list

    def node_mem_migrate(self, nodes):
        try:
            self._memcg.set_node_list(nodes)
        except IOError as err:
            raise Error('Cgroup write failed: {}'.format(err))

    def pin_node_mem(self, nodes):
        '''Change list of memory nodes for CT

        This function changes CT affinity for memory and migrates CT's memory
        accordingly
        '''
        try:
            self._cpusetcg.set_node_list(nodes)
        except IOError as err:
            raise Error('Cgroup write failed: {}'.format(err))

    def pin_cpu_list(self, cpus):
        '''Change list of CPUs for CT

        This function changes CT affinity for CPUs
        '''
        try:
            self._cpusetcg.set_cpu_list(cpus)
        except IOError as err:
            raise Error('Cgroup write failed: {}'.format(err))



class ServiceCTImpl(ABSVEImpl):

    VE_TYPE = VE_TYPE_SERVICE


register_ve_impl(CTImpl)
register_ve_impl(ServiceCTImpl)
