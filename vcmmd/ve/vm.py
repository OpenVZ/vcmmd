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
import time

import logging
from libvirt import libvirtError
from libvirt import (VIR_DOMAIN_STATS_BLOCK as STATS_BLOCK,
                     VIR_DOMAIN_STATS_BALLOON as STATS_BALLOON,
                     VIR_CONNECT_GET_ALL_DOMAINS_STATS_RUNNING as GET_ALL_RUNNING)

from vcmmd.cgroup import MemoryCgroup, CpuSetCgroup, CpuCgroup
from vcmmd.ve.base import Error, VEImpl, register_ve_impl
from vcmmd.ve_type import VE_TYPE_VM, VE_TYPE_VM_LINUX, VE_TYPE_VM_WINDOWS
from vcmmd.config import VCMMDConfig
from vcmmd.util.libvirt import (virDomainProxy,
                                lookup_qemu_machine_cgroup,
                                virConnectionProxy)
from vcmmd.util.misc import roundup
from vcmmd.util.limits import PAGE_SIZE


class VMImpl(VEImpl):

    VE_TYPE = VE_TYPE_VM
    __cached_stats = {}

    def __init__(self, name):
        try:
            self._libvirt_domain = virDomainProxy(name)
        except libvirtError as err:
            raise Error('Failed to lookup libvirt domain: %s' % err)

        self.set_memstats_period(0)
        # libvirt places every virtual machine in its own cgroup
        try:
            cgroup = lookup_qemu_machine_cgroup(self._libvirt_domain.name())
        except EnvironmentError as err:
            raise Error('Failed to lookup machine cgroup: %s' % err)

        self._memcg = MemoryCgroup(cgroup[MemoryCgroup.CONTROLLER])
        if not self._memcg.exists():
            raise Error("Memory cgroup not found: '%s'" % self._memcg.abs_path)
        self._cpucg = CpuCgroup(cgroup[CpuCgroup.CONTROLLER])
        if not self._cpucg.exists():
            raise Error("Cpu cgroup not found: '%s'" % self._cpucg.abs_path)
        self._cpusetcg = CpuSetCgroup(cgroup[CpuSetCgroup.CONTROLLER])
        if not self._cpusetcg.exists():
            raise Error("CpuSet cgroup not found: '%s'" % self._cpusetcg.abs_path)

    def set_memstats_period(self, period):
        try:
            self._libvirt_domain.setMemoryStatsPeriod(period)
            self.__memstats_update_period = period
        except libvirtError as err:
            raise Error('Failed to enable libvirt domain memory stats: %s' % err)

    @staticmethod
    def mem_overhead(config_limit):
        # we assume, that for one guest page need at least 8b overhead
        # in qemu process
        guest_mem_overhead = 8 * config_limit / PAGE_SIZE
        config_overhead =  VCMMDConfig().get_num('VE.VM.MemOverhead', default=(64 << 20),
                                                 integer=True, minimum=0)
        return config_overhead + guest_mem_overhead

    def get_stats(self):
        try:
            name = self._libvirt_domain.name()
            if name not in VMImpl.__cached_stats:
                conn = virConnectionProxy()
                VMImpl.__cached_stats = {dom.name(): stats for dom, stats in \
                                         conn.getAllDomainStats(STATS_BLOCK | STATS_BALLOON,
                                         GET_ALL_RUNNING)}
            stats = VMImpl.__cached_stats.pop(name, {})
        except libvirtError as err:
            raise Error('Failed to retrieve libvirt domain stats: %s' % err)

        memstats = {k.split('.')[1]: v for k,v in stats.iteritems() if k.startswith('balloon')}
        t = time.time()
        if t - memstats.get('last-update', t) > min(60, self.__memstats_update_period * 10):
            # remove stale counters
            # 'rss' and 'actual' should always be up-to-date
            for k in set(memstats.keys()) - set(['rss', 'actual']):
                del memstats[k]

        try:
            memcg_stat = self._memcg.read_mem_stat()
        except IOError as err:
            raise Error('Cgroup read failed: %s' % err)

        try:
            # Unmapped file pages are of no interest in case of VMs
            host_mem = memcg_stat['rss'] + memcg_stat['mapped_file']
        except KeyError:
            host_mem = -1

        host_swap = memcg_stat.get('swap', -1)

        blk_stat = {'rd.reqs': 0, 'rd.bytes': 0, 'wr.reqs': 0, 'wr.bytes': 0}
        for s in blk_stat:
            for c in range(0, stats.get('block.count', 0)):
                blk_stat[s] += stats['block.%d.%s' % (c, s)]

        # libvirt reports memory values in kB, so we need to convert them to
        # bytes
        return {'actual': memstats.get('actual', -1) << 10,
                'rss': memstats.get('rss', -1) << 10,
                'host_mem': host_mem,
                'host_swap': host_swap,
                'memfree': memstats.get('unused', -1) << 10,
                'memavail': memstats.get('usable', -1) << 10,
                'swapin': memstats.get('swap_in', -1) << 10,
                'swapout': memstats.get('swap_out', -1) << 10,
                'minflt': memstats.get('minor_fault', -1),
                'majflt': memstats.get('major_fault', -1),
                'rd_req': blk_stat['rd.reqs'],
                'rd_bytes': blk_stat['rd.bytes'],
                'wr_req': blk_stat['wr.reqs'],
                'wr_bytes': blk_stat['wr.bytes'],
                'last_update': memstats.get('last-update', -1)}

    def set_mem_protection(self, value):
        # Use memcg/memory.low to protect the VM from host pressure.
        try:
            self._memcg.write_mem_low(value)
        except IOError as err:
            raise Error('Cgroup write failed: %s' % err)

    def set_mem_target(self, value):
        # Update current allocation size by inflating/deflating balloon.
        try:
            # libvirt wants kB
            self._libvirt_domain.setMemory(value >> 10)
        except libvirtError as err:
            raise Error('Failed to set libvirt domain memory size: %s' % err)

    def _hotplug_memory(self, value):
        grain = VCMMDConfig().get_num('VE.VM.MemHotplugGrain',
                                      default=134217728, integer=True,
                                      minimum=1048576)
        value = roundup(value, grain)
        value >>= 10  # libvirt wants kB
        xml = ("<memory model='dimm'>"
               "  <target>"
               "    <size unit='KiB'>{memsize}</size>"
               "    <node>0</node>"
               "  </target>"
               "</memory>").format(memsize=value)
        self._libvirt_domain.attachDevice(xml)

    def set_config(self, config):
        # Set protection against OOM killer according to configured guarantee
        try:
            self._memcg.write_oom_guarantee(config.guarantee)
        except IOError as err:
            raise Error('Cgroup write failed: %s' % err)

        # Update memory limit
        value = config.limit
        try:
            # If value is greater than MaxMemory, we have to initiate memory
            # hotplug to increase the limit.
            #
            # We ignore limit decrease here, because memory hotunplug is not
            # expected to work. Memory allocation is supposed to be decreased
            # by the policy in this case.
            max_mem = self._libvirt_domain.maxMemory()
            max_mem <<= 10  # libvirt reports in kB
            if value > max_mem:
                self._hotplug_memory(value - max_mem)
        except libvirtError as err:
            raise Error('Failed to hotplug libvirt domain memory: %s' % err)

class VMLinImpl(VMImpl):

    VE_TYPE = VE_TYPE_VM_LINUX


class VMWinImpl(VMImpl):

    VE_TYPE = VE_TYPE_VM_WINDOWS

    def get_stats(self):
        stats = super(VMWinImpl, self).get_stats()
        if stats['memavail'] < 0:
            stats['memavail'] = stats['memfree']
        return stats

register_ve_impl(VMImpl)
register_ve_impl(VMLinImpl)
register_ve_impl(VMWinImpl)
