# Copyright (c) 2016-2017, Parallels International GmbH
# Copyright (c) 2017-2022, Virtuozzo International GmbH, All rights reserved
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

import psutil
from libvirt import libvirtError
from libvirt import (VIR_DOMAIN_STATS_BLOCK as STATS_BLOCK,
                     VIR_DOMAIN_STATS_BALLOON as STATS_BALLOON,
                     VIR_CONNECT_GET_ALL_DOMAINS_STATS_RUNNING as GET_ALL_RUNNING)
from libvirt import VIR_DOMAIN_NUMATUNE_MEM_STRICT as NUMATUNE_MEM_STRICT
from libvirt import VIR_DOMAIN_AFFECT_LIVE as AFFECT_LIVE

from vcmmd.cgroup import MemoryCgroup, CpuSetCgroup, CpuCgroup, pid_cgroup
from vcmmd.ve.base import Error, VEImpl, register_ve_impl
from vcmmd.ve_type import VE_TYPE_VM, VE_TYPE_VM_LINUX, VE_TYPE_VM_WINDOWS
from vcmmd.config import VCMMDConfig
from vcmmd.util.libvirt import VirtDomainProxy, get_qemu_proxy
from vcmmd.util.misc import roundup, lookup_qemu_machine_pid

from vcmmd.util.limits import PAGE_SIZE, INT64_MAX
from vcmmd.util.misc import parse_range_list
from vcmmd.util.threading import run_async

VM_DEFAULT_CACHE_LIMIT_MB = 512


class VMImpl(VEImpl):

    VE_TYPE = VE_TYPE_VM
    __cached_stats = {}

    def __init__(self, name):
        try:
            self._libvirt_domain = VirtDomainProxy(name)
        except libvirtError as err:
            raise Error('Failed to lookup libvirt domain: {}'.format(err))

        self.pid = -1
        self._update_cgroups()

    def _update_cgroups(self):
        try:
            pid = lookup_qemu_machine_pid(self._libvirt_domain.name())
        except EnvironmentError as err:
            raise Error('Failed to lookup machine pid: {}'.format(err))

        if self.pid == pid:
            return
        self.pid = pid

        # libvirt places every virtual machine in its own cgroup
        try:
            cgroup = pid_cgroup(self.pid)
        except EnvironmentError as err:
            raise Error('Failed to lookup machine cgroup: {}'.format(err))

        self._memcg = MemoryCgroup(cgroup[MemoryCgroup.CONTROLLER])
        if not self._memcg.exists():
            raise Error("Memory cgroup not found: "
                        "'{}'".format(self._memcg.abs_path))
        self._cpucg = CpuCgroup(cgroup[CpuCgroup.CONTROLLER])
        if not self._cpucg.exists():
            raise Error("Cpu cgroup not found: "
                        "'{}'".format(self._cpucg.abs_path))

        self._emulatorcg = CpuSetCgroup(cgroup[CpuSetCgroup.CONTROLLER])
        if not self._emulatorcg.exists():
            raise Error("Cpuset cgroup not found: "
                        "'{}'".format(self._cpucg.abs_path))

        max_vcpus = self._libvirt_domain.maxVcpus()
        self._vcpucg = {}
        vcpu_path = self._emulatorcg.path
        assert vcpu_path.endswith('emulator')
        vcpu_path = '{}/vcpu{{}}'.format(vcpu_path[:-len('emulator')])

        for vcpu in range(max_vcpus):
            self._vcpucg[vcpu] = CpuSetCgroup(vcpu_path.format(vcpu))

    def get_rss(self):
        try:
            p = psutil.Process(self.pid)
            return p.memory_info().rss
        except psutil.Error as err:
            raise Error(str(err))

    def set_memstats_period(self, period):
        try:
            self._libvirt_domain.setMemoryStatsPeriod(period)
            self.__memstats_update_period = period
        except libvirtError as err:
            raise Error('Failed to enable libvirt domain '
                        'memory stats: {}'.format(err))

    @staticmethod
    def mem_overhead(config_limit):
        # we assume, that for one guest page need at least 8b overhead
        # in qemu process
        guest_mem_overhead = 0
        if config_limit < INT64_MAX:
            guest_mem_overhead = 8 * config_limit // PAGE_SIZE
        config_overhead = VCMMDConfig().get_num(
            'VE.VM.MemOverhead', default=(64 << 20), integer=True, minimum=0)
        return config_overhead + guest_mem_overhead

    def get_stats(self):
        self.set_memstats_period(2)
        try:
            name = self._libvirt_domain.name()
            if name not in VMImpl.__cached_stats:
                conn = get_qemu_proxy()
                VMImpl.__cached_stats = {dom.name(): stats for dom, stats in \
                                         conn.getAllDomainStats(STATS_BLOCK | STATS_BALLOON,
                                         GET_ALL_RUNNING)}
            stats = VMImpl.__cached_stats.pop(name, {})
        except libvirtError as err:
            raise Error('Failed to retrieve libvirt domain stats: {}'.format(err))

        memstats = {k.split('.')[1]: v for k,v in stats.items() if k.startswith('balloon')}
        try:
            memcg_stat = self._memcg.read_mem_stat()
        except IOError as err:
            raise Error('Cgroup read failed: {}'.format(err))

        try:
            # Unmapped file pages are of no interest in case of VMs
            host_mem = memcg_stat['rss'] + memcg_stat['mapped_file']
        except KeyError:
            host_mem = -1

        host_swap = memcg_stat.get('swap', -1)

        blk_stat = {'rd.reqs': 0, 'rd.bytes': 0, 'wr.reqs': 0, 'wr.bytes': 0}
        for s in blk_stat:
            for c in range(0, stats.get('block.count', 0)):
                blk_stat[s] += stats.get('block.{}.{}'.format(c, s), 0)

        # libvirt reports memory values in kB, so we need to convert them to
        # bytes
        return {'actual': memstats.get('current', -1) << 10,
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
            raise Error('Cgroup write failed: {}'.format(err))

    def set_mem_target(self, value):
        # Update current allocation size by inflating/deflating balloon.
        try:
            # libvirt wants kB
            run_async(self._libvirt_domain.setMemory, value >> 10)
        except libvirtError as err:
            raise Error('Failed to set libvirt domain memory size: {}'.format(err))

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
            raise Error('Cgroup write failed: {}'.format(err))

        # Set memory.cache.limit_in_bytes to limit cache generated by backup
        cache_limit = VCMMDConfig().get('LoadManager.Controllers.VMCacheLimitTotal',
            VM_DEFAULT_CACHE_LIMIT_MB * 1024 * 1024)
        self._memcg.write_cache_limit_in_bytes(cache_limit)

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
            raise Error('Failed to hotplug libvirt domain memory: {}'.format(err))

    def get_node_list(self):
        '''Get list of nodes where VM is running
        NOTE: only for memory
        '''
        try:
            ret = self._libvirt_domain.numaParameters()
        except libvirtError as err:
            raise Error(str(err))

        return parse_range_list(ret['numa_nodeset'])

    def pin_node_mem(self, nodes, libvirt = False):
        '''Change list of memory nodes for VM

        This function changes VM affinity for memory and migrates VM's memory
        accordingly
        '''
        if libvirt:
            return self.pin_node_mem_libvirt(nodes)

        node_mask = ','.join([str(node) for node in nodes])
        try:
            for vcpu in range(self.nr_cpus):
                self._vcpucg[vcpu].set_node_list(nodes)

            self._emulatorcg.set_node_list(nodes)
        except (IOError, libvirtError) as err:
            raise Error('Cgroup write failed: {}'.format(err))

    def pin_node_mem_libvirt(self, nodes):
        params = {'numa_nodeset': ','.join([str(node) for node in nodes]),
                  'numa_mode': NUMATUNE_MEM_STRICT}

        try:
            self._libvirt_domain.setNumaParameters(params, AFFECT_LIVE)
        except libvirtError as err:
            raise Error(str(err))

    def pin_cpu_list(self, cpus, libvirt = False):
        '''Change list of CPUs for VM

        This function changes VM affinity for CPUs
        '''
        if libvirt:
            return self.pin_cpu_list_libvirt(cpus)

        try:
            for vcpu in range(self.nr_cpus):
                self._vcpucg[vcpu].set_cpu_list(cpus)
            self._emulatorcg.set_cpu_list(cpus)
        except (IOError, libvirtError) as err:
            raise Error('Cgroup write failed: {}'.format(err))

    def pin_cpu_list_libvirt(self, cpus):
        cpu_map = [0] * (max(cpus) + 1)
        for i in cpus:
            cpu_map[i] = 1
        cpu_map = tuple(cpu_map)

        try:
            for vcpu in range(self.nr_cpus):
                self._libvirt_domain.pinVcpu(vcpu, cpu_map)
            self._libvirt_domain.pinEmulator(cpu_map, AFFECT_LIVE)
        except libvirtError as err:
            raise Error(str(err))

    @property
    def nr_cpus(self):
        try:
            self._nr_cpus = self._libvirt_domain.vcpusFlags(AFFECT_LIVE)
            return self._nr_cpus
        except libvirtError:
            return getattr(self, "_nr_cpus", -1)


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
