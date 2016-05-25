from __future__ import absolute_import

import logging
from libvirt import libvirtError

from vcmmd.cgroup import MemoryCgroup
from vcmmd.ve.base import Error, VEImpl, register_ve_impl
from vcmmd.ve_type import VE_TYPE_VM, VE_TYPE_VM_LINUX, VE_TYPE_VM_WINDOWS
from vcmmd.config import VCMMDConfig
from vcmmd.util.libvirt import virDomainProxy, lookup_qemu_machine_cgroup
from vcmmd.util.misc import roundup


class VMImpl(VEImpl):

    VE_TYPE = VE_TYPE_VM

    def __init__(self, name):
        try:
            self._libvirt_domain = virDomainProxy(name)
        except libvirtError as err:
            raise Error('Failed to lookup libvirt domain: %s' % err)

        # libvirt must be explicitly told to collect memory statistics
        period = VCMMDConfig().get_num('VE.VM.MemStatsPeriod',
                                       default=5, integer=True, minimum=1)
        try:
            self._libvirt_domain.setMemoryStatsPeriod(period)
        except libvirtError as err:
            raise Error('Failed to enable libvirt domain memory stats: %s' % err)

        # libvirt places every virtual machine in its own cgroup
        try:
            cgroup = lookup_qemu_machine_cgroup(self._libvirt_domain.name())
        except EnvironmentError as err:
            raise Error('Failed to lookup machine cgroup: %s' % err)

        self._memcg = MemoryCgroup(cgroup[MemoryCgroup.CONTROLLER])
        if not self._memcg.exists():
            raise Error("Memory cgroup not found: '%s'" % self._memcg.abs_path)

    @staticmethod
    def mem_overhead():
        return VCMMDConfig().get_num('VE.VM.MemOverhead', default=33554432,
                                     integer=True, minimum=0)

    def get_stats(self):
        try:
            stat = self._libvirt_domain.memoryStats()
            blk_stat = self._libvirt_domain.blockStats('')
        except libvirtError as err:
            raise Error('Failed to retrieve libvirt domain stats: %s' % err)

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

        # libvirt reports memory values in kB, so we need to convert them to
        # bytes
        return {'actual': stat.get('actual', -1) << 10,
                'rss': stat.get('rss', -1) << 10,
                'host_mem': host_mem,
                'host_swap': host_swap,
                'memfree': stat.get('unused', -1) << 10,
                'memavail': stat.get('memavailable', -1) << 10,
                'committed': stat.get('committed', -1) << 10,
                'swapin': stat.get('swap_in', -1) << 10,
                'swapout': stat.get('swap_out', -1) << 10,
                'minflt': stat.get('minor_fault', -1),
                'majflt': stat.get('major_fault', -1),
                'rd_req': blk_stat[0],
                'rd_bytes': blk_stat[1],
                'wr_req': blk_stat[2],
                'wr_bytes': blk_stat[3]}

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
