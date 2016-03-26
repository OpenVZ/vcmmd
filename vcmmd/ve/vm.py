from __future__ import absolute_import

import logging
from libvirt import libvirtError, VIR_DOMAIN_XML_INACTIVE
from xml.etree import ElementTree as XMLET

from vcmmd.cgroup import MemoryCgroup
from vcmmd.ve.base import Error, VEImpl, register_ve_impl
from vcmmd.ve_type import VE_TYPE_VM
from vcmmd.config import VCMMDConfig
from vcmmd.util.libvirt import virDomainProxy, lookup_qemu_machine_cgroup
from vcmmd.util.misc import roundup


class VMImpl(VEImpl):

    VE_TYPE = VE_TYPE_VM

    def __init__(self, name):
        try:
            self._libvirt_domain = virDomainProxy(name)

            # libvirt must be explicitly told to collect memory statistics
            period = VCMMDConfig().get_num('VE.VM.MemStatsPeriod',
                                           default=5, integer=True, minimum=1)
            self._libvirt_domain.setMemoryStatsPeriod(period)
        except libvirtError as err:
            raise Error(err)

        # QEMU places every virtual machine in its own memory cgroup under
        # machine.slice
        try:
            cgroup = lookup_qemu_machine_cgroup(self._libvirt_domain.name())
        except EnvironmentError as err:
            raise Error("Failed to lookup VM's cgroup: %s" % err)

        self._memcg = MemoryCgroup(cgroup[MemoryCgroup.CONTROLLER])
        if not self._memcg.exists():
            raise Error('VM memory cgroup does not exist')

    @classmethod
    def estimate_overhead(cls, name):
        # VM overhad = QEMU process overhead + VRAM
        #
        # We assume the former to be constant. We retrieve the latter from the
        # persistent domain config.

        vram = 0
        try:
            # Domain may be inactive when this function is called
            xml_desc = virDomainProxy(name).XMLDesc(VIR_DOMAIN_XML_INACTIVE)
            for video_device in XMLET.fromstring(xml_desc).iter('video'):
                vram += int(video_device.find('model').get('vram'))
            vram <<= 10  # libvirt reports in KB
        except libvirtError as err:
            raise Error(err)
        except (XMLET.ParseError, ValueError) as err:
            raise Error("Failed to parse VM's XML descriptor: %s" % err)

        qemu_overhead = VCMMDConfig().get_num('VE.VM.QEMUOverhead',
                                              default=209715200,
                                              integer=True, minimum=0)

        return qemu_overhead + vram

    def get_mem_stats(self):
        try:
            stat = self._libvirt_domain.memoryStats()
        except libvirtError as err:
            raise Error(err)

        # libvirt reports memory values in kB, so we need to convert them to
        # bytes
        return {'actual': stat.get('actual', -1) << 10,
                'rss': stat.get('rss', -1) << 10,
                'memtotal': stat.get('available', -1) << 10,
                'memfree': stat.get('unused', -1) << 10,
                'memavail': stat.get('memavailable', -1) << 10,
                'committed': stat.get('committed', -1) << 10,
                'wss': stat.get('working_set_size', -1) << 10,
                'swapin': stat.get('swap_in', -1) << 10,
                'swapout': stat.get('swap_out', -1) << 10,
                'minflt': stat.get('minor_fault', -1),
                'majflt': stat.get('major_fault', -1)}

    def get_io_stats(self):
        try:
            stat = self._libvirt_domain.blockStats('')
        except libvirtError as err:
            raise Error(err)

        return {'rd_req': stat[0],
                'rd_bytes': stat[1],
                'wr_req': stat[2],
                'wr_bytes': stat[3]}

    def set_mem_protection(self, value):
        # Use memcg/memory.low to protect the VM from host pressure.
        try:
            self._memcg.write_mem_low(value)
        except IOError as err:
            raise Error(err)

    def set_mem_target(self, value):
        # Update current allocation size by inflating/deflating balloon.
        try:
            # libvirt wants kB
            self._libvirt_domain.setMemory(value >> 10)
        except libvirtError as err:
            raise Error(err)

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
            raise Error(err)

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
            raise Error(err)

register_ve_impl(VMImpl)
