from __future__ import absolute_import

from libvirt import libvirtError
from xml.etree import ElementTree as XMLET

from vcmmd.cgroup import MemoryCgroup
from vcmmd.ve import VE, Error, types as ve_types
from vcmmd.config import VCMMDConfig
from vcmmd.util.libvirt import virDomainProxy
from vcmmd.util.systemd import escape_unit_name, Error as SystemdError
from vcmmd.util.misc import roundup


class VM(VE):

    VE_TYPE = ve_types.VM

    def __init_libvirt_domain(self):
        try:
            self._libvirt_domain = virDomainProxy(self.name)

            # libvirt must be explicitly told to collect memory statistics
            period = VCMMDConfig().get_num('VE.VM.MemStatsPeriod',
                                           default=5, integer=True, minimum=1)
            self._libvirt_domain.setMemoryStatsPeriod(period)
        except libvirtError as err:
            raise Error(err)

    def __init_cgroup(self):
        # QEMU places every virtual machine in its own memory cgroup under
        # machine.slice
        try:
            dom_name = self._libvirt_domain.name()
            unit_name = escape_unit_name('qemu-' + dom_name, 'scope')
        except (libvirtError, SystemdError) as err:
            raise Error(err)
        self._memcg = MemoryCgroup('machine.slice/machine-' + unit_name)
        if not self._memcg.exists():
            raise Error('VM memory cgroup does not exist')

    def __init_mem_overhead(self):
        try:
            vram = 0
            xml_desc = self._libvirt_domain.XMLDesc()
            for video_device in XMLET.fromstring(xml_desc).iter('video'):
                vram += int(video_device.find('model').get('vram'))
            qemu_overhead = VCMMDConfig().get_num('VE.VM.QEMUOverhead',
                                                  default=33554432,
                                                  integer=True, minimum=0)
            self.mem_overhead = qemu_overhead + (vram << 10)
        except libvirtError as err:
            raise Error(err)
        except (XMLET.ParseError, ValueError) as err:
            raise Error("Failed to parse VM's XML descriptor: %s" % err)

    def activate(self):
        self.__init_libvirt_domain()
        self.__init_cgroup()
        self.__init_mem_overhead()
        super(VM, self).activate()

    def idle_ratio(self, age=0):
        # Besides the VM memory itself, there might be file caches in the VM's
        # cgroup. We are not interested in keeping them.
        return self._memcg.get_idle_mem_portion_anon(age)

    def _fetch_mem_stats(self):
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
                'wss': stat.get('working_set_size', -1) << 10,
                'swapin': stat.get('swap_in', -1) << 10,
                'swapout': stat.get('swap_out', -1) << 10,
                'minflt': stat.get('minor_fault', -1),
                'majflt': stat.get('major_fault', -1)}

    def _fetch_io_stats(self):
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

    def _apply_config(self, config):
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
