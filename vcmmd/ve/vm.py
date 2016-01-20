from __future__ import absolute_import

from libvirt import libvirtError

from vcmmd.cgroup import MemoryCgroup
from vcmmd.ve import VE, Error, types as ve_types, MemStats, IOStats
from vcmmd.util.libvirt import virDomainProxy


class LibvirtError(Error):
    pass


class CgroupError(Error):
    pass


class VM(VE):

    VE_TYPE = ve_types.VM

    _MEMSTAT_PERIOD = 5  # seconds

    def activate(self):
        try:
            self._libvirt_domain = virDomainProxy(self.name)

            # libvirt must be explicitly told to collect memory statistics
            self._libvirt_domain.setMemoryStatsPeriod(self._MEMSTAT_PERIOD)

            dom_name = self._libvirt_domain.name()
        except libvirtError as err:
            raise LibvirtError(err)

        # QEMU places every virtual machine in its own memory cgroup under
        # machine.slice
        self._memcg = MemoryCgroup('machine.slice/machine-qemu\\x2d%s.scope' %
                                   dom_name)

        if not self._memcg.exists():
            raise CgroupError('VM memory cgroup does not exist')

        super(VM, self).activate()

    def idle_ratio(self, age=0):
        # Besides the VM memory itself, there might be file caches in the VM's
        # cgroup. We are not interested in keeping them.
        return self._memcg.get_idle_mem_portion_anon(age)

    def _fetch_mem_stats(self):
        try:
            stat = self._libvirt_domain.memoryStats()
        except libvirtError as err:
            raise LibvirtError(err)

        # libvirt reports memory values in kB, so we need to convert them to
        # bytes
        return MemStats(actual=stat.get('actual', -1) << 10,
                        rss=stat.get('rss', -1) << 10,
                        available=stat.get('available', -1) << 10,
                        unused=stat.get('unused', -1) << 10,
                        swapin=stat.get('swap_in', -1) << 10,
                        swapout=stat.get('swap_out', -1) << 10,
                        minflt=stat.get('minor_fault', -1),
                        majflt=stat.get('major_fault', -1))

    def _fetch_io_stats(self):
        try:
            stat = self._libvirt_domain.blockStats('')
        except libvirtError as err:
            raise LibvirtError(err)

        return IOStats(rd_req=stat[0],
                       rd_bytes=stat[1],
                       wr_req=stat[2],
                       wr_bytes=stat[3])

    def _set_mem_target(self, value):
        # Set memory.low to protect the VM from the host pressure.
        try:
            self._memcg.write_mem_low(value)
        except IOError as err:
            raise CgroupError(err)

        # Update current allocation size by inflating/deflating balloon.
        try:
            # libvirt wants kB
            self._libvirt_domain.setMemory(value >> 10)
        except libvirtError as err:
            raise LibvirtError(err)

    def _hotplug_memory(self, value):
        xml = ("<memory model='dimm'>"
               "  <target>"
               "    <size unit='KiB'>{memsize}</size>"
               "    <node>0</node>"
               "  </target>"
               "</memory>").format(memsize=value)
        self._libvirt_domain.attachDevice(xml)

    def _set_mem_max(self, value):
        value >>= 10  # libvirt wants kB
        try:
            # If value is greater than MaxMemory, we have to initiate memory
            # hotplug to increase the limit.
            #
            # We ignore limit decrease here, because memory hotunplug is not
            # expected to work. Memory allocation is supposed to be decreased
            # by the policy in this case.
            max_mem = self._libvirt_domain.maxMemory()
            if value > max_mem:
                self._hotplug_memory(value - max_mem)
        except libvirtError as err:
            raise LibvirtError(err)
