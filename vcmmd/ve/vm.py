from __future__ import absolute_import

from vcmmd.cgroup import MemoryCgroup
from vcmmd.ve import VE, Error, types as ve_types, MemStats, IOStats

import libvirt


class LibvirtError(Error):
    pass


class CgroupError(Error):
    pass


class VM(VE):

    VE_TYPE = ve_types.VM
    VE_TYPE_NAME = 'VM'

    _MEMSTAT_PERIOD = 5  # seconds

    _libvirt_conn = None

    def activate(self):
        try:
            if not VM._libvirt_conn:
                VM._libvirt_conn = libvirt.open('qemu:///system')

            self._libvirt_domain = VM._libvirt_conn.\
                lookupByUUIDString(self.name)

            # libvirt must be explicitly told to collect memory statistics
            self._libvirt_domain.setMemoryStatsPeriod(self._MEMSTAT_PERIOD)
        except libvirt.libvirtError as err:
            raise LibvirtError(err)

        # QEMU places every virtual machine in its own memory cgroup under
        # machine.slice
        self._memcg = MemoryCgroup('machine.slice/machine-qemu\\x2d%s.scope' %
                                   self._libvirt_domain.name())

        if not self._memcg.exists():
            raise CgroupError('VM memory cgroup does not exist')

        super(VM, self).activate()

    def _fetch_mem_stats(self):
        try:
            stat = self._libvirt_domain.memoryStats()
        except libvirt.libvirtError as err:
            raise LibvirtError(err)

        try:
            used = max(stat['available'] - stat['unused'], 0)
        except KeyError:
            # Guest driver is not installed? Use qemu process rss then.
            used = stat['rss']

        # libvirt reports memory values in kB, so we need to convert them to
        # bytes
        return MemStats(actual=stat['actual'] << 10,
                        rss=stat['rss'] << 10,
                        used=used << 10,
                        minflt=stat.get('minor_fault', 0),
                        majflt=stat.get('major_fault', 0))

    def _fetch_io_stats(self):
        try:
            stat = self._libvirt_domain.blockStats('')
        except libvirt.libvirtError as err:
            raise LibvirtError(err)

        return IOStats(rd_req=stat[0],
                       rd_bytes=stat[1],
                       wr_req=stat[2],
                       wr_bytes=stat[3])

    def _set_mem_low(self, value):
        try:
            self._memcg.write_mem_low(value)
        except IOError as err:
            raise CgroupError(err)

    def _set_mem_high(self, value):
        value >>= 10  # libvirt wants kB
        try:
            self._libvirt_domain.setMemory(value)
        except libvirt.libvirtError as err:
            raise LibvirtError(err)

    def _set_mem_max(self, value):
        value >>= 10  # libvirt wants kB
        try:
            # If value is greater than MaxMemory, we have to initiate memory
            # hotplug to increase the limit.
            #
            # We ignore limit decrease here, because memory hotunplug is not
            # expected to work. Memory allocation is supposed to be decreased
            # by the policy in this case.
            if value > self._libvirt_domain.maxMemory():
                self._libvirt_domain.setMaxMemory(value)
        except libvirt.libvirtError as err:
            raise LibvirtError(err)
