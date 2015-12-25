from vcmmd.ve import VE, Error, types as ve_types, MemStats

import libvirt


class LibvirtError(Error):
    pass


class VM(VE):

    VE_TYPE = ve_types.VM
    VE_TYPE_NAME = 'VM'

    _MEMSTAT_PERIOD = 5  # seconds

    _libvirt_conn = None

    def __init__(self, name):
        super(VM, self).__init__(name)
        self._mem_stats_enabled = False

    def commit(self):
        try:
            if not VM._libvirt_conn:
                VM._libvirt_conn = libvirt.open('qemu:///system')
            self._libvirt_domain = VM._libvirt_conn.lookupByName(self.name)
        except libvirt.libvirtError as err:
            raise LibvirtError(err)

        super(VM, self).commit()

    def _fetch_mem_stats(self):
        try:
            if not self._mem_stats_enabled:
                # libvirt must be explicitly told to collect memory statistics
                self._libvirt_domain.setMemoryStatsPeriod(self._MEMSTAT_PERIOD)
                self._mem_stats_enabled = True
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

    def set_mem_high(self, value):
        value >>= 10  # libvirt wants kB
        try:
            self._libvirt_domain.setMemory(value)
        except libvirt.libvirtError as err:
            raise LibvirtError(err)

    def set_mem_max(self, value):
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
