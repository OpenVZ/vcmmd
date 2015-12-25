from vcmmd.ve import VE, Error, types as ve_types

import libvirt


class LibvirtError(Error):
    pass


class VM(VE):

    VE_TYPE = ve_types.VM
    VE_TYPE_NAME = 'VM'

    _libvirt_conn = None

    def commit(self):
        try:
            if not VM._libvirt_conn:
                VM._libvirt_conn = libvirt.open('qemu:///system')
            self._libvirt_domain = VM._libvirt_conn.lookupByName(self.name)
        except libvirt.libvirtError as err:
            raise LibvirtError(err)

        super(VM, self).commit()

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
