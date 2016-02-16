from __future__ import absolute_import

import threading

import dbus
import dbus.service
import dbus.mainloop.glib
import gobject

from vcmmd.ldmgr import Error as LoadManagerError
from vcmmd.rpc.dbus.common import (PATH, BUS_NAME, IFACE,
                                   ve_config_from_kv_array)


class _LoadManagerObject(dbus.service.Object):

    def __init__(self, ldmgr):
        self.ldmgr = ldmgr

        bus = dbus.SystemBus()
        bus_name = dbus.service.BusName(BUS_NAME, bus)
        super(_LoadManagerObject, self).__init__(bus_name, PATH)

    @dbus.service.method(IFACE, in_signature='sia(qt)', out_signature='i')
    def RegisterVE(self, ve_name, ve_type, ve_config):
        ve_name = str(ve_name)
        ve_type = int(ve_type)
        ve_config = ve_config_from_kv_array(ve_config)
        try:
            self.ldmgr.register_ve(ve_name, ve_type, ve_config)
        except LoadManagerError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='s', out_signature='i')
    def ActivateVE(self, ve_name):
        ve_name = str(ve_name)
        try:
            self.ldmgr.activate_ve(ve_name)
        except LoadManagerError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='sa(qt)', out_signature='i')
    def UpdateVE(self, ve_name, ve_config):
        ve_name = str(ve_name)
        ve_config = ve_config_from_kv_array(ve_config)
        try:
            self.ldmgr.update_ve(ve_name, ve_config)
        except LoadManagerError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='s', out_signature='i')
    def DeactivateVE(self, ve_name):
        ve_name = str(ve_name)
        try:
            self.ldmgr.deactivate_ve(ve_name)
        except LoadManagerError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='s', out_signature='i')
    def UnregisterVE(self, ve_name):
        ve_name = str(ve_name)
        try:
            self.ldmgr.unregister_ve(ve_name)
        except LoadManagerError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='s', out_signature='ib')
    def IsVEActive(self, ve_name):
        ve_name = str(ve_name)
        try:
            return (0, self.ldmgr.is_ve_active(ve_name))
        except LoadManagerError as err:
            return (err.errno, False)

    @dbus.service.method(IFACE, in_signature='s', out_signature='iat')
    def GetVEConfig(self, ve_name):
        ve_name = str(ve_name)
        try:
            return (0, self.ldmgr.get_ve_config(ve_name))
        except LoadManagerError as err:
            return (err.errno, [])

    @dbus.service.method(IFACE, in_signature='', out_signature='a(sibat)')
    def GetAllRegisteredVEs(self):
        return self.ldmgr.get_all_registered_ves()

    @dbus.service.method(IFACE, in_signature='', out_signature='')
    def Dump(self):
        return self.ldmgr.dump()


class RPCServer(object):

    def __init__(self, ldmgr):
        gobject.threads_init()
        dbus.mainloop.glib.threads_init()
        dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)

        self._mainloop = gobject.MainLoop()
        self._mainloop_thread = threading.Thread(target=self._mainloop.run)

        self._ldmgr_obj = _LoadManagerObject(ldmgr)

        self._mainloop_thread.start()

    def shutdown(self):
        self._mainloop.quit()
        self._mainloop_thread.join()
