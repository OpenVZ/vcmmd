import dbus
import dbus.service
import dbus.mainloop.glib
import gobject

from vcmmd.ldmgr import Error as LoadManagerError


def _handle_error(fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except LoadManagerError as err:
        return err.errno
    else:
        return 0


class _LoadManagerObject(dbus.service.Object):

    PATH = '/LoadManager'
    BUS_NAME = 'com.virtuozzo.vcmmd'
    IFACE = 'com.virtuozzo.vcmmd.LoadManager'

    def __init__(self, ldmgr):
        self.ldmgr = ldmgr

        bus = dbus.SystemBus()
        bus_name = dbus.service.BusName(self.BUS_NAME, bus)
        super(_LoadManagerObject, self).__init__(bus_name, self.PATH)

    @dbus.service.method(IFACE, in_signature='si(tttt)', out_signature='i')
    def RegisterVE(self, ve_name, ve_type, ve_config):
        return _handle_error(self.ldmgr.register_ve,
                             ve_name, ve_type, ve_config)

    @dbus.service.method(IFACE, in_signature='s', out_signature='i')
    def CommitVE(self, ve_name):
        return _handle_error(self.ldmgr.commit_ve, ve_name)

    @dbus.service.method(IFACE, in_signature='s(tttt)', out_signature='i')
    def UpdateVE(self, ve_name, ve_config):
        return _handle_error(self.ldmgr.update_ve, ve_name, ve_config)

    @dbus.service.method(IFACE, in_signature='s', out_signature='i')
    def UnregisterVE(self, ve_name):
        return _handle_error(self.ldmgr.unregister_ve, ve_name)

    @dbus.service.method(IFACE, in_signature='', out_signature='a(sib(tttt))')
    def GetAllRegisteredVEs(self):
        return self.ldmgr.get_all_registered_ves()


def init(ldmgr):
    gobject.threads_init()
    dbus.mainloop.glib.threads_init()
    dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)

    global _mainloop
    _mainloop = gobject.MainLoop()

    global _ldmgr_obj
    _ldmgr_obj = _LoadManagerObject(ldmgr)


def run():
    _mainloop.run()


def quit():
    _mainloop.quit()
