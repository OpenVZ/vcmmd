from __future__ import absolute_import

import dbus
import dbus.service
import dbus.mainloop.glib
import gobject

from vcmmd.ldmgr import Error as LoadManagerError
from vcmmd.ve import Config as VEConfig


def _config_dict_from_kv_array(kv_array):
    '''Convert an array of key-value tuples where key is an index of a config
    parameter in the VEConfig struct to key-value dictionary in which key is
    the name of the corresponding struct entry. Used to convert the input from
    dbus to the form accepted by the LoadManager class.
    '''
    dict_ = {}
    for k, v in kv_array:
        try:
            field_name = VEConfig._fields[k]
        except IndexError:
            # Silently ignore unknown fields in case the config is extended in
            # future
            continue
        dict_[field_name] = v
    return dict_


class _LoadManagerObject(dbus.service.Object):

    PATH = '/LoadManager'
    BUS_NAME = 'com.virtuozzo.vcmmd'
    IFACE = 'com.virtuozzo.vcmmd.LoadManager'

    def __init__(self, ldmgr):
        self.ldmgr = ldmgr

        bus = dbus.SystemBus()
        bus_name = dbus.service.BusName(self.BUS_NAME, bus)
        super(_LoadManagerObject, self).__init__(bus_name, self.PATH)

    @dbus.service.method(IFACE, in_signature='sia(qt)b', out_signature='i')
    def RegisterVE(self, ve_name, ve_type, ve_config, force):
        ve_config = _config_dict_from_kv_array(ve_config)
        try:
            self.ldmgr.register_ve(ve_name, ve_type, ve_config, force)
        except LoadManagerError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='s', out_signature='i')
    def CommitVE(self, ve_name):
        try:
            self.ldmgr.commit_ve(ve_name)
        except LoadManagerError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='sa(qt)b', out_signature='i')
    def UpdateVE(self, ve_name, ve_config, force):
        ve_config = _config_dict_from_kv_array(ve_config)
        try:
            self.ldmgr.update_ve(ve_name, ve_config, force)
        except LoadManagerError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='s', out_signature='i')
    def UnregisterVE(self, ve_name):
        try:
            self.ldmgr.unregister_ve(ve_name)
        except LoadManagerError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='', out_signature='a(sibat)')
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
