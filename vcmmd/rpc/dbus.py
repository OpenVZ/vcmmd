from __future__ import absolute_import

import threading

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


def _config_dict_to_kv_array(dict_):
    '''Convert a config dictionary to an array of pairs. The first value of
    each pair is the index of a config parameter in VEConfig struct while the
    second value is the value of the config parameter. Used to prepare a config
    for passing to dbus.
    '''
    kv_array = []
    for k in range(len(VEConfig._fields)):
        field_name = VEConfig._fields[k]
        try:
            kv_array.append((k, dict_[field_name]))
        except KeyError:
            pass  # No value is OK.
    return kv_array


def _config_dict_from_array(arr):
    return _config_dict_from_kv_array((i, arr[i])
                                      for i in range(len(arr)))


class _LoadManagerObject(dbus.service.Object):

    PATH = '/LoadManager'
    BUS_NAME = 'com.virtuozzo.vcmmd'
    IFACE = 'com.virtuozzo.vcmmd.LoadManager'

    def __init__(self, ldmgr):
        self.ldmgr = ldmgr

        bus = dbus.SystemBus()
        bus_name = dbus.service.BusName(self.BUS_NAME, bus)
        super(_LoadManagerObject, self).__init__(bus_name, self.PATH)

    @dbus.service.method(IFACE, in_signature='sia(qt)', out_signature='i')
    def RegisterVE(self, ve_name, ve_type, ve_config):
        ve_config = _config_dict_from_kv_array(ve_config)
        try:
            self.ldmgr.register_ve(ve_name, ve_type, ve_config)
        except LoadManagerError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='s', out_signature='i')
    def ActivateVE(self, ve_name):
        try:
            self.ldmgr.activate_ve(ve_name)
        except LoadManagerError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='sa(qt)', out_signature='i')
    def UpdateVE(self, ve_name, ve_config):
        ve_config = _config_dict_from_kv_array(ve_config)
        try:
            self.ldmgr.update_ve(ve_name, ve_config)
        except LoadManagerError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='s', out_signature='i')
    def DeactivateVE(self, ve_name):
        try:
            self.ldmgr.deactivate_ve(ve_name)
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


class RPCProxy(object):

    def __init__(self):
        bus = dbus.SystemBus()
        obj = bus.get_object(_LoadManagerObject.BUS_NAME,
                             _LoadManagerObject.PATH)
        self._iface = dbus.Interface(obj, _LoadManagerObject.IFACE)

    def register_ve(self, ve_name, ve_type, ve_config):
        err = self._iface.RegisterVE(ve_name, ve_type,
                                     _config_dict_to_kv_array(ve_config))
        if err:
            raise LoadManagerError(err)

    def activate_ve(self, ve_name):
        err = self._iface.ActivateVE(ve_name)
        if err:
            raise LoadManagerError(err)

    def update_ve(self, ve_name, ve_config):
        err = self._iface.UpdateVE(ve_name,
                                   _config_dict_to_kv_array(ve_config))
        if err:
            raise LoadManagerError(err)

    def deactivate_ve(self, ve_name):
        err = self._iface.DeactivateVE(ve_name)
        if err:
            raise LoadManagerError(err)

    def unregister_ve(self, ve_name):
        err = self._iface.UnregisterVE(ve_name)
        if err:
            raise LoadManagerError(err)

    def get_all_registered_ves(self):
        lst = self._iface.GetAllRegisteredVEs()
        return [(name, typ, actv, _config_dict_from_array(cfg))
                for name, typ, actv, cfg in lst]
