from __future__ import absolute_import

import dbus

from vcmmd.errno import strerror
from vcmmd.rpc.dbus.common import (PATH, BUS_NAME, IFACE,
                                   ve_config_to_kv_array,
                                   ve_config_from_array)


class RPCError(Exception):

    def __init__(self, errno):
        self.errno = errno

    def __str__(self):
        return strerror(self.errno)


class RPCProxy(object):

    def __init__(self):
        bus = dbus.SystemBus()
        obj = bus.get_object(BUS_NAME, PATH)
        self._iface = dbus.Interface(obj, IFACE)

    def register_ve(self, ve_name, ve_type, ve_config):
        err = self._iface.RegisterVE(ve_name, ve_type,
                                     ve_config_to_kv_array(ve_config))
        if err:
            raise RPCError(err)

    def activate_ve(self, ve_name):
        err = self._iface.ActivateVE(ve_name)
        if err:
            raise RPCError(err)

    def update_ve(self, ve_name, ve_config):
        err = self._iface.UpdateVE(ve_name,
                                   ve_config_to_kv_array(ve_config))
        if err:
            raise RPCError(err)

    def deactivate_ve(self, ve_name):
        err = self._iface.DeactivateVE(ve_name)
        if err:
            raise RPCError(err)

    def unregister_ve(self, ve_name):
        err = self._iface.UnregisterVE(ve_name)
        if err:
            raise RPCError(err)

    def get_all_registered_ves(self):
        lst = self._iface.GetAllRegisteredVEs()
        return [(str(name), int(typ), bool(actv), ve_config_from_array(cfg))
                for name, typ, actv, cfg in lst]
