from __future__ import absolute_import

import dbus

from vcmmd.error import VCMMDError
from vcmmd.ve_config import VEConfig
from vcmmd.rpc.dbus.common import PATH, BUS_NAME, IFACE


class RPCProxy(object):

    def __init__(self):
        bus = dbus.SystemBus()
        obj = bus.get_object(BUS_NAME, PATH)
        self._iface = dbus.Interface(obj, IFACE)

    def register_ve(self, ve_name, ve_type, ve_config, flags):
        err = self._iface.RegisterVE(ve_name, ve_type,
                                     ve_config.as_array(), flags)
        if err:
            raise VCMMDError(err)

    def activate_ve(self, ve_name, flags):
        err = self._iface.ActivateVE(ve_name, flags)
        if err:
            raise VCMMDError(err)

    def update_ve_config(self, ve_name, ve_config, flags):
        err = self._iface.UpdateVE(ve_name, ve_config.as_array(), flags)
        if err:
            raise VCMMDError(err)

    def deactivate_ve(self, ve_name):
        err = self._iface.DeactivateVE(ve_name)
        if err:
            raise VCMMDError(err)

    def unregister_ve(self, ve_name):
        err = self._iface.UnregisterVE(ve_name)
        if err:
            raise VCMMDError(err)

    def get_all_registered_ves(self):
        lst = self._iface.GetAllRegisteredVEs()
        return [(str(name), int(typ), bool(actv), VEConfig.from_tuple(cfg))
                for name, typ, actv, cfg in lst]

    def set_log_level(self, lvl):
        self._iface.SetLogLevel(lvl)

    def get_current_policy(self):
        return self._iface.GetCurrentPolicy()

    def get_stats(self, ve):
        err, stats = self._iface.GetStats(ve)
        if err:
            raise VCMMDError(err)
        return stats
