# Copyright (c) 2016-2017, Parallels International GmbH
# Copyright (c) 2017-2024, Virtuozzo International GmbH, All rights reserved
#
# This file is part of OpenVZ. OpenVZ is free software; you can redistribute
# it and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the License,
# or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#
# Our contact details: Virtuozzo International GmbH, Vordergasse 59, 8200
# Schaffhausen, Switzerland.

import dbus

from vcmmd.error import VCMMDError
from vcmmd.ve_config import VEConfig
from vcmmd.rpc.dbus.common import PATH, BUS_NAME, IFACE


class RPCProxy:

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
        return [(str(name), int(typ), bool(actv), VEConfig.from_array(cfg))
                for name, typ, actv, cfg in lst]

    def set_log_level(self, lvl):
        self._iface.SetLogLevel(lvl)

    def get_current_policy(self):
        return self._iface.GetCurrentPolicy()

    def get_policy_from_file(self):
        return self._iface.GetPolicyFromFile()

    def switch_policy(self, policy_name):
        err = self._iface.SwitchPolicy(policy_name)
        if err:
            raise VCMMDError(err)

    def get_config(self, full_config):
        return self._iface.GetConfig(full_config)

    def get_policy_counts(self):
        return self._iface.GetPolicyCounts()

    def get_stats(self, ve):
        err, stats = self._iface.GetStats(ve)
        if err:
            raise VCMMDError(err)
        return stats

    def get_free(self):
        return self._iface.GetFree()
