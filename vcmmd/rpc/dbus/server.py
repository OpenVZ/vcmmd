# Copyright (c) 2016 Parallels IP Holdings GmbH
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
# Our contact details: Parallels IP Holdings GmbH, Vordergasse 59, 8200
# Schaffhausen, Switzerland.

from __future__ import absolute_import

import logging
import threading

import dbus
import dbus.service
import dbus.mainloop.glib
import gobject

from vcmmd.error import VCMMDError
from vcmmd.ve_config import VEConfig
from vcmmd.rpc.dbus.common import PATH, BUS_NAME, IFACE


class _LoadManagerObject(dbus.service.Object):

    def __init__(self, ldmgr):
        self.ldmgr = ldmgr

        bus = dbus.SystemBus()
        bus_name = dbus.service.BusName(BUS_NAME, bus)
        super(_LoadManagerObject, self).__init__(bus_name, PATH)

    @dbus.service.method(IFACE, in_signature='sia(qts)u', out_signature='i')
    def RegisterVE(self, ve_name, ve_type, ve_config, flags):
        ve_name = str(ve_name)
        ve_type = int(ve_type)
        ve_config = VEConfig.from_array(ve_config)
        try:
            self.ldmgr.register_ve(ve_name, ve_type, ve_config)
        except VCMMDError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='su', out_signature='i')
    def ActivateVE(self, ve_name, flags):
        ve_name = str(ve_name)
        try:
            self.ldmgr.activate_ve(ve_name)
        except VCMMDError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='sa(qts)u', out_signature='i')
    def UpdateVE(self, ve_name, ve_config, flags):
        ve_name = str(ve_name)
        ve_config = VEConfig.from_array(ve_config)
        try:
            self.ldmgr.update_ve_config(ve_name, ve_config)
        except VCMMDError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='s', out_signature='i')
    def DeactivateVE(self, ve_name):
        ve_name = str(ve_name)
        try:
            self.ldmgr.deactivate_ve(ve_name)
        except VCMMDError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='s', out_signature='i')
    def UnregisterVE(self, ve_name):
        ve_name = str(ve_name)
        try:
            self.ldmgr.unregister_ve(ve_name)
        except VCMMDError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='s', out_signature='ib')
    def IsVEActive(self, ve_name):
        ve_name = str(ve_name)
        try:
            return (0, self.ldmgr.is_ve_active(ve_name))
        except VCMMDError as err:
            return (err.errno, False)

    @dbus.service.method(IFACE, in_signature='s', out_signature='ia(qts)')
    def GetVEConfig(self, ve_name):
        ve_name = str(ve_name)
        try:
            return (0, self.ldmgr.get_ve_config(ve_name))
        except VCMMDError as err:
            return (err.errno, [])

    @dbus.service.method(IFACE, in_signature='', out_signature='a(siba(qts))')
    def GetAllRegisteredVEs(self):
        return self.ldmgr.get_all_registered_ves()

    @dbus.service.method(IFACE, in_signature='i', out_signature='')
    def SetLogLevel(self, lvl):
        logging.getLogger('vcmmd').setLevel(lvl)

    @dbus.service.method(IFACE, in_signature='', out_signature='s')
    def GetCurrentPolicy(self):
        return self.ldmgr.get_current_policy()

    @dbus.service.method(IFACE, in_signature='b', out_signature='s')
    def GetConfig(self, j):
        return self.ldmgr.get_config(j)

    @dbus.service.method(IFACE, in_signature='b', out_signature='s')
    def GetPolicyCounts(self, j):
        return self.ldmgr.get_policy_counts(j)

    @dbus.service.method(IFACE, in_signature='', out_signature='ia(sx)')
    def GetStats(self, ve_name):
        ve_name = str(ve_name)
        try:
            return (0, self.ldmgr.get_stats(ve_name))
        except VCMMDError as err:
            return (err.errno, [])

    @dbus.service.method(IFACE, in_signature='', out_signature='a(stt)')
    def GetQuotas(self):
        return self.ldmgr.get_quotas()


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
