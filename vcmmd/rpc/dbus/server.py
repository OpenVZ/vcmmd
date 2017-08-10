# Copyright (c) 2016-2017, Parallels International GmbH
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
# Our contact details: Parallels International GmbH, Vordergasse 59, 8200
# Schaffhausen, Switzerland.

from __future__ import absolute_import

import logging
import threading
import time

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
        self.logger = logging.getLogger('vcmmd.ldmgr')
        self.request_num = 0

    def _log(self, fn):
        fname = fn.func_name

        def wrapped(*args, **kwargs):
            start = time.time()
            self.request_num += 1
            request = "Request %d %s" % (self.request_num, fname)
            self.logger.info("%s(%s) started" % (request, ', '.join(
                    map(str, list(args[1:]) + kwargs.items()))))
            ret = fn(*args, **kwargs)
            t = time.time() - start
            self.logger.info("%s worked %.2fs" % (request, t))
            return ret
        return wrapped

    @dbus.service.method(IFACE, in_signature='sia(qts)u', out_signature='i')
    def RegisterVE(self, ve_name, ve_type, ve_config, flags):
        ve_config = VEConfig.from_array(ve_config)
        @self._log
        def RegisterVE(self, ve_name, ve_type, ve_config, flags):
            ve_name = str(ve_name)
            ve_type = int(ve_type)
            try:
                self.ldmgr.register_ve(ve_name, ve_type, ve_config)
            except VCMMDError as err:
                return err.errno
            else:
                return 0
        return RegisterVE(self, ve_name, ve_type, ve_config, flags)

    @dbus.service.method(IFACE, in_signature='su', out_signature='i')
    def ActivateVE(self, ve_name, flags):
        @self._log
        def ActivateVE(self, ve_name, flags):
            ve_name = str(ve_name)
            try:
                self.ldmgr.activate_ve(ve_name)
            except VCMMDError as err:
                return err.errno
            else:
                return 0
        return ActivateVE(self, ve_name, flags)

    @dbus.service.method(IFACE, in_signature='sa(qts)u', out_signature='i')
    def UpdateVE(self, ve_name, ve_config, flags):
        ve_config = VEConfig.from_array(ve_config)
        @self._log
        def UpdateVE(self, ve_name, ve_config, flags):
            ve_name = str(ve_name)
            try:
                self.ldmgr.update_ve_config(ve_name, ve_config)
            except VCMMDError as err:
                return err.errno
            else:
                return 0
        return UpdateVE(self, ve_name, ve_config, flags)

    @dbus.service.method(IFACE, in_signature='s', out_signature='i')
    def DeactivateVE(self, ve_name):
        @self._log
        def DeactivateVE(self, ve_name):
            ve_name = str(ve_name)
            try:
                self.ldmgr.deactivate_ve(ve_name)
            except VCMMDError as err:
                return err.errno
            else:
                return 0
        return DeactivateVE(self, ve_name)

    @dbus.service.method(IFACE, in_signature='s', out_signature='i')
    def UnregisterVE(self, ve_name):
        @self._log
        def UnregisterVE(self, ve_name):
            ve_name = str(ve_name)
            try:
                self.ldmgr.unregister_ve(ve_name)
            except VCMMDError as err:
                return err.errno
            else:
                return 0
        return UnregisterVE(self, ve_name)

    @dbus.service.method(IFACE, in_signature='s', out_signature='ib')
    def IsVEActive(self, ve_name):
        @self._log
        def IsVEActive(self, ve_name):
            ve_name = str(ve_name)
            try:
                return (0, self.ldmgr.is_ve_active(ve_name))
            except VCMMDError as err:
                return (err.errno, False)
        return IsVEActive(self, ve_name)

    @dbus.service.method(IFACE, in_signature='s', out_signature='ia(qts)')
    def GetVEConfig(self, ve_name):
        @self._log
        def GetVEConfig(self, ve_name):
            ve_name = str(ve_name)
            try:
                return (0, self.ldmgr.get_ve_config(ve_name))
            except VCMMDError as err:
                return (err.errno, [])
        return GetVEConfig(self, ve_name)

    @dbus.service.method(IFACE, in_signature='', out_signature='a(siba(qts))')
    def GetAllRegisteredVEs(self):
        @self._log
        def GetAllRegisteredVEs(self):
            return self.ldmgr.get_all_registered_ves()
        return GetAllRegisteredVEs(self)

    @dbus.service.method(IFACE, in_signature='i', out_signature='')
    def SetLogLevel(self, lvl):
        @self._log
        def SetLogLevel(self, lvl):
            logging.getLogger('vcmmd').setLevel(lvl)
        return SetLogLevel(self, lvl)

    @dbus.service.method(IFACE, in_signature='', out_signature='s')
    def GetCurrentPolicy(self):
        @self._log
        def GetCurrentPolicy(self):
            return self.ldmgr.get_current_policy()
        return GetCurrentPolicy(self)

    @dbus.service.method(IFACE, in_signature='', out_signature='s')
    def GetPolicyFromFile(self):
        @self._log
        def GetPolicyFromFile(self):
            return self.ldmgr.get_policy_from_file()
        return GetPolicyFromFile(self)

    @dbus.service.method(IFACE, in_signature='s', out_signature='i')
    def SwitchPolicy(self, policy_name):
        @self._log
        def SwitchPolicy(self, policy_name):
            try:
                self.ldmgr.switch_policy(policy_name)
            except VCMMDError as err:
                return err.errno
            else:
                return 0
        return SwitchPolicy(self, policy_name)

    @dbus.service.method(IFACE, in_signature='b', out_signature='s')
    def GetConfig(self, j):
        @self._log
        def GetConfig(self, j):
            return self.ldmgr.get_config(j)
        return GetConfig(self, j)

    @dbus.service.method(IFACE, in_signature='b', out_signature='s')
    def GetPolicyCounts(self, j):
        @self._log
        def GetPolicyCounts(self, j):
            return self.ldmgr.get_policy_counts(j)
        return GetPolicyCounts(self, j)

    @dbus.service.method(IFACE, in_signature='', out_signature='ia(sx)')
    def GetStats(self, ve_name):
        @self._log
        def GetStats(self, ve_name):
            ve_name = str(ve_name)
            try:
                return (0, self.ldmgr.get_stats(ve_name))
            except VCMMDError as err:
                return (err.errno, [])
        return GetStats(self, ve_name)

    @dbus.service.method(IFACE, in_signature='', out_signature='a{st}')
    def GetFree(self):
        @self._log
        def GetFree(self):
            return self.ldmgr.get_free()
        return GetFree(self)


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
