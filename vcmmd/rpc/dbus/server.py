# Copyright (c) 2016-2017, Parallels International GmbH
# Copyright (c) 2017-2022, Virtuozzo International GmbH, All rights reserved
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

import inspect
import logging
import threading
import time
import traceback
from functools import wraps

import dbus
import dbus.service
import dbus.mainloop.glib
from gi.repository import GObject as gobject

from vcmmd.error import VCMMDError
from vcmmd.ve_config import VEConfig
from vcmmd.rpc.dbus.common import PATH, BUS_NAME, IFACE


def _add_logging(fn):
    """Wrap _LoadManagerObject unbound method for logging."""
    @wraps(fn)
    def wrapped(*args, **kwargs):
        load_manager_obj = args[0]
        if not hasattr(load_manager_obj, 'request_num'):
            load_manager_obj.request_num = 0
        logger = logging.getLogger('vcmmd.ldmgr')
        load_manager_obj.request_num += 1
        start = time.time()
        request = f'Request {load_manager_obj.request_num} {fn.__name__}'
        logger.info(f'{request}({", ".join(map(str, args[1:]))}) started')
        try:
            rv = fn(*args, **kwargs)
        except Exception as err:
            logger.error(traceback.format_exc())
            raise
        t = time.time() - start
        logger.info(f'{request} worked {t:.2f}s')
        return rv
    return wrapped


def _log_dbus_methods(klass):
    """Wrap all _LoadManagerObject dbus methods with _add_logging decorator."""
    def is_vcmmd_dbus_method(attr):
        return (inspect.isfunction(attr) and
                attr.__module__ == 'vcmmd.rpc.dbus.server' and
                not attr.__name__.startswith('_'))
    for name, fn in inspect.getmembers(klass, is_vcmmd_dbus_method):
        setattr(klass, name, _add_logging(fn))
    return klass


@_log_dbus_methods
class _LoadManagerObject(dbus.service.Object):

    def __init__(self, ldmgr, bus_name):
        super(_LoadManagerObject, self).__init__(bus_name, PATH)
        self.ldmgr = ldmgr

    @dbus.service.method(IFACE, in_signature='sia(qts)u', out_signature='i')
    def RegisterVE(self, ve_name, ve_type, ve_config, flags):
        ve_config = VEConfig.from_array(ve_config)
        ve_name = str(ve_name)
        ve_type = int(ve_type)
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
        ve_config = VEConfig.from_array(ve_config)
        ve_name = str(ve_name)
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
        try:
            self.ldmgr.unregister_ve(str(ve_name))
        except VCMMDError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='s', out_signature='ib')
    def IsVEActive(self, ve_name):
        try:
            return 0, self.ldmgr.is_ve_active(str(ve_name))
        except VCMMDError as err:
            return err.errno, False

    @dbus.service.method(IFACE, in_signature='s', out_signature='ia(qts)')
    def GetVEConfig(self, ve_name):
        try:
            return 0, self.ldmgr.get_ve_config(str(ve_name))
        except VCMMDError as err:
            return err.errno, []

    @dbus.service.method(IFACE, in_signature='', out_signature='a(siba(qts))')
    def GetAllRegisteredVEs(self):
        return self.ldmgr.get_all_registered_ves()

    @dbus.service.method(IFACE, in_signature='i', out_signature='')
    def SetLogLevel(self, lvl):
        logging.getLogger('vcmmd').setLevel(lvl)

    @dbus.service.method(IFACE, in_signature='', out_signature='s')
    def GetCurrentPolicy(self):
        return self.ldmgr.get_current_policy()

    @dbus.service.method(IFACE, in_signature='', out_signature='s')
    def GetPolicyFromFile(self):
        return self.ldmgr.get_policy_from_file()

    @dbus.service.method(IFACE, in_signature='s', out_signature='i')
    def SwitchPolicy(self, policy_name):
        try:
            self.ldmgr.switch_policy(str(policy_name))
        except VCMMDError as err:
            return err.errno
        else:
            return 0

    @dbus.service.method(IFACE, in_signature='b', out_signature='s')
    def GetConfig(self, full_config):
        return self.ldmgr.get_config(full_config)

    @dbus.service.method(IFACE, in_signature='', out_signature='s')
    def GetPolicyCounts(self):
        return self.ldmgr.get_policy_counts()

    @dbus.service.method(IFACE, in_signature='', out_signature='ia(sx)')
    def GetStats(self, ve_name):
        try:
            return 0, self.ldmgr.get_stats(str(ve_name))
        except VCMMDError as err:
            return err.errno, []

    @dbus.service.method(IFACE, in_signature='', out_signature='a{st}')
    def GetFree(self):
        return self.ldmgr.get_free()


class RPCServer:

    def __init__(self, ldmgr):
        dbus.mainloop.glib.threads_init()
        dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)

        self._mainloop = gobject.MainLoop()
        self._mainloop_thread = threading.Thread(target=self._mainloop.run)

        self._ldmgr_obj = _LoadManagerObject(
            ldmgr, dbus.service.BusName(BUS_NAME, dbus.SystemBus()))

        self._mainloop_thread.start()

    def shutdown(self):
        self._mainloop.quit()
        self._mainloop_thread.join()
