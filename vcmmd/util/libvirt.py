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
import psutil
import libvirt

from vcmmd.cgroup import pid_cgroup
from vcmmd.util.singleton import Singleton


class virConnectionProxy(object):
    ''' Singleton for handle connection to libvirt.
    An instance of this class will delegate all its method calls to the
    underlying virConnect, (re)establishing connection to libvirt whenever
    necessary.
    '''

    __metaclass__ = Singleton

    def __init__(self):
        self.__logger = logging.getLogger('vcmmd.libvirt')
        self.__connect()

    def __open_connection(self):
        self.__conn = libvirt.open('qemu:///system')

    def __connect(self):
        self.__logger.debug('Connecting to libvirt')
        self.__open_connection()

    def __reconnect(self):
        conn = self.__conn

        self.__logger.debug('Connection to libvirt broken, reconnecting')
        self.__open_connection()

        # Close the stale connection once we've established a new one.
        if conn is not None:
            try:
                conn.close()
            except libvirt.libvirtError:
                pass  # don't bother about errors on close

    def __handle_conn_err(self):
        if self.__conn.isAlive():
            return False

        # Looks like connection is broken. Try to reconnect.
        self.__reconnect()

        return True

    def __getattr__(self, name):
        attr = getattr(self.__conn, name)
        def wrapper(*args, **kwargs):
            try:
                return attr(*args, **kwargs)
            except libvirt.libvirtError:
                if not self.__handle_conn_err():
                    raise
                return attr(*args, **kwargs)
            except:
                raise
        return wrapper


class virDomainProxy(object):
    '''Proxy to libvirt.virDomain with reconnect support.

    An instance of this class will delegate all its method calls to the
    underlying virDomain, (re)establishing connection to libvirt whenever
    necessary.
    '''

    def __init__(self, uuid):
        self.__logger = logging.getLogger('vcmmd.libvirt')
        self.__uuid = uuid
        self.__conn = virConnectionProxy()
        # Let's delegate handling connection problems to virConnectionProxy
        self.__dom = self.__conn.lookupByUUIDString(self.__uuid)

    def __getattr__(self, name):
        attr = getattr(self.__dom, name)
        def wrapper(*args, **kwargs):
            try:
                return attr(*args, **kwargs)
            except libvirt.libvirtError:
                self.__dom = self.__conn.lookupByUUIDString(self.__uuid)
                return attr(*args, **kwargs)
            except:
                raise
        return wrapper


def lookup_qemu_machine_pid(name):
    '''Given the name of a QEMU machine, lookup its PID.
    '''
    for proc in psutil.process_iter():
        cmd = proc.cmdline()
        if not cmd or not cmd[0].endswith('qemu-kvm'):
            continue
        name_idx = cmd.index('-name') + 1
        if (name_idx < len(cmd) and
                cmd[name_idx].split(',')[0] == name):
            return proc.pid
    raise OSError("No such process: '%s'" % name)


def lookup_qemu_machine_cgroup(name):
    pid = lookup_qemu_machine_pid(name)
    return pid_cgroup(pid)
