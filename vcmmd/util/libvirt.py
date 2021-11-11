# Copyright (c) 2016-2017, Parallels International GmbH
# Copyright (c) 2017-2021, Virtuozzo International GmbH, All rights reserved
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

import logging
import libvirt
import subprocess
import time

from contextlib import suppress


def _driver_installed(driver_name: str) -> bool:
    cp = subprocess.run(['rpm', '-q', f'libvirt-daemon-driver-{driver_name}'])
    return cp.returncode == 0


class _LibvirtProxy():

    def __init__(self, driver_name):
        self.__driver_name = driver_name
        self.__logger = logging.getLogger('vcmmd.util.libvirt')
        self.__connect()

    def __connect(self):
        self.__logger.debug('Connecting to libvirt')
        if not _driver_installed(self.__driver_name):
            raise LookupError(f'libvirt-{self.__driver_name} is not found')
        libvirt_endpoint = self.__driver_name + ':///system'
        attempts_to_connect = 120
        while attempts_to_connect > 0:
            try:
                self.__logger.debug('Connecting to libvirt')
                self.__conn = libvirt.open(libvirt_endpoint)
            except libvirt.libvirtError as e:
                self.__logger.error('Can\'t connect to libvirtd: %s', e)
                time.sleep(1)
                attempts_to_connect -= 1
            else:
                break
        else:
            raise Exception('Can\'t connect to libvirtd')

    def __is_connection_error(self):
        if self.__conn.isAlive():
            return False
        with suppress(libvirt.libvirtError):
            self.__conn.close()
        self.__logger.debug('Connection to libvirt broken')
        self.__connect()
        return True

    def __getattr__(self, name):
        attr = getattr(self.__conn, name)

        def wrapper(*args, **kwargs):
            try:
                return attr(*args, **kwargs)
            except libvirt.libvirtError:
                if self.__is_connection_error():
                    return attr(*args, **kwargs)
        return wrapper


__proxies = {
    'qemu': None,
    'vzct': None,
}


def __get_proxy(driver_name):
    if not __proxies[driver_name]:
        __proxies[driver_name] = _LibvirtProxy(driver_name)
    return __proxies[driver_name]


def get_qemu_proxy():
    return __get_proxy('qemu')


def get_vzct_proxy():
    return __get_proxy('vzct')


class VirtDomainProxy:
    """
    Proxy to libvirt.virDomain with reconnect support.

    An instance of this class will delegate all its method calls to the
    underlying virDomain, (re)establishing connection to libvirt whenever
    necessary.
    """
    def __init__(self, uuid, libvirt_proxy=None):
        self.__logger = logging.getLogger('vcmmd.libvirt')
        self.__uuid = uuid
        self.__conn = libvirt_proxy or get_qemu_proxy()
        self.__dom = self.__conn.lookupByUUIDString(self.__uuid)

    def __getattr__(self, name):
        attr = getattr(self.__dom, name)

        def wrapper(*args, **kwargs):
            try:
                return attr(*args, **kwargs)
            except libvirt.libvirtError:
                self.__dom = self.__conn.lookupByUUIDString(self.__uuid)
                return attr(*args, **kwargs)
        return wrapper
