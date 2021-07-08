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
import time

from contextlib import suppress
from vcmmd.error import VCMMDError


class _LibvirtProxy():

    def __init__(self, libvirt_endpoint):
        self.__end_point = libvirt_endpoint
        self.__logger = logging.getLogger('vcmmd.util.libvirt')
        self.__connect()

    def __connect(self):
        self.__logger.debug('Connecting to libvirt')
        attempts_to_connect = 120
        while attempts_to_connect > 0:
            try:
                self.__logger.debug('Connecting to libvirt')
                self.__conn = libvirt.open(self.__end_point)
            except libvirt.libvirtError as e:
                self.__logger.error('Can\'t connect to libvirtd: %s', e)
                time.sleep(1)
                attempts_to_connect -= 1
            else:
                break
        else:
            raise VCMMDError('Can\'t connect to libvirtd')

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


def __get_proxy(name):
    if not __proxies[name]:
        __proxies[name] = _LibvirtProxy(name + ':///system')
    return __proxies[name]


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
