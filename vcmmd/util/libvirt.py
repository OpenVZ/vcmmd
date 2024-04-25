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

import functools
import os
import logging
import libvirt
from libvirt import libvirtError
import time

from contextlib import suppress


logger = logging.getLogger(__name__)


class _LibvirtProxy:

    def __init__(self, endpoint):
        self.__endpoint = endpoint
        self.__conn = None
        self.__connect()

    def __connect(self):
        if self.__conn:
            with suppress(libvirtError):
                self.__conn.close()
        retry_num = 120
        while retry_num > 0:
            try:
                logger.debug(f'libvirtd.open("{self.__endpoint}")')
                self.__conn = libvirt.open(self.__endpoint)
            except libvirtError as e:
                logger.error('Failed to connect to libvirtd: %s', e)
                time.sleep(1)
                retry_num -= 1
            else:
                break
        else:
            raise Exception('Failed connect to libvirtd')

    def __is_connection_error(self):
        if self.__conn.isAlive():
            return False
        logger.debug('Connection to libvirtd broken')
        self.__connect()
        return True

    def __attr(self, name):
        return getattr(self.__conn, name)

    def __getattr__(self, name):
        def wrapped_attr(*args, **kwargs):
            try:
                return self.__attr(name)(*args, **kwargs)
            except libvirtError:
                if self.__is_connection_error():
                    return self.__attr(name)(*args, **kwargs)
                else:
                    raise
        return wrapped_attr


@functools.lru_cache()
def get_qemu_proxy():
    return _LibvirtProxy('qemu:///system')


@functools.lru_cache()
def get_vzct_proxy():
    if not os.path.exists(
            '/usr/lib64/libvirt/connection-driver/libvirt_driver_vzct.so'):
        raise LookupError('vzct driver is not found')
    return _LibvirtProxy('vzct:///system')


class VirtDomainProxy:
    """
    Proxy to libvirt.virDomain with reconnect support.

    An instance of this class will delegate all its method calls to the
    underlying virDomain, (re)establishing connection to libvirt whenever
    necessary.
    """
    def __init__(self, uuid, libvirt_proxy=None):
        self.__uuid = uuid
        self.__conn = libvirt_proxy or get_qemu_proxy()
        self.__dom = self.__conn.lookupByUUIDString(self.__uuid)

    def __getattr__(self, name):
        def wrapped_attr(*args, **kwargs):
            try:
                return getattr(self.__dom, name)(*args, **kwargs)
            except libvirtError:
                self.__dom = self.__conn.lookupByUUIDString(self.__uuid)
                return getattr(self.__dom, name)(*args, **kwargs)
        return wrapped_attr


def list_active_domains():
    domains = []
    for getter in get_vzct_proxy, get_qemu_proxy:
        try:
            domains += getter().listAllDomains(
                libvirt.VIR_CONNECT_LIST_DOMAINS_ACTIVE)
        except LookupError as e:
            logger.warning(f'{getter.__name__} failed: {e}')
    return domains
