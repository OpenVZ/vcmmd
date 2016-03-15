from __future__ import absolute_import

import logging
import libvirt


class _virDomainProxyMethod(object):

    def __init__(self, proxy, name):
        self.__proxy = proxy
        self.__name = name

    def __call__(self, *args, **kwargs):
        return self.__proxy._call_real(self.__name, args, kwargs)


class virDomainProxy(object):
    '''Proxy to libvirt.virDomain with reconnect support.

    An instance of this class will delegate all its method calls to the
    underlying virDomain, (re)establishing connection to libvirt whenever
    necessary.
    '''

    __conn = None

    def __init__(self, uuid):
        self.__logger = logging.getLogger('vcmmd.libvirt')
        self.__uuid = uuid

        if self.__conn is None:
            self.__connect()
            # Connect lookups domain, so we're done.
            return

        try:
            self.__lookup_domain()
        except libvirt.libvirtError as err:
            if not self.__handle_conn_err():
                raise err
            # Reconnect lookups domain, so we're done.

    @classmethod
    def __open_connection(cls):
        cls.__conn = libvirt.open('qemu:///system')

    def __lookup_domain(self):
        self.__dom = self.__conn.lookupByUUIDString(self.__uuid)

    def __do_connect(self):
        try:
            self.__open_connection()
        except libvirt.libvirtError as err:
            self.__logger.error('Error connecting to libvirt: %s', err)
            raise

    def __connect(self):
        self.__logger.info('Connecting to libvirt')
        self.__do_connect()
        self.__lookup_domain()

    def __reconnect(self):
        conn = self.__conn

        self.__logger.info('Connection to libvirt broken, reconnecting')
        self.__do_connect()

        # Close the stale connection once we've established a new one.
        if conn is not None:
            try:
                conn.close()
            except libvirt.libvirtError:
                pass  # don't bother about errors on close

        # Domain is stale now. Update it.
        self.__lookup_domain()

    def __handle_conn_err(self):
        if self.__conn.isAlive():
            return False

        # Looks like connection is broken. Try to reconnect.
        self.__reconnect()

        return True

    def __do_call_real(self, name, args, kwargs):
        return getattr(self.__dom, name)(*args, **kwargs)

    def _call_real(self, name, args, kwargs):
        try:
            # Stale connection? Update domain.
            if self.__dom.connect() != self.__conn:
                self.__lookup_domain()

            return self.__do_call_real(name, args, kwargs)
        except libvirt.libvirtError as err:
            if not self.__handle_conn_err():
                raise err

            # Retry after reconnect.
            return self.__do_call_real(name, args, kwargs)

    def __getattr__(self, name):
        attr = getattr(self.__dom, name)
        if not callable(attr):
            return attr
        return _virDomainProxyMethod(self, name)
