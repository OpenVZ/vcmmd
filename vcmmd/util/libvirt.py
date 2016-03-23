from __future__ import absolute_import

import logging
import psutil
import libvirt
from libvirt_qemu import (qemuMonitorCommand,
                          VIR_DOMAIN_QEMU_MONITOR_COMMAND_DEFAULT)

from vcmmd.cgroup import pid_cgroup


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
        except libvirt.libvirtError:
            if not self.__handle_conn_err():
                raise
            # Reconnect lookups domain, so we're done.

    @classmethod
    def __open_connection(cls):
        cls.__conn = libvirt.open('qemu:///system')

    def __lookup_balloon(self):
        path = '/machine/i440fx/pci.0'
        cmd = ('{'
               '    "execute": "qom-list",'
               '    "arguments": {'
               '        "path": "%s"'
               '    }'
               '}' % path)
        out = qemuMonitorCommand(self.__dom, cmd,
                                 VIR_DOMAIN_QEMU_MONITOR_COMMAND_DEFAULT)
        devlist = eval(out)['return']

        for dev in devlist:
            if dev['type'] == 'link<virtio-balloon-pci>':
                self.__balloon_path = path + '/' + dev['name']
                self.__logger.debug('VM %s: balloon at "%s"',
                                    self.__uuid, self.__balloon_path)
                break
        else:
            self.__logger.warn('Could not find balloon for VM %s. '
                               'Some memory statistics may be unavailable' %
                               self.__uuid)
            self.__balloon_path = None

    def __lookup_domain(self):
        self.__dom = self.__conn.lookupByUUIDString(self.__uuid)
        self.__lookup_balloon()

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

    def __check_conn(fn):
        def wrapped(self, *args, **kwargs):
            try:
                # Stale connection? Update domain.
                if self.__dom.connect() != self.__conn:
                    self.__lookup_domain()
                return fn(self, *args, **kwargs)
            except libvirt.libvirtError:
                if not self.__handle_conn_err():
                    raise
                # Retry after reconnect.
                return fn(self, *args, **kwargs)
        return wrapped

    @__check_conn
    def _call_real(self, name, args, kwargs):
        return getattr(self.__dom, name)(*args, **kwargs)

    def __getattr__(self, name):
        return _virDomainProxyMethod(self, name)

    @__check_conn
    def memoryStats(self):
        memstats = self.__dom.memoryStats()

        if self.__balloon_path is None:
            return memstats

        cmd = ('{'
               '    "execute": "qom-get",'
               '    "arguments": {'
               '        "path": "%s",'
               '        "property": "guest-stats"'
               '    }'
               '}' % self.__balloon_path)
        out = qemuMonitorCommand(self.__dom, cmd,
                                 VIR_DOMAIN_QEMU_MONITOR_COMMAND_DEFAULT)
        xstats = eval(out)['return']['stats']

        def export_xstat(tag, name):
            try:
                # libvirt reports in kB, qemu in bytes
                memstats[name] = xstats['x-stat-%04x' % tag] >> 10
            except KeyError:
                pass

        export_xstat(0xfff0, 'memavailable')
        export_xstat(0xfff1, 'committed')

        return memstats


def lookup_qemu_machine_pid(name):
    '''Given the name of a QEMU machine, lookup its PID.
    '''
    for proc in psutil.process_iter():
        cmd = proc.cmdline()
        if not cmd or not cmd[0].endswith('qemu-kvm'):
            continue
        name_idx = cmd.index('-name') + 1
        if name_idx < len(cmd) and cmd[name_idx] == name:
            return proc.pid
    raise OSError("No such process: '%s'" % name)


def lookup_qemu_machine_cgroup(name):
    pid = lookup_qemu_machine_pid(name)
    return pid_cgroup(pid)
