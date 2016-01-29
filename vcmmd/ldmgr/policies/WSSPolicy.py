from __future__ import absolute_import

from vcmmd.ldmgr import Policy
from vcmmd.ve import types as ve_types
import prlsdkapi
import os
from prlsdkapi import consts


class VmGuestSession(object):
    """
    Parent class for VE wrappers. This is bootstraps for prlsdkapi
    """

    _ve_list = []

    def __init__(self, uuid, *args, **kwargs):
        if not hasattr(type(self), '_server'):
            type(self)._init_server()
        self._sdk_ve = None
        self._lookup_ve(uuid)
        if self._sdk_ve is None:
            self._update_ve_list()
            self._lookup_ve(uuid)
        assert self._sdk_ve, 'Lookup VE failed %r' % uuid
        self._os_type = self._sdk_ve.get_os_type()
        self._connected = False

    @classmethod
    def _init_server(cls):
        helper = prlsdkapi.ApiHelper()
        helper.init(consts.PRL_VERSION_7X)
        cls._server = prlsdkapi.Server()
        cls._server.login_local().wait()

    @classmethod
    def _update_ve_list(cls):
        cls._ve_list = cls._server.get_vm_list_ex(consts.PVTF_VM |
                                                  consts.PVTF_CT).wait()

    def _lookup_ve(self, uuid):
        if set(('{', '}')) - set((uuid[0], uuid[-1])):
            uuid = '{%s}' % uuid
        for ve in self._ve_list:
            if ve.get_uuid() == uuid:
                self._sdk_ve = ve
                break

    def disconnect(self):
        if not self._connected:
            return
        try:
            self._sdk_veguest.logout().wait()
            self._sdk_ve.disconnect()
        except prlsdkapi.PrlSDKError, e:
            pass
        self._connected = False

    def connect(self):
        if self._connected:
            return
        try:
            sdk_user = 'root'
            self._sdk_ve.connect(0).wait()
            result = self._sdk_ve.login_in_guest(sdk_user, '', 0).wait()
            self._sdk_veguest = result.get_param()
            self._connected = True
        except prlsdkapi.PrlSDKError, e:
            self._connected = False

    def _run_program(self, cmd, **kw):
        args = [cmd] and isinstance(cmd, basestring) or cmd
        args_list = prlsdkapi.StringList()
        for arg in args[1:]:
            args_list.add_item(arg)
        return self._sdk_veguest.run_program(args[0], args_list,
                                             prlsdkapi.StringList(), **kw)

    def getstatusoutput(self, cmd):
        status = -1
        self.connect()

        if not self._connected:
            return status, None

        r, nStdout = os.pipe()
        r_fo = os.fdopen(r, 'r')
        try:
            status = self._run_program(cmd, nFlags=consts.PFD_STDOUT,
                                       nStdout=nStdout).wait()
            status = status.get_param().get_param(0).to_int32()
            os.close(nStdout)
            nStdout = None
            # should be in thread
            out = r_fo.read()
        except prlsdkapi.PrlSDKError, e:
            return status, None
        finally:
            nStdout is not None and os.close(nStdout)
            r_fo.close()
            self.disconnect()
        return status, out


class _VEPrivate(object):

    __UNITS = 1 << 20  # MB

    _AVG_WINDOW = 10

    _ALIGN = 4 * __UNITS
    _MIN_GAP = 64 * __UNITS

    # thresholds/fine/rewards should be tuneable in case
    # we discover later that the _choose_gap is still
    # too aggressive in some workloads
    _IO_THRESH = 20
    _PGFLT_THRESH = 20
    _SWAPIN_THRESH = 20
    _DELTA_THRESHOLD = 32 * __UNITS

    _MEM_FINE = 32 * __UNITS
    _IO_REWARD = 4.
    _PGFLT_REWARD = 8.
    _SWAPEXCH_REWARD = 8.
    _POSITIVE_REWARD = -8.

    _DOWNHYSTERESIS = 8
    _UPHYSTERESIS = 1

    def __init__(self, ve):
        self._ve = ve
        self._ve_session = VmGuestSession(ve.name)

        self.quota = ve.config.effective_limit

        self._io = 0
        self._io_avg = 0

        self._pgflt = 0
        self._pgflt_avg = 0

        # _prev_gap is some empirical initial value.
        # It doesn't influence the final result
        self._prev_gap = self._MIN_GAP
        self._prev_size = None

    def _update_stats(self):
        self._io = self._ve.io_stats.rd_req + self._ve.io_stats.wr_req
        self._pgflt = self._ve.mem_stats.majflt
        self._swapin = self._ve.mem_stats.swapin
        self._swapout = self._ve.mem_stats.swapout

    def _update_add_stat(self):
        {consts.PVS_GUEST_TYPE_LINUX: self._update_linux_stat,
         consts.PVS_GUEST_TYPE_WINDOWS: self._update_win_stat}[self._ve_session._os_type]()

    def _update_win_stat(self):
        pass

    def _update_linux_stat(self):
        self.linux_memstat = {}
        status, out = self._ve_session.getstatusoutput(['cat',
                                                        '/proc/meminfo'])
        if status:
            return
        for line in out.splitlines():
            line = line.split()
            if not line:
                continue
            self.linux_memstat[line[0].strip(':')] = int(line[1]) << 10

    def update(self):
        self._update_stats()
        self._update_add_stat()
        self._update_quota()

    def _align(self, val):
        val = int(val)
        val &= ~(self._ALIGN - 1)
        return val

    def _choose_gap(self):
        '''
        Put a fine or a prize for the previous change
        '''
        delta = (((self._swapin > self._SWAPIN_THRESH and
                   self._swapout > self._SWAPIN_THRESH) * self._SWAPEXCH_REWARD +

                  (self._pgflt > self._PGFLT_THRESH) * self._PGFLT_REWARD +
                  (self._io > self._IO_THRESH) * self._IO_REWARD) or

                   self._POSITIVE_REWARD) * self._MEM_FINE

        gap = self._prev_gap + delta
        gap = max(min(gap, self.wss / 2), self._MIN_GAP)
        gap = self._align(gap)
        return gap

    def _app_hysteresis(self, cur, goal):
        tgt = cur
        if cur > goal:
                tgt = cur - ((cur - goal) / self._DOWNHYSTERESIS)
        elif cur < goal:
                tgt = cur + ((goal - cur) / self._UPHYSTERESIS)
        return tgt

    def _get_wss(self):
        if self._ve.mem_stats.wss > 0:
            return self._ve.mem_stats.wss

        return {consts.PVS_GUEST_TYPE_LINUX: self._get_wss_linux,
                consts.PVS_GUEST_TYPE_WINDOWS: self._get_wss_win}[self._ve_session._os_type]()

    def _get_wss_linux(self):
        # available  on  kernels  3.14
        if not self.linux_memstat or 'MemAvailable' not in self.linux_memstat:
            return self._ve.mem_stats.rss

        return self._ve.mem_stats.actual - self.linux_memstat['MemAvailable']

    def _get_wss_win(self):
        unused = 0
        if self._ve.mem_stats.unused > 0:
            unused = self._ve.mem_stats.unused
        return self._ve.mem_stats.actual - unused

    def _update_quota(self):
        '''
        Calculate the best fit size of WS.
        The simplest calculation of WS guest size based on unused memory.
        In case that we have own guest balloon driver we have more precisely
        WS value
        '''
        self.wss = self._get_wss()
        if self._prev_size is not None and \
           self._ve.mem_stats.actual - self._prev_size > self._DELTA_THRESHOLD:
            # looks like balloon can't grow correctly,
            # so let's reduce it a little
            size = self._ve.mem_stats.actual + \
                   max(self._prev_gap, self._MEM_FINE)
        else:
            # Align new size at page size.
            self.wss = self._align(self.wss)
            gap = self._choose_gap()
            size = self.wss + gap
            if self._prev_size and \
               abs(size - self._prev_size) < self._DELTA_THRESHOLD:
                size = self._prev_size

        # This approach have sense a special in case with WS
        # based on unused memory which really far from real
        size = self._app_hysteresis(self._ve.mem_stats.actual, size)
        # Align new size at page size.
        size = self._align(size)

        self._prev_gap = size - self.wss
        self.quota = size


class WSSPolicy(Policy):
    '''
    In this policy we want to inflate the balloon at all times.
    The new quota size based on WS size.
    '''

    REQUIRES_PERIODIC_UPDATES = True

    def balance(self, active_ves, mem_avail, stats_updated):
        sum_quota = 0
        for ve in active_ves:

            if ve.VE_TYPE != ve_types.VM:
                self.logger.error('This policy should be apply only for VM')

            vepriv = ve.policy_priv
            if vepriv is None:
                vepriv = _VEPrivate(ve)
                ve.policy_priv = vepriv
            if stats_updated:
                vepriv.update()
            vepriv.quota = min(max(vepriv.quota, ve.config.guarantee),
                               ve.config.effective_limit)
            sum_quota += vepriv.quota

        if sum_quota > mem_avail:
            self.logger.error('Sum VE quotas out of mem_avail limit')
        return {ve: ve.policy_priv.quota for ve in active_ves}
