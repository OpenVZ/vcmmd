from __future__ import absolute_import

from vcmmd.ldmgr import Policy
from vcmmd.ve import types as ve_types


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
    _DELTA_THRESHOLD = 32 * __UNITS

    _MEM_FINE = 32 * __UNITS
    _IO_REWARD = 4.
    _PGFLT_REWARD = 8.
    _POSITIVE_REWARD = -8.

    _DOWNHYSTERESIS = 8
    _UPHYSTERESIS = 1

    def __init__(self, ve):
        self._ve = ve

        self.quota = ve.config.effective_limit

        self._io = 0
        self._io_avg = 0

        self._pgflt = 0
        self._pgflt_avg = 0

        # _prev_gap is some empirical initial value.
        # It doesn't influence the final result
        self._prev_gap = self._MIN_GAP
        self._prev_size = None

    def _update_io(self):
        self._io = self._ve.io_stats.rd_req + self._ve.io_stats.wr_req
        self._io_avg = ((self._io + self._AVG_WINDOW * self._io_avg) /
                        (self._AVG_WINDOW + 1))

    def _update_pgflt(self):
        self._pgflt = self._ve.mem_stats.majflt
        self._pgflt_avg = ((self._pgflt + self._AVG_WINDOW * self._pgflt_avg) /
                           (self._AVG_WINDOW + 1))

    def update(self):
        self._update_io()
        self._update_pgflt()
        self._update_quota()

    def _align(self, val):
        val = int(val)
        val &= ~(self._ALIGN - 1)
        return val

    def _choose_gap(self):
        '''
        Put a fine or a prize for the previous change
        '''
        delta = ((self._pgflt > self._PGFLT_THRESH) * self._PGFLT_REWARD +
                 (self._io > self._IO_THRESH) * self._IO_REWARD) or \
                  self._MEM_FINE * self._POSITIVE_REWARD

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
        unused = self._ve.mem_stats.unused if self._ve.mem_stats.unused > 0 else 0
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
