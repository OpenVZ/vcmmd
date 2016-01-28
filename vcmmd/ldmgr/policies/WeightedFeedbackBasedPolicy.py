from __future__ import absolute_import

import logging

from vcmmd.ldmgr import Policy
from vcmmd.util.misc import clamp


class _VEPrivate(object):

    _MIN_WEIGHT = 1.
    _MAX_WEIGHT = 20.
    _BASE_WEIGHT = 10.

    _AVG_WINDOW = 10

    _QUOTA_INC = 0.1

    _MEM_LOW = 0.1  # 10%
    _MEM_LOW_MIN = 192 << 20  # 192 MB
    _MEM_LOW_MAX = 768 << 20  # 768 MB

    _IO_THRESH = 20
    _PGFLT_THRESH = 20

    _IO_REWARD = 4.
    _PGFLT_REWARD = 8.
    _UNUSED_FINE = 8.
    _IDLE_FINE = [4., 2., 2., 1., 1.]

    def __init__(self, ve):
        self._ve = ve

        self.quota = ve.config.guarantee
        self._weight = self._BASE_WEIGHT

        self._unused = 0

        self._io = 0
        self._io_avg = 0

        self._pgflt = 0
        self._pgflt_avg = 0

    def _update_unused(self):
        unused = self._ve.mem_stats.unused
        # If no value is provided by guest OS, rely on rss.
        if unused < 0:
            unused = self.quota - self._ve.mem_stats.rss
        self._unused = clamp(unused, 0, self.quota)

    def _update_io(self):
        self._io = self._ve.io_stats.rd_req + self._ve.io_stats.wr_req
        self._io_avg = ((self._io + self._AVG_WINDOW * self._io_avg) /
                        (self._AVG_WINDOW + 1))

    def _update_pgflt(self):
        self._pgflt = self._ve.mem_stats.majflt
        self._pgflt_avg = ((self._pgflt + self._AVG_WINDOW * self._pgflt_avg) /
                           (self._AVG_WINDOW + 1))

    def _update_quota(self):
        # If a VE is struggling to reclaim its memory so as to fit in its
        # quota, do not push it too hard.
        self.quota = max(self.quota, self._ve.mem_stats.actual)

        # High io/pgflt rate and not much free memory? Looks like the VE is
        # thrashing, so consider increasing its quota.
        mem_low = clamp(int(self._ve.config.effective_limit * self._MEM_LOW),
                        self._MEM_LOW_MIN, self._MEM_LOW_MAX)
        if (self._unused <= mem_low and
                (self._io > self._IO_THRESH or
                 self._pgflt > self._PGFLT_THRESH)):
            self.quota += int(self._ve.config.effective_limit *
                              self._QUOTA_INC)

    def _update_weight(self):
        ve = self._ve

        weight = self._BASE_WEIGHT

        # Fine for memory left completely unused.
        weight -= self._unused * self._UNUSED_FINE / self.quota

        # Fine for allocated, but not actively used memory.
        for i in range(len(self._IDLE_FINE)):
            weight -= self._IDLE_FINE[i] * ve.idle_ratio(i)

        # Reward for page faults and io. Take into account both instant and
        # average values.
        weight += ((self._io > self._IO_THRESH) * self._IO_REWARD +
                   (self._io_avg > self._IO_THRESH) * self._IO_REWARD / 2 +
                   (self._pgflt > self._PGFLT_THRESH) * self._PGFLT_REWARD +
                   (self._pgflt_avg > self._PGFLT_THRESH) *
                   self._PGFLT_REWARD / 2)

        self._weight = clamp(weight, self._MIN_WEIGHT, self._MAX_WEIGHT)

    def update(self):
        self._update_unused()
        self._update_io()
        self._update_pgflt()
        self._update_quota()
        self._update_weight()

    @property
    def weight(self):
        # This VE can't consume more memory.
        if self.quota >= self._ve.config.effective_limit:
            return 0

        # Normalize weight by quota so as not to grant/subtract too much from
        # tiny VEs at once.
        return self._weight / (self.quota + 1)

    @property
    def inv_weight(self):
        # Nothing to reclaim from this VE.
        if self.quota <= self._ve.config.guarantee:
            return 0

        # Normalize weight by quota so as not to grant/subtract too much from
        # tiny VEs at once.
        return self.quota / self._weight

    _DUMP_FMT = ('%s: quota=%d weight=%.2f pgflt=%d/%d io=%d/%d unused=%d '
                 'idle=' + ':%0.2f' * 5)

    def dump(self):
        return (self._DUMP_FMT %
                ((self._ve, self.quota, self._weight,
                  self._pgflt, self._pgflt_avg,
                  self._io, self._io_avg, self._unused) +
                 tuple(self._ve.idle_ratio(i) for i in range(5))))


class WeightedFeedbackBasedPolicy(Policy):
    '''Weighted feedback-based policy.

    The idea is simple. Whenever the policy detects that a VE needs more memory
    by checking io/pgflt counters, we increase its quota a little. In order to
    compensate for the quota increase, we reclaim memory from each VE inversely
    proportionally to their weights so that the greater the weight of a VE the
    less memory is reclaimed from it. Weights are calculated heuristically so
    as to try to reclaim memory from idle VEs more than from those that are
    actively using their allocation.
    '''

    def __grant_quota(self, active_ves, value):
        # There is an excess of quota. Grant it too all active VEs
        # proportionally to their weights, respecting configured limits.
        denominator = sum(ve.policy_priv.weight for ve in active_ves)
        if denominator == 0:
            return

        left = 0
        for ve in active_ves:
            vepriv = ve.policy_priv
            vepriv.quota += int(value * ve.policy_priv.weight / denominator)
            if vepriv.quota > ve.config.effective_limit:
                left += vepriv.quota - ve.config.effective_limit
                vepriv.quota = ve.config.effective_limit

        # Ignore delta < 16 Mb.
        if left > (16 << 20):
            self.__grant_quota(active_ves, left)

    def __subtract_quota(self, active_ves, value):
        # There is a shortage of quota. Subtract it from all active VEs
        # inversely proportionally to their weights, respecting configured
        # guarantees.
        denominator = sum(ve.policy_priv.inv_weight for ve in active_ves)
        if denominator == 0:
            return

        left = 0
        for ve in active_ves:
            vepriv = ve.policy_priv
            vepriv.quota -= int(value * ve.policy_priv.inv_weight /
                                denominator)
            if vepriv.quota < ve.config.guarantee:
                left += ve.config.guarantee - vepriv.quota
                vepriv.quota = ve.config.guarantee

        # Ignore delta < 16 Mb.
        if left > (16 << 20):
            self.__subtract_quota(active_ves, left)

    def balance(self, active_ves, mem_avail, stats_updated):
        sum_quota = 0
        for ve in active_ves:
            vepriv = ve.policy_priv
            if vepriv is None:
                vepriv = _VEPrivate(ve)
                ve.policy_priv = vepriv
            if stats_updated:
                vepriv.update()
            vepriv.quota = clamp(vepriv.quota, ve.config.guarantee,
                                 ve.config.effective_limit)
            sum_quota += vepriv.quota

        if sum_quota < mem_avail:
            self.__grant_quota(active_ves, mem_avail - sum_quota)
        elif sum_quota > mem_avail:
            self.__subtract_quota(active_ves, sum_quota - mem_avail)

        # Due to calculation errors, it might turn out that sum_quota is still
        # greater than mem_avail. We don't want it, because that would reset
        # memory protections, so we scale down quotas proportionally in this
        # case.
        sum_quota = sum(ve.policy_priv.quota for ve in active_ves)
        if sum_quota > mem_avail:
            for ve in active_ves:
                ve.policy_priv.quota = (ve.policy_priv.quota *
                                        mem_avail / sum_quota)

        # Dump stats of all active VEs for debugging.
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug('=' * 4 + ' VE stats ' + '=' * 4)
            for ve in active_ves:
                self.logger.debug(ve.policy_priv.dump())
            self.logger.debug('')

        return {ve: ve.policy_priv.quota for ve in active_ves}
