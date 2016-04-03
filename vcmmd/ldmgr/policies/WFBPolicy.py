from __future__ import absolute_import

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
        unused = self._ve.stats.memfree
        # If no value is provided by guest OS, rely on rss.
        if unused < 0:
            unused = self.quota - self._ve.stats.rss
        self._unused = clamp(unused, 0, self.quota)

    def _update_io(self):
        self._io = self._ve.stats.rd_req + self._ve.stats.wr_req
        self._io_avg = ((self._io + self._AVG_WINDOW * self._io_avg) /
                        (self._AVG_WINDOW + 1))

    def _update_pgflt(self):
        self._pgflt = self._ve.stats.majflt
        self._pgflt_avg = ((self._pgflt + self._AVG_WINDOW * self._pgflt_avg) /
                           (self._AVG_WINDOW + 1))

    def _update_quota(self):
        # If a VE is struggling to reclaim its memory so as to fit in its
        # quota, do not push it too hard.
        self.quota = max(self.quota, self._ve.stats.actual)

        # High io/pgflt rate and not much free memory? Looks like the VE is
        # thrashing, so consider increasing its quota.
        mem_low = clamp(int(self._ve.effective_limit * self._MEM_LOW),
                        self._MEM_LOW_MIN, self._MEM_LOW_MAX)
        if (self._unused <= mem_low and
                (self._io > self._IO_THRESH or
                 self._pgflt > self._PGFLT_THRESH)):
            self.quota += int(self._ve.effective_limit * self._QUOTA_INC)

        self.quota = clamp(self.quota, self._ve.config.guarantee,
                           self._ve.effective_limit)

    def _update_weight(self):
        ve = self._ve

        weight = self._BASE_WEIGHT

        # Fine for memory left completely unused.
        weight -= self._unused * self._UNUSED_FINE / (self.quota + 1)

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
        self.logger.debug('%s: quota:%d weight:%.2f '
                          'pgflt:%d/%d io:%d/%d unused:%d',
                          self._ve, self.quota, self._weight,
                          self._pgflt, self._pgflt_avg,
                          self._io, self._io_avg, self._unused)

    @property
    def weight(self):
        # This VE can't consume more memory.
        if self.quota >= self._ve.effective_limit:
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


class WFBPolicy(Policy):
    '''Weighted feedback-based policy.

    The idea is simple. Whenever the policy detects that a VE needs more memory
    by checking io/pgflt counters, we increase its quota a little. In order to
    compensate for the quota increase, we reclaim memory from each VE inversely
    proportionally to their weights so that the greater the weight of a VE the
    less memory is reclaimed from it. Weights are calculated heuristically so
    as to try to reclaim memory from idle VEs more than from those that are
    actively using their allocation.
    '''

    def ve_activated(self, ve):
        Policy.ve_activated(self, ve)
        ve.policy_data = _VEPrivate(ve)
        ve.policy_data.logger = self.logger

    def ve_deactivated(self, ve):
        Policy.ve_deactivated(self, ve)
        ve.policy_data = None

    def ve_updated(self, ve):
        Policy.ve_updated(self, ve)
        ve.policy_data.update()

    def ve_config_updated(self, ve):
        Policy.ve_config_updated(self, ve)
        ve.policy_data.quota = clamp(ve.policy_data.quota,
                                     ve.config.guarantee, ve.effective_limit)

    def __grant_quota(self, value):
        # There is an excess of quota. Grant it too all active VEs
        # proportionally to their weights, respecting configured limits.
        denominator = sum(ve.policy_data.weight for ve in self.ve_list)
        if denominator == 0:
            return

        left = 0
        for ve in self.ve_list:
            p = ve.policy_data
            p.quota += int(value * ve.policy_data.weight / denominator)
            if p.quota > ve.effective_limit:
                left += p.quota - ve.effective_limit
                p.quota = ve.effective_limit

        # Ignore delta < 16 Mb.
        if left > (16 << 20):
            self.__grant_quota(left)

    def __subtract_quota(self, value):
        # There is a shortage of quota. Subtract it from all active VEs
        # inversely proportionally to their weights, respecting configured
        # guarantees.
        denominator = sum(ve.policy_data.inv_weight for ve in self.ve_list)
        if denominator == 0:
            return

        left = 0
        for ve in self.ve_list:
            p = ve.policy_data
            p.quota -= int(value * ve.policy_data.inv_weight / denominator)
            if p.quota < ve.config.guarantee:
                left += ve.config.guarantee - p.quota
                p.quota = ve.config.guarantee

        # Ignore delta < 16 Mb.
        if left > (16 << 20):
            self.__subtract_quota(left)

    def balance(self, mem_avail):
        sum_quota = sum(ve.policy_data.quota for ve in self.ve_list)
        if sum_quota < mem_avail:
            self.__grant_quota(mem_avail - sum_quota)
        elif sum_quota > mem_avail:
            self.__subtract_quota(sum_quota - mem_avail)

        # Due to calculation errors, it might turn out that sum_quota is still
        # greater than mem_avail. We don't want it, because that would reset
        # memory protections, so we scale down quotas proportionally in this
        # case.
        sum_quota = sum(ve.policy_data.quota for ve in self.ve_list)
        if sum_quota > mem_avail:
            for ve in self.ve_list:
                ve.policy_data.quota = (ve.policy_data.quota *
                                        mem_avail / sum_quota)

        return {ve: ve.policy_data.quota for ve in self.ve_list}
