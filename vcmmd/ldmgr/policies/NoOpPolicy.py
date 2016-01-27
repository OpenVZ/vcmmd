from __future__ import absolute_import

from vcmmd.ldmgr import Policy


class NoOpPolicy(Policy):
    '''No Operation load manager policy.

    Set memory quotas to configured limits and let the host kernel do the rest.
    This will only work satisfactory if the host kernel can reclaim memory from
    VEs effectively and is smart enough to detect a VE's working set by itself.
    '''

    def balance(self, active_ves, mem_avail, stats_updated):
        return {ve: ve.config.limit for ve in active_ves}
