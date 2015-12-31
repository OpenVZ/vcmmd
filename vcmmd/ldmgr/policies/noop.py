from __future__ import absolute_import

from vcmmd.ldmgr import PolicyWithGuarantees


class NoOpPolicy(PolicyWithGuarantees):
    '''No Operation load manager policy.

    Set best-effort memory protection and throttle limit to configured
    guarantee and limit, respectively, and let the host kernel do the rest.
    This will only work satisfactory if the host kernel can reclaim memory from
    VEs effectively and is smart enough to detect a VE's working set by itself.
    '''

    def balance(self, all_ves, timeout):
        result = {}
        for ve in all_ves:
            result[ve] = (ve.config.guarantee, ve.config.limit)
        return result
