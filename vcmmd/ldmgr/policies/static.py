from __future__ import absolute_import

from vcmmd.ldmgr import PolicyWithGuarantees


class StaticPolicy(PolicyWithGuarantees):
    '''Static load manager policy.

    A straightforward policy that assigns each VE a share of memory
    proportional to memory limit, but never less than guarantee.
    '''

    def balance(self, all_ves, timeout):
        avail = self._mem_available()

        # Always leave a small gap between low and high boundaries so as to let
        # the system breathe.
        mem_range = lambda x: (x * 95 / 100, x)

        # If the host is not overcommitted, just give each VE as much as
        # configured limit allows.
        sum_lim = sum(ve.config.limit for ve in all_ves)
        if sum_lim <= avail:
            return {ve: mem_range(ve.config.limit) for ve in all_ves}

        # In an overcommitted case, give each VE its guaranteed amount of
        # memory and distribute the rest proportionally to configured limits.
        sum_guar = sum(ve.config.guarantee for ve in all_ves)
        return {ve: mem_range(ve.config.guarantee + (avail - sum_guar) *
                              (ve.config.limit - ve.config.guarantee) /
                              (sum_lim - sum_guar + 1))
                for ve in all_ves}
