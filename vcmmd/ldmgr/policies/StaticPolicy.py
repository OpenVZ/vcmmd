from __future__ import absolute_import

from vcmmd.ldmgr import Policy


class StaticPolicy(Policy):
    '''Static load manager policy.

    A straightforward policy that assigns each VE a share of memory
    proportional to memory limit, but never less than guarantee.
    '''

    def balance(self, mem_avail):
        # If the host is not overcommitted, just give each VE as much as
        # configured limit allows.
        sum_lim = sum(ve.effective_limit for ve in self.ve_list)
        if sum_lim <= mem_avail:
            return {ve: ve.effective_limit for ve in self.ve_list}

        # In an overcommitted case, give each VE its guaranteed amount of
        # memory and distribute the rest proportionally to configured limits.
        sum_guar = sum(ve.config.guarantee for ve in self.ve_list)
        return {ve: (ve.config.guarantee + (mem_avail - sum_guar) *
                     (ve.effective_limit - ve.config.guarantee) /
                     (sum_lim - sum_guar + 1))
                for ve in self.ve_list}
