from vcmmd.ldmgr import Policy


class ElementaryPolicy(Policy):
    '''Elementary load manager policy.

    Assume optimal memory consumption range for a VE is (guarantee, limit).
    That's it, simple as that.
    '''

    def balance(self, all_ves, timeout):
        result = {}
        for ve in all_ves:
            result[ve] = (ve.config.guarantee, ve.config.limit)
        return result
