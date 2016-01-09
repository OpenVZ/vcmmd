class Policy(object):
    '''Load manager policy interface.
    '''

    def may_register(self, ve_to_register, all_ves, mem_total):
        '''Check if a VE may be started.

        'all_ves' is the list of all registered VEs.
        'mem_total' is the total amount of memory available for VEs.

        A sub-class may override this function to forbid starting a VE if it
        finds that this will result in overloading the host.

        By default this function checks that the sum of VEs' guarantees fits in
        available memory.
        '''
        sum_guarantee = sum(ve.config.guarantee for ve in all_ves)
        sum_guarantee += ve_to_register.config.guarantee
        return sum_guarantee <= mem_total

    def may_update(self, ve_to_update, new_config, all_ves, mem_total):
        '''Check if a VE configuration may be updated.

        'all_ves' is the list of all registered VEs.
        'mem_total' is the total amount of memory available for VEs.

        A sub-class may override this function to forbid updating a VE's
        configuration if it finds that this will result in overloading the
        host.

        By default this function checks that the sum of VEs' guarantees fits in
        available memory.
        '''
        sum_guarantee = sum(ve.config.guarantee for ve in all_ves)
        sum_guarantee += new_config.guarantee - ve_to_update.config.guarantee
        return sum_guarantee <= mem_total

    def balance(self, active_ves, mem_avail, update_stats):
        '''Calculate VE memory quotas.

        This function is called by the load manager on VE configuration changes
        and periodically when VE statistics get updated. In the latter case
        'update_stats' is set to True. It is passed a list of all active VEs
        and the amount of memory available for them. It should return a mapping
        VE -> quota, where quota is the memory consumption target that should
        be set for a VE.

        'active_ves' is the list of active VEs to balance memory among.
        'mem_avail' is the amount of memory available for active VEs.
        'update_stats' is set to True if VE statistics has been updated since
        the last time this function was called.

        This function must be overridden in sub-class.
        '''
        pass
