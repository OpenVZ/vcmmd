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

    def balance(self, active_ves, mem_avail, timeout):
        '''Calculate VE memory quotas.

        This function is called whenever the load manager detects load
        configuration change. It is passed a list of all registered VEs and
        should return a dictionary VE -> quota, where quota is a VE memory
        consumption target calculated by the policy.

        'active_ves' is the list of active VEs to balance memory among.
        'mem_avail' is the amount of memory available for active VEs.
        'timeout' is the time, in seconds, that has passed since the last call
        of this function or None if this function is called for the first time.

        This function must be overridden in sub-class.
        '''
        pass

    def timeout(self):
        '''Return maximal timeout, in seconds, before 'balance' should be
        called again or None if the policy does not need it to be invoked
        unless the load configuration changes.

        This function is called after 'balance'. It is OK to return different
        timeouts from this function.

        Note, the load manager may call 'balance' prematurely, before the
        timeout has passed, in case the load configuration changes (e.g. if a
        new VE is registered). The 'timeout' argument of the 'balance' method
        is there to help detect this.

        This function may be overridden in sub-class.
        '''
        return None
