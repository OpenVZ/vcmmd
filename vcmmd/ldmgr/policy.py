class Policy(object):
    '''Load manager policy interface.
    '''

    def balance(self, active_ves, mem_avail, stats_updated):
        '''Calculate VE memory quotas.

        This function is called by the load manager on VE configuration changes
        and periodically when VE statistics get updated. In the latter case
        'stats_updated' is set to True. It is passed a list of all active VEs
        and the amount of memory available for them. It should return a mapping
        VE -> quota, where quota is the memory consumption target that should
        be set for a VE.

        'active_ves' is the list of active VEs to balance memory among.
        'mem_avail' is the amount of memory available for active VEs.
        'stats_updated' is set to True if VE statistics has been updated since
        the last time this function was called.

        This function must be overridden in sub-class.
        '''
        pass

    def dump_ve(self, ve):
        '''Return extra info about an active VE.

        A sub-class may override this function in order to provide the
        `vcmmdctl dump` command with extra information about active VEs.
        '''
        pass
