class Policy(object):
    '''Load manager policy interface.
    '''

    def __init__(self):
        self.ve_list = []  # List of all managed VEs

    def ve_activated(self, ve):
        '''Called right after a VE gets activated.
        '''
        self.ve_list.append(ve)

    def ve_deactivated(self, ve):
        '''Called right after a VE gets deactivated.
        '''
        self.ve_list.remove(ve)

    def balance(self, mem_avail, stats_updated):
        '''Calculate VE memory quotas.

        This function is called by the load manager on VE configuration changes
        and periodically when VE statistics get updated. In the latter case
        'stats_updated' is set to True. The function should return a mapping
        VE -> quota, where quota is the memory consumption target that should
        be set for a VE.

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
