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

    def ve_updated(self, ve):
        '''Called right after a VE's stats get updated.
        '''
        pass

    def balance(self, mem_avail):
        '''Calculate VE memory quotas.

        This function is called by the load manager on VE configuration changes
        and periodically when VE statistics get updated. It is passed the
        amount of memory available for all managed VEs. It should return a
        mapping VE -> quota, where quota is the memory consumption target that
        should be set for a VE.

        This function must be overridden in sub-class.
        '''
        pass

    def dump_ve(self, ve):
        '''Return extra info about an active VE.

        A sub-class may override this function in order to provide the
        `vcmmdctl dump` command with extra information about active VEs.
        '''
        pass
