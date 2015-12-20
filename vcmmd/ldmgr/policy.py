class Policy(object):
    '''Load manager policy interface.
    '''

    def may_register(self, ve, all_ves):
        '''Check if a VE may be started.

        A sub-class may overwrite this function to forbid starting a VE if it
        finds that this will result in overloading the host.
        '''
        return True

    def may_update(self, ve, new_config, all_ves):
        '''Check if a VE configuration may be updated.

        A sub-class may overwrite this function to forbid updating a VE's
        configuration if it finds that this will result in overloading the
        host.
        '''
        return True

    def balance(self, all_ves):
        '''Calculate optimal memory consumption range for VEs.

        This function is called whenever the load manager detects load
        configuration change. It is passed a list of all registered VEs and
        should return a dictionary VE -> (low, high), where low and high are
        lower and higher boundaries for optimal memory consumption range of a
        VE.

        This function must be overwritten in sub-class.
        '''
        pass
