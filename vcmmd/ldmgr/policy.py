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

    def balance(self, all_ves, timeout=None):
        '''Calculate optimal memory consumption range for VEs.

        This function is called whenever the load manager detects load
        configuration change. It is passed a list of all registered VEs and
        should return a dictionary VE -> (low, high), where low and high are
        lower and higher boundaries for optimal memory consumption range of a
        VE.

        'all_ves' is the list of all registered VEs to balance memory among.
        'timeout' is the time, in seconds, that has passed since the last call
        of this function or None if this function is called for the first time.

        This function must be overwritten in sub-class.
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
