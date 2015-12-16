class Policy(object):
    '''Load manager policy interface.
    '''

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
