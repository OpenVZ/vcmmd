from __future__ import absolute_import

import psutil


class Policy(object):
    '''Load manager policy interface.
    '''

    def may_register(self, ve_to_register, all_ves):
        '''Check if a VE may be started.

        A sub-class may override this function to forbid starting a VE if it
        finds that this will result in overloading the host.
        '''
        return True

    def may_update(self, ve_to_update, new_config, all_ves):
        '''Check if a VE configuration may be updated.

        A sub-class may override this function to forbid updating a VE's
        configuration if it finds that this will result in overloading the
        host.
        '''
        return True

    def balance(self, all_ves, timeout=None):
        '''Calculate VE memory quotas.

        This function is called whenever the load manager detects load
        configuration change. It is passed a list of all registered VEs and
        should return a dictionary VE -> quota, where quota is a VE memory
        consumption target calculated by the policy.

        'all_ves' is the list of all registered VEs to balance memory among.
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


class PolicyWithGuarantees(Policy):
    '''This is an abstract sub-class of Policy that implements register and
    update checks.
    '''

    _HOST_MEM_PCT = 5           # 5 %
    _HOST_MEM_MIN = 128 << 20   # 128 MB
    _HOST_MEM_MAX = 1 << 30     # 1 GB

    def _mem_available(self):
        '''Return size of memory, in bytes, available for VEs.
        '''
        mem = psutil.virtual_memory()

        # We should leave some memory for the host. Give it some percentage of
        # total memory, but never give too little or too much.
        host_rsrv = mem.total * self._HOST_MEM_PCT / 100
        host_rsrv = max(host_rsrv, self._HOST_MEM_MIN)
        host_rsrv = min(host_rsrv, self._HOST_MEM_MAX)

        return mem.total - host_rsrv

    def may_register(self, ve_to_register, all_ves):
        sum_guarantee = sum(ve.config.guarantee for ve in all_ves)
        sum_guarantee += ve_to_register.config.guarantee
        return sum_guarantee <= self._mem_available()

    def may_update(self, ve_to_update, new_config, all_ves):
        sum_guarantee = sum(ve.config.guarantee for ve in all_ves)
        sum_guarantee += new_config.guarantee - ve_to_update.config.guarantee
        return sum_guarantee <= self._mem_available()
