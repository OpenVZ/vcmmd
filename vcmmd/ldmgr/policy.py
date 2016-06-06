from vcmmd.host import Host
import logging


class Policy(object):
    '''Load manager policy interface.
    '''

    def __init__(self):
        self.logger = logging.getLogger('vcmmd.ldmgr.policy')
        self.ve_list = []  # List of all managed VEs
        self.ve_data = {}  # Dictionary of all managed VEs to their policy data
        self.host = Host() # Singleton object with host related data

    def get_name(self):
        return self.__class__.__name__

    def get_policy_data(self, ve):
        return self.ve_data[ve]

    def set_policy_data(self, ve, data):
        self.ve_data[ve] = data

    def ve_activated(self, ve):
        '''Called right after a VE gets activated.
        '''
        self.ve_list.append(ve)

    def ve_deactivated(self, ve):
        '''Called right after a VE gets deactivated.
        '''
        self.ve_list.remove(ve)
        self.ve_data.pop(ve, None)

    def ve_updated(self, ve):
        '''Called right after a VE's stats get updated.
        '''
        pass

    def ve_config_updated(self, ve):
        '''Called right after a VE's configuration update.
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
