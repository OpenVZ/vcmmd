# Copyright (c) 2016 Parallels IP Holdings GmbH
#
# This file is part of OpenVZ. OpenVZ is free software; you can redistribute
# it and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the License,
# or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#
# Our contact details: Parallels IP Holdings GmbH, Vordergasse 59, 8200
# Schaffhausen, Switzerland.

from vcmmd.host import Host
import logging


class Policy(object):
    '''Load manager policy interface.
    '''

    def __init__(self):
        self.logger = logging.getLogger('vcmmd.ldmgr.policy')
        self.ve_list = []  # List of all managed activated VEs
        self.ve_list_all = []  # List of all managed VEs
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

    def ve_registered(self, ve):
        '''Called right after a VE gets activated.
        '''
        self.ve_list_all.append(ve)

    def ve_unregistered(self, ve):
        '''Called right after a VE gets deactivated.
        '''
        self.ve_list_all.remove(ve)

    def ve_updated(self, ve):
        '''Called right after a VE's stats get updated.
        '''
        pass

    def ve_config_updated(self, ve):
        '''Called right after a VE's configuration update.
        '''
        pass

    def balance(self):
        '''Balance ve resources.

        This function is called by the load manager on VE configuration changes
        and periodically when VE statistics get updated.

        This function must be overridden in sub-class.
        '''
        pass

class BalloonPolicy(Policy):
    '''Manages balloons in VEs.
    '''
    def balance(self):
        '''Set VE memory quotas

        Expects that self is an appropriate BalloonPolicy with overwritten
        calculate_balloon_size.
        '''
        self.host.ve_mem_reserved = sum(ve.mem_min for ve in self.ve_list_all if not ve.active)
        self.host.active_ve_mem = self.host.ve_mem - self.host.ve_mem_reserved

        ve_quotas = self.calculate_balloon_size()

        sum_protection = sum(ve_quotas[ve][1] for ve in ve_quotas)
        if sum_protection > self.host.active_ve_mem:
            self.logger.error('Sum protection greater than mem available (%d > %d)',
                              sum_protection, self.host.active_ve_mem)

        # Apply the quotas.
        for ve, (target, protection) in ve_quotas.iteritems():
            if sum_protection > self.host.active_ve_mem:
                protection = ve.mem_min
            ve.set_mem(target=target, protection=protection)

        # We need to set memory.low for machine.slice to infinity, otherwise
        # memory.low in sub-cgroups won't have any effect. We can't do it on
        # start, because machine.slice might not exist at that time (it is
        # created on demand, when the first VM starts).
        #
        # This is safe, because there is nothing running inside machine.slice
        # but VMs, each of which should have its memory.low configured
        # properly.
        self.host._set_slice_mem('machine', -1, verbose=False)

    def calculate_balloon_size(self):
        '''Calculate VE memory quotas

        Returns a mapping VE -> (target, protection), where 'target'
        is the memory consumption that should be set for a VE and 'protection'
        is the amount memory that should be protected from host pressure.

        This function must be overridden in sub-class.
        '''
        pass

class PolicySet(object):
    def __init__(self, balloon):
        self.policies = {
            BalloonPolicy : balloon
        }
        self.unique_policies = set(self.policies.values())
        self.DEFAULT_BALANCE_INTERVAL = min(
                [policy.DEFAULT_BALANCE_INTERVAL for policy in self.unique_policies]
        )

    def get_name(self):
        return ", ".join(
            map(
                lambda p: p.__name__ + ": " + self.policies[p].get_name(),
                self.policies
            )
        )

    def __getattr__(self, name):
        if name not in ["ve_activated", "ve_deactivated", "ve_registered",
                "ve_unregistered", "ve_updated", "ve_config_updated"]:
            raise AttributeError
        return (lambda ve: map(lambda policy : getattr(policy,name)(ve), self.unique_policies))

    def balance(self):
        map(lambda policy: policy.balance(self.policies[policy]), self.policies)
