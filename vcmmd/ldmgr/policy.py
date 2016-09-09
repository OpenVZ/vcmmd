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
from vcmmd.ldmgr.base import Request
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
        self.controllers = set()

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

    def ve_config_updated(self, ve):
        '''Called right after a VE's configuration update.
        '''
        pass

    def sched_req(self):
        ret = []
        for ctrl in self.controllers:
            ret.append(ctrl())
        return ret


class BalloonPolicy(Policy):
    '''Manages balloons in VEs.
    '''
    def __init__(self):
        super(BalloonPolicy, self).__init__()
        bc = VCMMDConfig().get_bool("LoadManager.Controllers.Balloon", True)
        self.logger.info("Controllers.Balloon = %r" % bc)
        if not bc:
            return
        self.controllers.add(self.balloon_controller)
        self.balloon_timeout = 5

    def update_balloon_stats(self):
        pass

    def balloon_controller(self):
        '''Set VE memory quotas

        Expects that self is an appropriate BalloonPolicy with overwritten
        calculate_balloon_size.
        '''
        self.update_balloon_stats()

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
        # TODO need only once
        self.host._set_slice_mem('machine', -1, verbose=False)

        return Request(self.balloon_controller, timeout=self.balloon_timeout, blocker=True)

    def calculate_balloon_size(self):
        '''Calculate VE memory quotas

        Returns a mapping VE -> (target, protection), where 'target'
        is the memory consumption that should be set for a VE and 'protection'
        is the amount memory that should be protected from host pressure.

        This function must be overridden in sub-class.
        '''
        pass


class NumaPolicy(Policy):
    '''Manages NUMA nodes' load by VEs.
    '''
    def __init__(self):
        super(NumaPolicy, self).__init__()
        nc = VCMMDConfig().get_bool("LoadManager.Controllers.NUMA", True)
        self.logger.info("Controllers.NUMA = %r" % nc)
        if not nc:
            return
        self.controllers.add(self.numa_controller)
        self.numa_timeout = 60 * 5

    def update_numa_stats(self):
        pass

    def numa_controller(self):
        '''Reapply_policy VEs between NUMA nodes.

        Expects that self is an appropriate NumaPolicy with overwritten
        get_numa_migrations.
        '''
        self.update_numa_stats()

        changes = self.get_numa_migrations()
        for ve, nodes in changes.iteritems():
            if nodes:
                ve.set_node_list(nodes)

        return Request(self.numa_controller, timeout=self.numa_timeout, blocker=True)

    def get_numa_migrations(self):
        '''Suggest VE numa node migrations.

        Returns a mapping VE -> new node list, or None to preserve old list.

        This function must be overridden in sub-class.
        '''
        pass


class KSMPolicy(Policy):
    '''Manages ksm parametrs on host
    '''
    def __init__(self):
        super(KSMPolicy, self).__init__()
        kc = VCMMDConfig().get_bool("LoadManager.Controllers.KSM", True)
        self.logger.info("Controllers.KSM = %r" % kc)
        if not kc:
            return
        self.controllers.add(self.ksm_controller)
        self.ksm_timeout = 60

    def update_ksm_stats(self):
        pass

    def ksm_controller(self):
        self.update_ksm_stats()
        params = self.get_ksm_params()
        self.host.ksmtune(params)

        return Request(self.ksm_controller, timeout=self.ksm_timeout, blocker=True)

    def get_ksm_params(self):
        return {}
