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

from __future__ import absolute_import

from vcmmd.ldmgr.policy import NumaPolicy, KSMPolicy as AbsKsmPolicy


class KsmPolicy(AbsKsmPolicy):
    ''' VCMMD in conflict with ksmtuned, so this base KSM policy,
        mostly "copycat" ksmtuned
    '''
    def update_ksm_stats(self):
        self.host.update_stats()
        self.host.log_debug('update stats: %s', self.host.stats)

    def get_ksm_params(self):
        ksm_pages_boost = 300
        ksm_pages_decay = -50
        ksm_npages_min = 64
        ksm_npages_max = 1250
        ksm_threshold = 0.20
        ksm_sleep_ms_baseline = 10
        ksm_hostmem_baseline = 16 << 30

        params = {'merge_across_nodes': int(not isinstance(self, NumaPolicy))}

        need_stats = (self.host.stats.memtotal, self.host.stats.memfree,
                      self.host.stats.memavail, self.host.stats.ksm_pages_to_scan)

        if filter(lambda x: x < 0, need_stats):
            return params

        if self.host.stats.memfree > ksm_threshold * self.host.stats.memtotal:
            params['run'] = 0
        else:
            params['run'] = 1

            params['sleep_millisecs'] = int(ksm_sleep_ms_baseline * \
                                            (float(ksm_hostmem_baseline) / self.host.stats.memtotal))
            if self.host.stats.memavail < self.host.stats.memtotal * ksm_threshold:
                delta = ksm_pages_boost
            else:
                delta = ksm_pages_decay

            new = self.host.stats.ksm_pages_to_scan + delta
            params['pages_to_scan'] = min(max(ksm_npages_min, new), ksm_npages_max)

        return params


class NoOpPolicy(KsmPolicy):
    '''No Operation load manager policy.

    Set memory quotas to configured limits and let the host kernel do the rest.
    This will only work satisfactory if the host kernel can reclaim memory from
    VEs effectively and is smart enough to detect a VE's working set by itself.
    '''
    def ve_activated(self, ve):
        super(NoOpPolicy, self).ve_activated(ve)
        ve.set_mem(ve.config.limit, ve.mem_min)

    def ve_config_updated(self, ve):
        super(NoOpPolicy, self).ve_config_updated(ve)
        ve.set_mem(ve.config.limit, ve.mem_min)
