from __future__ import absolute_import

from vcmmd.ldmgr.policies import WSSPolicy
from vcmmd.ve.ct import CT
from vcmmd.ve.vm import VM
import os
import logging


class AbstractVE(WSSPolicy.AbstractVE):

    @WSSPolicy.align
    def _choose_gap(self, wss):
        '''
        Put a fine or a prize for the previous change
        '''
        delta = (((self._swapin > self._SWAPIN_THRESH and
                   self._swapout > self._SWAPIN_THRESH) * self._SWAPEXCH_REWARD +

                  (self._pgflt > self._PGFLT_THRESH) * self._PGFLT_REWARD +
                  (self._io_avg > self._IO_THRESH) * self._IO_REWARD +
                  # if actual - prev quota > threshold
                  # looks like balloon can't grow correctly,
                  # so let's reduce it a little
                  (self._actual - self.quota > self._DELTA_THRESHOLD) * self._INSIZE_REWARD) or

                   self._POSITIVE_REWARD) * self._MEM_FINE

        if self._actual - self.quota > self._DELTA_THRESHOLD:
            self.logger.error("balloon in %r can't grow correctly"
                              "(actual: %d, quota: %d)" % (self._ve, self._actual, self.quota))
        gap = self._actual - wss + delta
        gap = max(gap, self._MIN_GAP)
        return gap


class LinuxGuest(AbstractVE):

    def _get_wss(self):
        # available  on  kernels  3.14
        if not self.linux_memstat or 'MemAvailable' not in self.linux_memstat:
            self.logger.error('Failed to get "MemAvailable" '
                              'from linux guest(%s), using RSS' % self._ve)
            return self._ve.mem_stats.rss

        return self._actual - self.linux_memstat['MemAvailable']

    def _read_meminfo(self):
        pass

    def _update_add_stat(self):
        self.linux_memstat = {}
        out = self._read_meminfo()
        if out is None:
            return
        for line in out.splitlines():
            line = line.split()
            if not line:
                continue
            self.linux_memstat[line[0].strip(':')] = int(line[1]) << 10


class LinuxVM(LinuxGuest):

    def _read_meminfo(self):
        status, out = self._ve_session.getstatusoutput(['cat',
                                                        '/proc/meminfo'])
        if status:
            return
        return out


class LinuxCT(LinuxGuest):

    def _read_meminfo(self):
        meminfo_path = '/proc/bc/%s/meminfo' % self._ve.name
        with open(meminfo_path) as f:
            return f.read()


class WindowsVM(AbstractVE):

    def _get_wss(self):
        unused = 0
        if self._ve.mem_stats.unused > 0:
            unused = self._ve.mem_stats.unused
        else:
            self.logger.error('Failed to get "unused" '
                              'from windows guest(%s), using "actual"' % self._ve)
        return self._actual - unused


class WSSSoftPolicy(WSSPolicy.WSSPolicy):
    '''
    In this policy we want to inflate the balloon at all times.
    The new quota size based on WS size.
    The only difference from WSSPolicy is gap calculation.
    '''

    def balance(self, active_ves, mem_avail, stats_updated):
        sum_quota = 0
        for ve in active_ves:
            vepriv = ve.policy_priv
            if vepriv is None:
                TypeGuest = None
                if isinstance(ve, CT):
                    session = None
                    TypeGuest = LinuxCT
                elif isinstance(ve, VM):
                    session = WSSPolicy.VmGuestSession(ve.name)
                    TypeGuest = {WSSPolicy.GUEST_LINUX: LinuxVM,
                                 WSSPolicy.GUEST_WINDOWS: WindowsVM}[session.os_type]
                assert TypeGuest, 'Unknown guest type'
                vepriv = TypeGuest(ve, session)
                ve.policy_priv = vepriv
            if stats_updated:
                vepriv.update()
            sum_quota += vepriv.quota

        if sum_quota > mem_avail:
            self.logger.error('Sum VE quotas out of mem_avail limit')

        return {ve: ve.policy_priv.quota for ve in active_ves}
