from __future__ import absolute_import

import logging
import psutil

from vcmmd.util.singleton import Singleton
from vcmmd.util.stats import Stats
from vcmmd.util.misc import clamp
from vcmmd.config import VCMMDConfig
from vcmmd.cgroup import MemoryCgroup


class HostStats(Stats):

    ABSOLUTE_STATS = [
        'memtotal',         # total amount of physical memory on host
        'memfree',          # amount of memory left completely unused by host
        'memavail',         # an estimate of how much memory is available for
                            # starting new applications, without swapping
        'ksm_pg_shared',    # how many shared pages are being used for ksm
        'ksm_pg_sharing',   # how many more sites are sharing them
        'ksm_pg_unshared',  # how many pages unique but repeatedly checked for merging
        'ksm_pg_volatile',  # how many pages changing too fast to be placed in a tree

    ]

    CUMULATIVE_STATS = [
        'ksm_full_scans',   # how many times all mergeable areas have been scanned
    ]

    ALL_STATS = ABSOLUTE_STATS + CUMULATIVE_STATS


class Host(object):

    __metaclass__ = Singleton

    def __init__(self):
        self.logger = logging.getLogger('vcmmd.host')

        self.stats = HostStats()
        # Reserve memory for the system
        total_mem = psutil.virtual_memory().total
        self.total_mem = total_mem
        self.host_mem = self._mem_size_from_config('HostMem', total_mem,
                                                   (0.04, 128 << 20, 320 << 20))
        self.sys_mem = self._mem_size_from_config('SysMem', total_mem,
                                                  (0.04, 128 << 20, 320 << 20))
        self._set_slice_mem('system', self.sys_mem)
        self.user_mem = self._mem_size_from_config('UserMem', total_mem,
                                                   (0.02, 32 << 20, 128 << 20))
        self._set_slice_mem('user', self.user_mem)
        # Calculate size of memory available for VEs
        self.ve_mem = self.total_mem - self.host_mem - self.user_mem - self.sys_mem
        self.logger.info('%d bytes available for VEs', self.ve_mem)
        if self.ve_mem < 0:
            self.logger.error('Not enough memory to run VEs!')

    def _mem_size_from_config(self, name, mem_total, default):
        cfg = VCMMDConfig()
        share = cfg.get_num('Host.%s.Share' % name,
                            default=default[0], minimum=0.0, maximum=1.0)
        min_ = cfg.get_num('Host.%s.Min' % name,
                           default=default[1], integer=True, minimum=0)
        max_ = cfg.get_num('Host.%s.Max' % name,
                           default=default[2], integer=True, minimum=0)
        return clamp(int(mem_total * share), min_, max_)

    def _set_slice_mem(self, name, value, verbose=True):
        memcg = MemoryCgroup(name + '.slice')
        if not memcg.exists():
            return
        try:
            memcg.write_mem_low(value)
            memcg.write_oom_guarantee(value)
        except IOError as err:
            self.logger.error('Failed to set reservation for %s slice: %s',
                              name, err)
        else:
            if verbose:
                self.logger.info('Reserved %s bytes for %s slice', value, name)

    def update_stats(self):
        '''Update host stats.
        '''
        sysfs_keys = ['full_scans', 'pages_sharing', 'pages_unshared',
                      'pages_shared', 'pages_volatile']

        ksm_stats = {}
        for datum in sysfs_keys:
            name = '/sys/kernel/mm/ksm/%s' % datum
            try:
                with open(name, 'r') as ksm_stats_file:
                    ksm_stats[datum] = int(ksm_stats_file.read())
            except IOError, (errno, msg):
                ksm_stats[datum] = -1
                self.logger.error("Failed to update stat: open %s failed: %s" % (name, msg))
        mem = psutil.virtual_memory()

        stats = {'memtotal': self.total_mem,
                 'memfree': mem.free,
                 'memavail': mem.available,
                 'ksm_pg_shared': ksm_stats.get('pages_shared', -1),
                 'ksm_pg_sharing': ksm_stats.get('pages_sharing', -1),
                 'ksm_pg_unshared': ksm_stats.get('pages_unshared', -1),
                 'ksm_pg_volatile': ksm_stats.get('pages_volatile', -1),
                 'ksm_full_scans': ksm_stats.get('full_scans', -1),
                 }
        self.stats._update(**stats)
        self.logger.debug('update_stats: %s', self.stats)
