from __future__ import absolute_import

import logging
import psutil

from vcmmd.util.singleton import Singleton
from vcmmd.util.stats import Stats


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

        stats = {'memtotal': mem.total,
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
