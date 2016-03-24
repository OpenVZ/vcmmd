from __future__ import absolute_import

import time


class Stats(object):

    ABSOLUTE_STATS = []
    CUMULATIVE_STATS = []

    def __init__(self):
        self._raw_stats = {}
        self._last_update = 0

        for k in self.ABSOLUTE_STATS + self.CUMULATIVE_STATS:
            setattr(self, k, -1)

    def __str__(self):
        return ' '.join('%s:%d' % (k, getattr(self, k))
                        for k in self.ABSOLUTE_STATS + self.CUMULATIVE_STATS)

    def _update(self, **stats):
        prev_stats = self._raw_stats
        self._raw_stats = stats

        for k in self.ABSOLUTE_STATS:
            v = stats.get(k, -1)
            if v < 0:  # stat unavailable => return -1
                v = -1
            setattr(self, k, v)

        now = time.time()
        delta_t = now - self._last_update
        self._last_update = now

        for k in self.CUMULATIVE_STATS:
            cur, prev = stats.get(k, -1), prev_stats.get(k, -1)
            if cur < 0 or prev < 0:  # stat unavailable => return -1
                delta = -1
            else:
                delta = int((cur - prev) / delta_t)
            setattr(self, k, delta)


class MemStats(Stats):

    ABSOLUTE_STATS = [
        'rss',          # resident set size
        'actual',       # actual amount of memory committed to the guest
                        # (RAM size - balloon size for VM, memory limit for CT)
        'memtotal',     # total amount of memory as seen by guest OS
        'memfree',      # amount of memory left completely unused by guest OS
        'memavail',     # an estimate of how much memory is available for
                        # starting new applications, without swapping
        'committed',    # amount of memory presently allocated by applications
                        # running inside the guest
        'wss',          # working set size reported by guest OS
    ]

    CUMULATIVE_STATS = [
        'swapin',       # amount of memory read in from swap space
        'swapout',      # amount of memory written out to swap space
        'minflt',       # minor page fault count
        'majflt',       # major page fault count
    ]


class IOStats(Stats):

    CUMULATIVE_STATS = [
        'rd_req',       # number of read requests
        'rd_bytes',     # number of read bytes
        'wr_req',       # number of write requests
        'wr_bytes',     # number of written bytes
    ]
