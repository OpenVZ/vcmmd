from __future__ import absolute_import

from vcmmd.util.stats import Stats

class VEStats(Stats):

    ABSOLUTE_STATS = [
        'rss',          # resident set size
        'actual',       # actual amount of memory committed to the guest
                        # (RAM size - balloon size for VM, memory limit for CT)
        'host_mem',     # size of host memory used
                        # (note it can be less than rss due to shared pages)
        'host_swap',    # size of host swap used
        'memfree',      # amount of memory left completely unused by guest OS
        'memavail',     # an estimate of how much memory is available for
                        # starting new applications, without swapping
        'committed',    # amount of memory presently allocated by applications
                        # running inside the guest
    ]

    CUMULATIVE_STATS = [
        'swapin',       # amount of memory read in from swap space
        'swapout',      # amount of memory written out to swap space
        'minflt',       # minor page fault count
        'majflt',       # major page fault count
        'rd_req',       # number of read requests
        'rd_bytes',     # number of read bytes
        'wr_req',       # number of write requests
        'wr_bytes',     # number of written bytes
    ]

    ALL_STATS = ABSOLUTE_STATS + CUMULATIVE_STATS
