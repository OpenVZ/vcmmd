from __future__ import absolute_import

import time
import psutil
from collections import namedtuple

from vcmmd.config import VCMMDConfig
from vcmmd.cgroup import MemoryCgroup
from vcmmd.util.limits import UINT64_MAX

_MAX_EFFECTIVE_LIMIT = psutil.virtual_memory().total

_CONFIG_FIELDS = (
    'guarantee',
    'limit',
    'swap',
)


class Error(Exception):
    pass


class Config(namedtuple('Config', _CONFIG_FIELDS)):
    '''Represents a VE's memory configuration.

    guarantee:      VE memory guarantee

                    A VE should be always given at least as much memory as
                    specified by this parameter.

    limit:          VE memory limit

                    Maximal size of host memory that can be used by a VE.
                    Must be >= guarantee.

    swap:           VE swap limit

                    Maximal size of host swap that can be used by a VE.

    All values are in bytes.
    '''

    def __init__(self, *args, **kwargs):
        super(Config, self).__init__(*args, **kwargs)

        if self.guarantee > self.limit:
            raise ValueError('guarantee must be <= limit')

    def __str__(self):
        return '(guarantee=%s, limit=%s, swap=%s)' % self

    @property
    def effective_limit(self):
        return min(self.limit, _MAX_EFFECTIVE_LIMIT)

    @staticmethod
    def from_dict(dict_, default=None):
        '''Make a Config from a dict.

        Some fields may be omitted in which case values given in 'default' will
        be used. 'default' must be an instance of a Config or None. If it is
        None, global default values will be used for omitted fields.
        '''
        if default is None:
            default = DEFAULT_CONFIG
        kv = default._asdict()
        kv.update(dict_)
        return Config(**kv)

DEFAULT_CONFIG = Config(guarantee=0,
                        limit=UINT64_MAX,
                        swap=UINT64_MAX)


_MEM_STATS_FIELDS = (
    'rss',              # resident set size
    'actual',           # current balloon value
    'memtotal',         # total amount of memory as seen by guest OS
    'memfree',          # amount of memory left completely unused by guest OS
    'wss',              # working set size reported by guest OS
    'swapin',           # total amount of memory read in from swap space
    'swapout',          # total amount of memory written out to swap space
    'minflt',           # total # of minor page faults
    'majflt',           # total # of major page faults
)


class MemStats(namedtuple('MemStats', _MEM_STATS_FIELDS)):
    '''VE memory statistics.

    All memory values are in bytes.

    If a statistic is unavailable, its value will be < 0.
    '''
    pass

MemStats.__new__.__defaults__ = (-1, ) * len(_MEM_STATS_FIELDS)


_IO_STATS_FIELDS = (
    'rd_req',           # number of read requests
    'rd_bytes',         # number of read bytes
    'wr_req',           # number of write requests
    'wr_bytes',         # number of written bytes
)


class IOStats(namedtuple('IOStats', _IO_STATS_FIELDS)):
    '''VE IO statistics.

    If a statistic is unavailable, its value will be < 0.
    '''
    pass

IOStats.__new__.__defaults__ = (-1, ) * len(_IO_STATS_FIELDS)


def _stats_delta(cur, prev, fields, timeout):
    '''Convert cumulative statistic counters to delta per sec.

    'cur' and 'prev' is the current and the previous values of the statistics,
    respectively. 'fields' is names of fields containing cumulative counters to
    be converted; all other fields are copied from 'cur'. 'timeout' is the
    time that has passed since the last update.

    If 'prev' is None, all cumulative counters are set to -1.
    '''
    klass = type(cur)
    cur = cur._asdict()
    prev = prev._asdict() if prev is not None else {}
    for f in fields:
        cur_val = cur[f]
        prev_val = prev.get(f, -1)
        cur[f] = (int((cur_val - prev_val) / timeout)
                  if cur_val >= 0 and prev_val >= 0 and timeout > 0
                  else -1)
    return klass(**cur)


class VE(object):

    VE_TYPE = -1

    def __init__(self, name):
        self.__name = name
        self.__config = None
        self.__active = False

        self.__last_stats_update = 0
        self.__prev_mem_stats_raw = None
        self.__prev_io_stats_raw = None

        self.__mem_stats = MemStats()
        self.__io_stats = IOStats()

        # Additional memory that should be taken into account
        # when calculating memory.low
        self.mem_overhead = 0

        self.policy_priv = None

    def __str__(self):
        return "%s '%s'" % (self.__class__.__name__, self.name)

    @property
    def name(self):
        '''Return VE name.
        '''
        return self.__name

    @property
    def config(self):
        '''Return current VE config.
        '''
        return self.__config

    def _apply_config(self, config):
        '''Try to apply VE config.

        A sub-class is supposed to override this function to propagate config
        changes to the underlying implementation.

        This function May raise Error, in which case config update will be
        aborted.
        '''
        pass

    def set_config(self, config):
        '''Update VE config.

        If the VE is active, it will try to apply the new config right away and
        throw Error in case of failure. Otherwise, config will be applied only
        when VE gets activated.
        '''
        if self.active:
            self._apply_config(config)
        self.__config = config

    @property
    def active(self):
        '''Return True iff VE is active.

        Active VEs may be tuned by the load manager, while inactive ones may
        not (adjusting configuration and statistics update are not supposed to
        work for inactive VEs). To activate a VE call the 'activate' method.
        '''
        return self.__active

    def activate(self):
        '''Activate VE.

        This function marks a VE as active. It also tries to apply the VE
        config. The latter may fail hence this function may throw Error.

        This function is supposed to be called after a VE has been started or
        resumed.
        '''
        self._apply_config(self.config)
        self.__active = True

    def deactivate(self):
        '''Deactivate VE.

        This function marks a VE as inactive. It never raises an exception.

        This function is supposed to be called before pausing or suspending a
        VE.
        '''
        self.__active = False

    @property
    def mem_stats(self):
        '''Return memory statistics for this VE.

        The value is cached. To get up-to-date stats, one need to call
        'update_stats' first.

        Note, cumulative counters are reported as delta since the last update
        per second.
        '''
        return self.__mem_stats

    @property
    def io_stats(self):
        '''Return IO statistics for this VE.

        The value is cached. To get up-to-date stats, one need to call
        'update_stats' first.

        Note, cumulative counters are reported as delta since the last update
        per second.
        '''
        return self.__io_stats

    def update_stats(self):
        '''Update statistics for this VE.

        May raise Error.
        '''
        mem_stats = self._fetch_mem_stats()
        io_stats = self._fetch_io_stats()

        now = time.time()
        timeout = now - self.__last_stats_update
        self.__last_stats_update = now

        # Convert cumulative counters to delta per sec

        self.__mem_stats = _stats_delta(mem_stats, self.__prev_mem_stats_raw,
                                        ['swapin', 'swapout',
                                         'minflt', 'majflt'],
                                        timeout)

        self.__io_stats = _stats_delta(io_stats, self.__prev_io_stats_raw,
                                       ['rd_req', 'rd_bytes',
                                        'wr_req', 'wr_bytes'],
                                       timeout)

        self.__prev_mem_stats_raw = mem_stats
        self.__prev_io_stats_raw = io_stats

    @staticmethod
    def enable_idle_mem_tracking():
        '''Enable idle memory tracking.

        Must be called for 'idle_ratio' to work.
        '''
        cfg = VCMMDConfig()
        sampling = cfg.get_num('VE.IdleMemTracking.Sampling',
                               default=0.1, minimum=0.01, maximum=1.0)
        period = cfg.get_num('VE.IdleMemTracking.Period',
                             default=60, integer=True, minimum=1)
        # Both containers and VMs currently use the infrastructure provided by
        # memory cgroup for tracking idle memory.
        MemoryCgroup.set_idle_mem_sampling(sampling)
        MemoryCgroup.set_idle_mem_period(period)

    def idle_ratio(self, age=0):
        '''Return an estimate of the portion of memory that have been found
        idle for more than 'age' idle scan periods.

        Only relevant if 'enable_idle_mem_tracking' was called. The value is
        updated each 'period' seconds.

        This function is supposed to be overridden in sub-class.
        '''
        return 0.0

    def _fetch_mem_stats(self):
        '''Fetch memory statistics for this VE.

        Returns an object of MemStats class.

        May raise Error.

        This function is supposed to be overridden in sub-class.
        '''
        pass

    def _fetch_io_stats(self):
        '''Fetch IO statistics for this VE.

        Returns an object of IOStats class.

        May raise Error.

        This function is supposed to be overridden in sub-class.
        '''
        pass

    def set_mem_protection(self, value):
        '''Set memory best-effort protection.

        If memory usage of a VE is below this value, the VE's memory shouldn't
        be reclaimed on host pressure if memory can be reclaimed from
        unprotected VEs.

        May raise Error.

        This function is supposed to be overridden in sub-class.
        '''
        pass

    def set_mem_target(self, value):
        '''Set memory allocation target.

        This function sets memory consumption target for a VE. Note, it does
        not necessarily mean that the VE memory usage will reach the target
        instantly or even any time soon - in fact, it may not reach it at all
        in case allocation is reduced. However, reducing the value will put the
        VE under heavy local memory pressure forcing it to release its memory
        to the host.

        May raise Error.

        This function is supposed to be overridden in sub-class.
        '''
        pass
