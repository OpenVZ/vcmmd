from collections import namedtuple

from vcmmd.util import UINT64_MAX


_CONFIG_FIELDS = (
    'guarantee',
    'limit',
    'swap',
)


class Error(Exception):
    pass


class Config(namedtuple('Config', _CONFIG_FIELDS)):

    def __init__(self, *args, **kwargs):
        super(Config, self).__init__(*args, **kwargs)

        if self.guarantee > self.limit:
            raise ValueError('guarantee must be <= limit')

    def __str__(self):
        return '(guarantee=%s, limit=%s, swap=%s)' % self

    @staticmethod
    def from_dict(dict_, default=None):
        if default is None:
            default = DEFAULT_CONFIG
        kv = default._asdict()
        kv.update(dict_)
        return Config(**kv)

DEFAULT_CONFIG = Config(guarantee=0,
                        limit=UINT64_MAX,
                        swap=UINT64_MAX)


# All memory values are in bytes
_MEM_STATS_FIELDS = (
    'actual',           # actual allocation size
    'rss',              # resident set size
    'used',             # in use by guest OS
    'minflt',           # total # of minor page faults
    'majflt',           # total # of major page faults
)


class MemStats(namedtuple('Stats', _MEM_STATS_FIELDS)):
    pass

MemStats.__new__.__defaults__ = (0, ) * len(_MEM_STATS_FIELDS)


class VE(object):

    VE_TYPE = -1
    VE_TYPE_NAME = 'UNKNOWN'

    def __init__(self, name):
        self.__name = name
        self.__config = None
        self.__committed = False

        self.__mem_stats = MemStats()

    def __str__(self):
        return "%s '%s'" % (self.VE_TYPE_NAME, self.name)

    @property
    def name(self):
        return self.__name

    @property
    def config(self):
        return self.__config

    def _apply_config(self, config):
        self.set_mem_max(config.limit)
        self.set_swap_max(config.swap)

    def set_config(self, config):
        assert isinstance(config, Config)
        if self.committed:
            self._apply_config(config)
        self.__config = config

    @property
    def committed(self):
        return self.__committed

    def commit(self):
        assert self.config is not None
        self._apply_config(self.config)
        self.__committed = True

    @property
    def mem_stats(self):
        return self.__mem_stats

    def _fetch_mem_stats(self):
        '''Fetch memory statistics for this VE.

        Returns an object of Stats class.

        May raise Error.

        This function is supposed to be overridden in sub-class.
        '''
        return self.__mem_stats

    def update_stats(self):
        self.__mem_stats = self._fetch_mem_stats()

    def set_mem_range(self, low, high):
        self.set_mem_low(low)
        self.set_mem_high(high)

    def set_mem_low(self, value):
        '''Set best-effort memory protection.

        If the memory usage of a VE is below its low boundary, the VE's memory
        shouldn't be reclaimed if memory can be reclaimed from unprotected VEs.

        May raise Error.

        This function is supposed to be overridden in sub-class.
        '''
        pass

    def set_mem_high(self, value):
        '''Set memory usage throttle limit.

        If VE's memory usage goes over the high boundary, it should be
        throttled and put under heavy reclaim pressure. Going over the high
        limit never invokes the OOM killer and under extreme conditions the
        limit may be breached.

        May raise Error.

        This function is supposed to be overridden in sub-class.
        '''
        pass

    def set_mem_max(self, value):
        '''Set hard memory limit.

        This is the final protection mechanism. If a VE's memory usage reaches
        this limit and can't be reduced, the OOM killer is invoked in the VE.

        May raise Error.

        This function is supposed to be overridden in sub-class.
        '''
        pass

    def set_swap_max(self, value):
        '''Set hard swap limit.

        May raise Error.

        This function is supposed to be overridden in sub-class.
        '''
        pass
