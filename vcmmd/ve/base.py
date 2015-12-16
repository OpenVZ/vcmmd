from collections import namedtuple


_CONFIG_FIELDS = (
    'guarantee',
    'limit',
    'max_limit',
    'swap',
)


class Config(namedtuple('Config', _CONFIG_FIELDS)):

    def __init__(self, *args, **kwargs):
        super(Config, self).__init__(*args, **kwargs)

        if self.guarantee > self.limit:
            raise ValueError('guarantee must be <= limit')
        if self.limit > self.max_limit:
            raise ValueError('limit must be <= max_limit')

    def __str__(self):
        return '(guarantee=%s, limit=%s, max_limit=%s, swap=%s)' % self


class VE(object):

    VE_TYPE = -1
    VE_TYPE_NAME = 'UNKNOWN'

    def __init__(self, name):
        self.__name = name
        self.__config = None
        self.__committed = False

    @property
    def name(self):
        return self.__name

    @property
    def config(self):
        return self.__config

    def _apply_config(self, config):
        self.set_mem_max(config.max_limit)
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

    def set_mem_range(self, low, high):
        self.set_mem_low(low)
        self.set_mem_high(high)

    def __str__(self):
        return "%s '%s'" % (self.VE_TYPE_NAME, self.name)

    def set_mem_low(self, value):
        '''Set best-effort memory protection.

        If the memory usage of a VE is below its low boundary, the VE's memory
        shouldn't be reclaimed if memory can be reclaimed from unprotected VEs.

        This function is supposed to be overwritten in sub-class.
        '''
        pass

    def set_mem_high(self, value):
        '''Set memory usage throttle limit.

        If VE's memory usage goes over the high boundary, it should be
        throttled and put under heavy reclaim pressure. Going over the high
        limit never invokes the OOM killer and under extreme conditions the
        limit may be breached.

        This function is supposed to be overwritten in sub-class.
        '''
        pass

    def set_mem_max(self, value):
        '''Set hard memory limit.

        This is the final protection mechanism. If a VE's memory usage reaches
        this limit and can't be reduced, the OOM killer is invoked in the VE.

        This function is supposed to be overwritten in sub-class.
        '''
        pass

    def set_swap_max(self, value):
        '''Set hard swap limit.

        This function is supposed to be overwritten in sub-class.
        '''
        pass
