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

    def set_config(self, config):
        assert isinstance(config, Config)
        self.__config = config

    @property
    def committed(self):
        return self.__committed

    def commit(self):
        self.__committed = True

    def __str__(self):
        return "%s '%s'" % (self.VE_TYPE_NAME, self.name)
