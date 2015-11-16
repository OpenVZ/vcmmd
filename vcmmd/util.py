INT64_MAX = 2**63-1
UINT64_MAX = 2**64-1


def strmemsize(val):
    if val >= 10 ** 10:
        return str(val >> 30) + 'G'
    if val >= 10 ** 7:
        return str(val >> 20) + 'M'
    if val >= 10 ** 4:
        return str(val >> 10) + 'K'
    return str(val)


def divroundup(n, d):
    return (n + d - 1) / d


def clamp(val, min_, max_):
    return min(max(val, min_), max_)


class LoggerWriter:
    ##
    # Helper for redirecting stdout/stderr to a logger.

    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
        self.__buf = ''

    def write(self, message):
        l = message.split('\n')
        l[0] = self.__buf + l[0]
        for s in l[:-1]:
            self.logger.log(self.level, s)
        self.__buf = l[-1]


class SingletonDecorator:

    def __init__(self, klass):
        self.klass = klass
        self.instance = None

    def __call__(self, *args, **kwargs):
        if self.instance is None:
            self.instance = self.klass(*args, **kwargs)
        return self.instance
