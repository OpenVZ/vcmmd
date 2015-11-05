def strmemsize(val):
    if val > 10 * 1024 * 1024:
        return str(val / (1024 * 1024)) + 'M'
    if val > 10 * 1024:
        return str(val / 1024) + 'K'
    return str(val)


def divroundup(n, d):
    return (n + d - 1) / d


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
