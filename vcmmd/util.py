UINT64_MAX = int(2**64-1)


class LoggerWriter:
    ##
    # Helper for redirecting stdout/stderr to a logger.

    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
        self._buf = ''

    def write(self, message):
        l = message.split('\n')
        l[0] = self._buf + l[0]
        for s in l[:-1]:
            self.logger.log(self.level, s)
        self._buf = l[-1]
