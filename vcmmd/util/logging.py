# hardcode logging constants to avoid importing the logging module
LOG_LEVELS = {
    'debug': 10,
    'info': 20,
    'warn': 30,
    'error': 40,
    'critical': 50,
}


class LoggerWriter:
    '''Helper for redirecting stdout/stderr to a logger.

    Usage example:

    logger = logging.getLogger()
    sys.stdout = LoggerWriter(logger, logging.INFO)
    sys.stderr = LoggerWriter(logger, logging.CRITICAL)
    '''

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

    def flush(self):
        pass
