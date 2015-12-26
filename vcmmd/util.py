import copy
import optparse


INT64_MAX = int(2**63-1)
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


# borrowed from chromium
class OptionWithMemsize(optparse.Option):

    @staticmethod
    def _CheckMemsize(option, opt, value):
        # Note: purposely no 'b' suffix, since that makes 0x12b ambiguous.
        multiplier_table = [
            ('g', 1024 * 1024 * 1024),
            ('m', 1024 * 1024),
            ('k', 1024),
            ('', 1),
        ]
        for (suffix, multiplier) in multiplier_table:
            if value.lower().endswith(suffix):
                new_value = value
                if suffix:
                    new_value = new_value[:-len(suffix)]
                try:
                    # Convert w/ base 0 (handles hex, binary, octal, etc)
                    return int(new_value, 0) * multiplier
                except ValueError:
                    # Pass and try other suffixes; not useful now, but may be
                    # useful later if we ever allow B vs. GB vs. GiB.
                    pass
        raise optparse.OptionValueError("option %s: invalid memsize value: %r"
                                        % (opt, value))

    TYPES = optparse.Option.TYPES + ('memsize',)
    TYPE_CHECKER = copy.copy(optparse.Option.TYPE_CHECKER)

OptionWithMemsize.TYPE_CHECKER['memsize'] = OptionWithMemsize._CheckMemsize
