import ConfigParser
import logging

# Amount of memory to reserve for the host, in bytes
SYSTEM_MEM = 536870912

# Start reclaim in a container if the amount of free memory is less than
# min(<container RAM size> * HIGH_WMARK_RATIO, HIGH_WMARK_MAX)
HIGH_WMARK_RATIO = 0.02
HIGH_WMARK_MAX = 16777216  # bytes

# Treat a page as unused if it has not been touched for more than
MEM_IDLE_DELAY = 300  # seconds

# Do not take into account memory that is not used by a container if its
# relative portion is less than
MEM_IDLE_THRESH = 0.1

# Enable tcache?
USE_TCACHE = True

# Enable tswap?
USE_TSWAP = True


class _ConfigLoader:

    # option value types
    OPT_STR = 1
    OPT_INT = 2
    OPT_BOOL = 3
    OPT_FLOAT = 4

    def __init__(self, config_filename, config_section='DEFAULT', logger=None):
        self.config_filename = config_filename
        self.config_section = config_section
        self.logger = logger or logging.getLogger(__name__)
        self.parser = ConfigParser.RawConfigParser()

        # functions used to get an option value of each type;
        # must accept two args - config section and option name
        self.opt_getter = {
            self.OPT_STR: self.parser.get,
            self.OPT_INT: self.parser.getint,
            self.OPT_BOOL: self.parser.getboolean,
            self.OPT_FLOAT: self.parser.getfloat,
        }

    def update_opt(self, name, type):
        try:
            val = self.opt_getter[type](self.config_section, name)
        except (ConfigParser.Error, ValueError) as err:
            self.logger.warning("Error parsing config option '%s': %s" %
                                (name, err))
        else:
            globals()[name.upper()] = val

    def load(self):
        self.logger.debug("Loading config from file '%s'" %
                          self.config_filename)
        try:
            with open(self.config_filename, 'r') as fp:
                self.parser.readfp(fp)
        except (IOError, ConfigParser.Error) as err:
            self.logger.warning("Error reading config file: %s" % err)
            return

        self.update_opt('system_mem', self.OPT_INT)
        self.update_opt('mem_idle_delay', self.OPT_INT)
        self.update_opt('mem_idle_thresh', self.OPT_FLOAT)
        self.update_opt('high_wmark_ratio', self.OPT_FLOAT)
        self.update_opt('high_wmark_max', self.OPT_INT)
        self.update_opt('use_tcache', self.OPT_BOOL)
        self.update_opt('use_tswap', self.OPT_BOOL)


def load_from_file(config_filename, logger=None):
    _ConfigLoader(config_filename, logger=logger).load()
