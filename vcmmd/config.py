import ConfigParser
import logging

# Config options are stored in an ini file. When parsed, the value of option
# NAME living in section SECTION is stored in global variable NAME__SECTION.
# List of all config options with their default values is given below.

##
# core section

# Maximal memory portion that can be reserved for containers.
CORE__MAX_RESERVATION = 0.75

# Amount of memory to reserve for the host, in bytes
CORE__SYSTEM_MEM = 536870912

# Start reclaim in a container if the amount of free memory is less than
# min(<container RAM size> * memcg.high, memcg.high_max)
MEMCG__HIGH = 0.02
MEMCG__HIGH_MAX = 16777216  # bytes

##
# memcg section

# Path to memory cgroup mount point
MEMCG__ROOT_PATH = "/sys/fs/cgroup/memory"

# Treat a page as unused if it has not been touched for more than
MEMCG__MEM_INUSE_TIME = 300  # seconds

# Do not take into account memory that is not used by a container if its
# relative portion is less than
MEMCG__MIN_UNUSED_MEM = 0.1

##
# tmem section

# Enable tcache?
TMEM__TCACHE = True

# Enable tswap?
TMEM__TSWAP = True


class _ConfigLoader:

    # option value types
    OPT_STR = 1
    OPT_INT = 2
    OPT_BOOL = 3
    OPT_FLOAT = 4

    def __init__(self, config_filename, logger=None):
        self.config_filename = config_filename
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

    def update_opt(self, full_name, type):
        section, name = full_name.split('.')
        try:
            val = self.opt_getter[type](section, name)
        except (ConfigParser.Error, ValueError) as err:
            self.logger.warning("Error parsing config option '%s': %s" %
                                (full_name, err))
        else:
            globals()[section.upper() + '__' + name.upper()] = val

    def load(self):
        self.logger.debug("Loading config from file '%s'" %
                          self.config_filename)
        try:
            with open(self.config_filename, 'r') as fp:
                self.parser.readfp(fp)
        except (IOError, ConfigParser.Error) as err:
            self.logger.warning("Error reading config file: %s" % err)
            return

        self.update_opt('core.max_reservation', self.OPT_FLOAT)
        self.update_opt('core.system_mem', self.OPT_INT)

        self.update_opt('memcg.root_path', self.OPT_STR)
        self.update_opt('memcg.mem_inuse_time', self.OPT_INT)
        self.update_opt('memcg.min_unused_mem', self.OPT_FLOAT)
        self.update_opt('memcg.high', self.OPT_FLOAT)
        self.update_opt('memcg.high_max', self.OPT_INT)

        self.update_opt('tmem.tcache', self.OPT_BOOL)
        self.update_opt('tmem.tswap', self.OPT_BOOL)


def load_from_file(config_filename, logger=None):
    _ConfigLoader(config_filename, logger).load()
