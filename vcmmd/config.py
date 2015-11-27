import ConfigParser
import logging

from sysinfo import MEM_TOTAL
from idlemem import ANON, FILE, NR_MEM_TYPES, MAX_AGE
from util import divroundup, clamp

_OPTIONS = {
    # name                              default

    # Enable tcache/tswap?
    'USE_TCACHE':                       True,
    'USE_TSWAP':                        True,

    # Amount of memory to reserve for the host, in bytes
    'SYSTEM_MEM':                       536870912,

    # Start reclaim in a container if the amount of free memory is less than
    # min(<container RAM size> * HIGH_WMARK_RATIO, HIGH_WMARK_MAX)
    'HIGH_WMARK_RATIO':                 0.02,
    'HIGH_WMARK_MAX':                   16777216,

    # If this option is enabled, the daemon will attempt to estimate working
    # set size of containers dynamically as they are running and adjust their
    # memory allocation quotas accordingly.
    'DYNAMIC_BALANCING':                True,

    ##
    # Remaining options are used only if DYNAMIC_BALANCING is enabled
    ##

    # Determines the portion of memory to scan for estimating idle memory size,
    # inverse ratio
    'MEM_IDLE_SAMPLING_RATIO':          20,

    # The interval between successive updates of idle memory size estimate,
    # in seconds
    'MEM_IDLE_DELAY':                   5,

    # Do not take into account memory that is not used by a container if its
    # relative portion is less than
    'MEM_IDLE_THRESH':                  0.1,

    # Assume a memory page is very unlikely to be used again if it has not been
    # touched for more than MEM_STALE_AGE seconds
    'MEM_STALE_AGE':                    300,

    # Start to account an anon/file page as idle if it has not been used for
    # this long, in seconds
    'ANON_IDLE_AGE':                    60,
    'FILE_IDLE_AGE':                    10,

    # An interval back in time to consider while estimating a decrease in a
    # working set of anon/file pages, in seconds. In other words, this defines
    # how fast a working set slacks off if being untouched.
    'ANON_WS_SLACK':                    180,
    'FILE_WS_SLACK':                    120,
}


def _age_to_shift(age):
    return clamp(divroundup(age, MEM_IDLE_DELAY) - 1, 0, MAX_AGE)


def _update_options():
    globals().update(_OPTIONS)

    globals()['SYSTEM_MEM'] = clamp(SYSTEM_MEM, 0, MEM_TOTAL)
    globals()['MEM_AVAIL'] = MEM_TOTAL - SYSTEM_MEM
    globals()['HIGH_WMARK_RATIO'] = clamp(HIGH_WMARK_RATIO, 0., 1.)
    globals()['HIGH_WMARK_MAX'] = max(HIGH_WMARK_MAX, 0)
    globals()['MEM_IDLE_SAMPLING_RATIO'] = max(MEM_IDLE_SAMPLING_RATIO, 1)
    globals()['MEM_IDLE_DELAY'] = max(MEM_IDLE_DELAY, 1)
    globals()['MEM_IDLE_THRESH'] = clamp(MEM_IDLE_THRESH, 0., 1.)
    globals()['MEM_STALE_SHIFT'] = clamp(MEM_STALE_AGE / MEM_IDLE_DELAY,
                                         1, MAX_AGE)
    globals()['MEM_IDLE_SHIFT'] = {}
    globals()['MEM_SLACK_SHIFT'] = {}
    for t in xrange(NR_MEM_TYPES):
        MEM_IDLE_SHIFT[t] = _age_to_shift(
            {
                ANON: ANON_IDLE_AGE,
                FILE: FILE_IDLE_AGE,
            }[t])
        MEM_SLACK_SHIFT[t] = _age_to_shift(
            {
                ANON: ANON_WS_SLACK,
                FILE: FILE_WS_SLACK,
            }[t])


def load_from_file(filename, section='DEFAULT', logger=None):
    if not logger:
        logger = logging.getLogger(__name__)

    logger.info("Loading config from file '%s' section '%s'" %
                (filename, section))

    class MyConfigParser(ConfigParser.RawConfigParser):
        def optionxform(self, option):
            return str.upper(option)

    # 'defaults' wants strings for values, so we cannot just pass _OPTIONS
    parser = MyConfigParser(defaults={k: str(v) for (k, v) in
                                      _OPTIONS.iteritems()})
    try:
        with open(filename, 'r') as fp:
            parser.readfp(fp)
    except (IOError, ConfigParser.Error) as err:
        logger.error("Error reading config: %s" % err)
        return

    if section != 'DEFAULT' and not parser.has_section(section):
        logger.error("No section '%s' found in config file" % section)
        return

    for (name, val) in parser.items(section):
        if name not in _OPTIONS:
            logger.warning("Unknown config option '%s'" % name)
            continue
        try:
            _OPTIONS[name] = {
                int: parser.getint,
                float: parser.getfloat,
                bool: parser.getboolean,
            }[type(_OPTIONS[name])](section, name)
        except (ValueError, ConfigParser.Error) as err:
            logger.error("Error parsing config option '%s': %s" % (name, err))
            continue

    _update_options()

_update_options()
