import ConfigParser
import logging

from sysinfo import MEM_TOTAL
from idlemem import ANON, FILE, NR_MEM_TYPES, MAX_AGE
from util import divroundup, clamp

_OPTIONS = {
    # name                              default

    # Amount of memory to reserve for the host, in bytes
    'SYSTEM_MEM':                       536870912,

    # Start reclaim in a container if the amount of free memory is less than
    # min(<container RAM size> * HIGH_WMARK_RATIO, HIGH_WMARK_MAX)
    'HIGH_WMARK_RATIO':                 0.02,
    'HIGH_WMARK_MAX':                   16777216,

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
    'MEM_STALE_AGE':                    360,

    # Start to account an anon/file page as idle if it has not been used for
    # this long, in seconds
    'ANON_IDLE_AGE':                    120,
    'FILE_IDLE_AGE':                    10,

    # Enable tcache/tswap?
    'USE_TCACHE':                       True,
    'USE_TSWAP':                        True,
}


def _update_options():
    globals().update(_OPTIONS)

    globals()['MEM_AVAIL'] = max(MEM_TOTAL - SYSTEM_MEM, 1)
    globals()['MEM_STALE_SHIFT'] = clamp(MEM_STALE_AGE / MEM_IDLE_DELAY,
                                         1, MAX_AGE)
    globals()['MEM_IDLE_SHIFT'] = {
        t: clamp(
            divroundup(
                {
                    ANON: ANON_IDLE_AGE,
                    FILE: FILE_IDLE_AGE,
                }[t], MEM_IDLE_DELAY) - 1,
            0, MAX_AGE)
        for t in xrange(NR_MEM_TYPES)
    }


def load_from_file(filename, section='DEFAULT', logger=None):
    if not logger:
        logger = logging.getLogger(__name__)

    logger.info("Loading config from file '%s' section '%s'" %
                (filename, section))

    parser = ConfigParser.RawConfigParser()
    try:
        with open(filename, 'r') as fp:
            parser.readfp(fp)
    except (IOError, ConfigParser.Error) as err:
        logger.warning("Error reading config: %s" % err)
        return

    for name in _OPTIONS:
        try:
            _OPTIONS[name] = {
                int: parser.getint,
                float: parser.getfloat,
                bool: parser.getboolean,
            }[type(_OPTIONS[name])](section, name)
        except (ValueError, ConfigParser.Error) as err:
            logger.warning("Error parsing config option '%s': %s" %
                           (name, err))
            continue

    _update_options()

_update_options()
