import ConfigParser
import logging

_OPTIONS = {
    # name                              default

    # Amount of memory to reserve for the host, in bytes
    'SYSTEM_MEM':                       536870912,

    # Start reclaim in a container if the amount of free memory is less than
    # min(<container RAM size> * HIGH_WMARK_RATIO, HIGH_WMARK_MAX)
    'HIGH_WMARK_RATIO':                 0.02,
    'HIGH_WMARK_MAX':                   16777216,

    # Minimal time after which to treat a memory page as unused, in seconds
    'MEM_IDLE_DELAY':                   300,

    # Do not take into account memory that is not used by a container if its
    # relative portion is less than
    'MEM_IDLE_THRESH':                  0.1,

    # Enable tcache/tswap?
    'USE_TCACHE':                       True,
    'USE_TSWAP':                        True,
}


def _update_options():
    globals().update(_OPTIONS)


def load_from_file(filename, section='DEFAULT', logger=None):
    if not logger:
        logger = logging.getLogger(__name__)

    logger.debug("Loading config from file '%s' section '%s'" %
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
