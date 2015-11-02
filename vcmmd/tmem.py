import logging

import config

logger = logging.getLogger(__name__)


def _set_active(mod, active):
    available = False
    try:
        with open("/sys/module/%s/parameters/enabled" % mod, 'r') as f:
            if f.read(1) == 'Y':
                available = True
    except IOError:
        pass
    if not available:
        if active:
            logger.error("Failed to activate %s: Module is unavailable" % mod)
        return
    try:
        with open("/sys/module/%s/parameters/active" % mod, 'w') as f:
            f.write('Y' if active else 'N')
    except IOError as err:
        logger.error("Failed to %s %s: %s" %
                     ("activate" if active else "deactivate", mod, err))
    else:
        logger.info("%s %s" %
                    (mod, "activated" if active else "deactivated"))


def initialize():
    if config.USE_TCACHE:
        _set_active('tcache', True)
    if config.USE_TSWAP:
        _set_active('tswap', True)


def finilize():
    if config.USE_TCACHE:
        _set_active('tcache', False)
    if config.USE_TSWAP:
        _set_active('tswap', False)
