from __future__ import absolute_import

import sys
import logging
import signal
import optparse
import time

import daemon
import daemon.pidfile

from vcmmd.ldmgr import LoadManager
from vcmmd.rpc.dbus import RPCServer
from vcmmd.util.logging import LoggerWriter

PID_FILE = '/var/run/vcmmd.pid'
LOG_FILE = '/var/log/vcmmd.log'

_should_stop = False


def _sighandler(signum, frame):
    global _should_stop
    _should_stop = True


def _run():
    # DaemonContext closes stdout and stderr, redirect them to the logger.
    logger = logging.getLogger()
    sys.stdout = LoggerWriter(logger, logging.INFO)
    sys.stderr = LoggerWriter(logger, logging.CRITICAL)

    logger.info('Started')

    ldmgr = LoadManager(logger=logger)
    rpcsrv = RPCServer(ldmgr)

    # threading.Event would fit better here, but it ignores signals.
    while not _should_stop:
        time.sleep(1)

    rpcsrv.shutdown()
    ldmgr.shutdown()

    logger.info('Stopped')


def main():
    parser = optparse.OptionParser("Usage: %prog [-i] [-d]")
    parser.add_option("-i", action="store_true", dest="interactive",
                      help="run interactive (not a daemon)")
    parser.add_option("-d", action="store_true", dest="debug",
                      help="increase verbosity to debug level")

    (opts, args) = parser.parse_args()
    if args:
        parser.error("incorrect number of arguments")

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if opts.debug else logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s",
                            "%Y-%m-%d %H:%M:%S")
    fh = logging.StreamHandler() if opts.interactive else \
        logging.FileHandler(LOG_FILE)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    if opts.interactive:
        signal.signal(signal.SIGINT, _sighandler)
        _run()
    else:
        context = daemon.DaemonContext(
            pidfile=daemon.pidfile.TimeoutPIDLockFile(PID_FILE, -1),
            files_preserve=[fh.stream],
            signal_map={signal.SIGTERM: _sighandler}
        )
        with context:
            _run()

if __name__ == "__main__":
    main()
