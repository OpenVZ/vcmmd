from __future__ import absolute_import

import sys
import os
import logging
import signal
import optparse
import time
import traceback

import daemon
import daemon.pidfile

from vcmmd.config import VCMMDConfig
from vcmmd.ldmgr import LoadManager
from vcmmd.rpc.dbus.server import RPCServer
from vcmmd.util.logging import LoggerWriter
from vcmmd.util.threading import setup_thread_excepthook


class _App(object):

    PID_FILE = '/var/run/vcmmd.pid'
    LOG_FILE = '/var/log/vcmmd.log'
    DEFAULT_CONFIG = '/etc/vz/vcmmd.conf'

    def __init__(self):
        self.parse_args()
        self.init_logging()

        if self.opts.interactive:
            self.run_interactive()
        else:
            self.run_daemon()

    def parse_args(self):
        parser = optparse.OptionParser("Usage: %prog [-i] [-c CONFIG]")
        parser.add_option("-i", action="store_true", dest="interactive",
                          help="run interactive (not a daemon)")
        parser.add_option("-c", type="string", dest="config",
                          default=self.DEFAULT_CONFIG,
                          help="path to config file")

        (opts, args) = parser.parse_args()
        if args:
            parser.error("incorrect number of arguments")

        self.opts = opts

    def init_logging(self):
        logger = logging.getLogger('vcmmd')

        fmt = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s",
            "%Y-%m-%d %H:%M:%S")

        fh = logging.StreamHandler() if self.opts.interactive else \
            logging.FileHandler(self.LOG_FILE)
        fh.setFormatter(fmt)

        logger.addHandler(fh)

        self.logger = logger
        self.logger_stream = fh.stream  # for DaemonContext:files_preserve

    def run(self):
        # Redirect stdout and stderr to logger
        sys.stdout = LoggerWriter(self.logger, logging.INFO)
        sys.stderr = LoggerWriter(self.logger, logging.CRITICAL)

        self.logger.info('Started')

        cfg = VCMMDConfig()
        cfg.load(self.opts.config)
        if cfg.get_bool('Logging.Debug', False):
            self.logger.setLevel(logging.DEBUG)

        ldmgr = LoadManager()
        rpcsrv = RPCServer(ldmgr)

        # threading.Event would fit better here, but it ignores signals.
        self.should_stop = False
        while not self.should_stop:
            time.sleep(1)

        rpcsrv.shutdown()
        ldmgr.shutdown()

        self.logger.info('Stopped')

    def sighandler(self, signum, frame):
        self.should_stop = True

    def run_interactive(self):
        signal.signal(signal.SIGINT, self.sighandler)
        self.run()

    def run_daemon(self):
        with daemon.DaemonContext(
                pidfile=daemon.pidfile.TimeoutPIDLockFile(self.PID_FILE, -1),
                files_preserve=[self.logger_stream],
                signal_map={signal.SIGTERM: self.sighandler}):
            self.run()


def _excepthook(exc_type, exc_value, exc_traceback):
    sys.stderr.write('Terminating program due to unhandled exception:\n' +
                     ''.join(traceback.format_exception(exc_type, exc_value,
                                                        exc_traceback)))
    os._exit(1)  # force all threads to exit


def main():
    # setup handler for uncaught exceptions in all threads
    setup_thread_excepthook()
    sys.excepthook = _excepthook

    _App()

if __name__ == "__main__":
    main()
