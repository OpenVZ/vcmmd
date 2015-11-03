import daemon
import daemon.pidfile
import logging
import optparse
import signal
import socket
import sys
import threading
import time

import config
import memcg
import rpc
import tmem
import util

RPC_SOCKET = "/var/run/vcmmd.socket"
CONFIG_FILE = "/etc/vz/vcmmd.conf"
STATE_FILE = "/var/run/vcmmd.state"
PID_FILE = "/var/run/vcmmd.pid"
LOG_FILE = "/var/log/vcmmd.log"

_shutdown_request = False


def _sighandler(signum, frame):
    global _shutdown_request
    _shutdown_request = True


def _serve_forever(ldmgr, rpcsrv):
    rpcsrv_thread = threading.Thread(target=rpcsrv.serve_forever)
    rpcsrv_thread.start()

    ldmgr_thread = threading.Thread(target=ldmgr.serve_forever)
    ldmgr_thread.start()

    while not _shutdown_request:
        if not rpcsrv_thread.isAlive():
            break
        if not ldmgr_thread.isAlive():
            break
        time.sleep(1)

    rpcsrv.shutdown()
    ldmgr.shutdown()


def _run():
    logger = logging.getLogger()

    # DaemonContext closes stdout and stderr, redirect them to the logger.
    sys.stdout = util.LoggerWriter(logger, logging.INFO)
    sys.stderr = util.LoggerWriter(logger, logging.CRITICAL)

    logger.info("Started")

    config.load_from_file(filename=CONFIG_FILE, logger=logger)

    tmem.logger = logger
    tmem.initialize()

    ldmgr = memcg.DefaultMemCgManager(state_filename=STATE_FILE, logger=logger)

    try:
        rpcsrv = rpc.RPCServer(server_address=RPC_SOCKET,
                               load_manager=ldmgr, logger=logger)
    except socket.error as err:
        logger.critical("Failed to activate RPC server: %s" % err)
    else:
        _serve_forever(ldmgr, rpcsrv)

    tmem.finilize()

    logger.info("Stopped")


def main():
    parser = optparse.OptionParser("Usage: %prog [-i] [-d]")
    parser.add_option("-i", action="store_true", dest="interactive",
                      help="run interactive (not a daemon)")
    parser.add_option("-d", action="store_true", dest="debug",
                      help="increase verbosity to debug level")

    (options, args) = parser.parse_args()
    if args:
        parser.error("incorrect number of arguments")

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if options.debug else logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s",
                            "%Y-%m-%d %H:%M:%S")
    fh = logging.StreamHandler() if options.interactive else \
        logging.FileHandler(LOG_FILE)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    if options.interactive:
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
