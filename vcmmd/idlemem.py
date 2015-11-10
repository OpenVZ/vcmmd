import logging
import threading
import time

import idlememscan
import util

ANON = 0
FILE = 1
NR_MEM_TYPES = 2

MAX_AGE = idlememscan.MAX_AGE

logger = logging.getLogger(__name__)

##
# Dict: cg path -> idle stat (as returned by idlememscan.result)
# Updated periodically by _Scanner.__scan_done. Mutable.
last_idle_stat = {}


@util.SingletonDecorator
class _Scanner:

    IDLE_STAT_ZERO = ((0, ) * (MAX_AGE + 1), ) * NR_MEM_TYPES

    ##
    # interval: interval between updates, in seconds
    # on_update: callback to run on each update
    #
    # To avoid CPU bursts, the estimator will distribute the scanning in time
    # so that a full scan fits in the given interval.

    def __init__(self):
        self.interval = 0
        self.on_update = None
        self.__warned_lag = False
        self.__is_shut_down = threading.Event()
        self.__is_shut_down.set()
        self.__should_shut_down = threading.Event()
        self.__should_shut_down.clear()

    @staticmethod
    def __time():
        return time.time()

    # like sleep, but is interrupted by shutdown
    def __sleep(self, seconds):
        self.__should_shut_down.wait(seconds)

    def __init_scan(self):
        self.__scan_iters = idlememscan.nr_iters()
        self.__iter = 0
        self.__scan_time = 0.0
        self.__scan_start = self.__time()

    def __scan_done(self):
        global last_idle_stat
        last_idle_stat = idlememscan.result()
        if self.on_update:
            self.on_update()

    def __throttle(self):
        iters_left = self.__scan_iters - self.__iter
        time_left = self.interval - (self.__time() - self.__scan_start)
        time_required = iters_left * self.__scan_time / self.__iter
        if time_required > time_left:
            # only warn about significant lags (> 1% of interval)
            if not self.__warned_lag and \
                    time_required - time_left > self.interval / 100.0:
                logger.warning("Memory scanner is lagging behind "
                               "(%.2f s left, %s s required)" %
                               (time_left, time_required))
                self.__warned_lag = True
            return
        self.__sleep((time_left - time_required) / iters_left
                     if iters_left > 0 else time_left)

    def __scan_iter(self):
        start = self.__time()
        done = idlememscan.iter()
        self.__scan_time += self.__time() - start
        self.__iter += 1
        self.__throttle()
        return done

    ##
    # Scan memory periodically counting unused pages until shutdown.

    def serve_forever(self):
        self.__is_shut_down.clear()
        try:
            idlememscan.set_sampling(self.sampling)
            self.__init_scan()
            while not self.__should_shut_down.is_set():
                if self.__scan_iter():
                    self.__scan_done()
                    self.__init_scan()
        finally:
            self.__should_shut_down.clear()
            self.__is_shut_down.set()

    ##
    # Stop the serve_forever loop and wait until it exits.

    def shutdown(self):
        if self.__is_shut_down.is_set():
            return
        self.__should_shut_down.set()
        self.__is_shut_down.wait()


def start_background_scan(interval, sampling, on_update=None):
    scanner = _Scanner()
    scanner.interval = interval
    scanner.sampling = sampling
    scanner.on_update = on_update
    threading.Thread(target=scanner.serve_forever).start()


def stop_background_scan():
    _Scanner().shutdown()
