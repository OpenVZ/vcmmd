import logging
import os
import stat
import threading
import time

import config
import kpageutil
import sysinfo
import util


logger = logging.getLogger(__name__)

if os.path.exists("/sys/kernel/mm/page_idle/bitmap"):
    kpageutil.init(sysinfo.END_PFN)
    AVAILABLE = True
else:
    AVAILABLE = False


@util.SingletonDecorator
class _Scanner:

    SCAN_CHUNK = 32768

    ##
    # interval: interval between updates, in seconds
    # on_update: callback to run on each update
    #
    # To avoid CPU bursts, the estimator will distribute the scanning in time
    # so that a full scan fits in the given interval.

    def __init__(self):
        self.interval = 0
        self.on_update = None
        self.__idle_stat = {}
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
        self.__nr_unused = {}
        self.__scan_pfn = 0
        self.__scan_time = 0.0
        self.__scan_start = self.__time()
        self.__warned = False

    # kpageutil.count_idle_pages uses cgroup ino as a key in the resulting
    # dictionary while we want it to be referenced by cgroup name. This
    # functions does the conversion.
    def __update_idle_stat(self):
        result = {}
        Z = (0, 0)
        for name in os.listdir(config.MEMCG__ROOT_PATH):
            path = os.path.join(config.MEMCG__ROOT_PATH, name)
            if not os.path.isdir(path):
                continue
            cnt = Z
            for root, subdirs, files in os.walk(path):
                try:
                    ino = os.stat(root)[stat.ST_INO]
                except OSError:  # cgroup dir removed?
                    continue
                cnt = map(sum, zip(cnt, self.__nr_unused.get(ino, Z)))
            # convert pages to bytes
            result[name] = tuple(x * sysinfo.PAGE_SIZE for x in cnt)
        self.__idle_stat = result
        logger.debug("Unused memory estimate (anon/file)): %s" %
                     "; ".join('%s: %s/%s' % (k,
                                              util.strmemsize(v1),
                                              util.strmemsize(v2))
                               for k, (v1, v2) in result.iteritems()))

    def __scan_done(self):
        self.__update_idle_stat()
        if self.on_update:
            self.on_update()

    def __scan_iter(self):
        start_time = self.__time()
        start_pfn = self.__scan_pfn
        end_pfn = min(self.__scan_pfn + self.SCAN_CHUNK, sysinfo.END_PFN)
        # count idle pages
        cur = kpageutil.count_idle_pages(start_pfn, end_pfn)
        # accumulate the result
        Z = (0, 0)
        tot = self.__nr_unused
        for k in set(cur.keys() + tot.keys()):
            tot[k] = map(sum, zip(tot.get(k, Z), cur.get(k, Z)))
        # mark the scanned pages as idle for the next iteration
        kpageutil.set_idle_pages(start_pfn, end_pfn)
        # advance the pos and accumulate the time spent
        self.__scan_pfn = end_pfn
        self.__scan_time += self.__time() - start_time

    def __throttle(self):
        if self.__scan_time == 0:
            return
        pages_left = sysinfo.END_PFN - self.__scan_pfn
        time_left = self.interval - (self.__time() - self.__scan_start)
        time_required = pages_left * self.__scan_time / self.__scan_pfn
        if time_required > time_left:
            # only warn about significant lags (> 0.1% of interval)
            if not self.__warned and \
                    time_required - time_left > self.interval / 1000.0:
                logger.warning("Memory scanner is lagging behind "
                               "(%s s left, %s s required)" %
                               (time_left, time_required))
                self.__warned = True
            return
        chunks_left = float(pages_left) / self.SCAN_CHUNK
        self.__sleep((time_left - time_required) / chunks_left
                     if pages_left > 0 else time_left)

    def __scan(self):
        self.__scan_iter()
        self.__throttle()
        if self.__scan_pfn >= sysinfo.END_PFN:
            self.__scan_done()
            self.__init_scan()

    ##
    # Scan memory periodically counting unused pages until shutdown.

    def serve_forever(self):
        self.__is_shut_down.clear()
        try:
            self.__init_scan()
            while not self.__should_shut_down.is_set():
                self.__scan()
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

    def get_idle_stat(self, cg):
        return self.__idle_stat.get(cg, (0, 0))


def start_background_scan(interval, on_update=None):
    if not AVAILABLE:
        logger.error("Failed to activate idle memory estimator: "
                     "Not supported by the kernel")
        return
    scanner = _Scanner()
    scanner.interval = interval
    scanner.on_update = on_update
    threading.Thread(target=scanner.serve_forever).start()


def stop_background_scan():
    _Scanner().shutdown()


def get_idle_stat(cg):
    return _Scanner().get_idle_stat(cg)
