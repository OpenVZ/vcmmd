import errno
import logging
import os
import os.path
import stat
import threading
import time

import config
from core import Error, LoadConfig, AbstractLoadEntity, AbstractLoadManager
import kpageutil
import sysinfo
import util


class MemCg(AbstractLoadEntity):

    MAX_LIMIT = 9223372036854775807  # int64

    # Estimate of the amount of unused memory per cgroup.
    # Stored as a dictionary: id -> (unused_anon, unused_file)
    # Updated by UnusedMemEstimator.
    unused_mem_estimate = {}

    def __init__(self, id):
        AbstractLoadEntity.__init__(self, id)

        # exclude the root to avoid confusion
        if not self.id:
            raise Error(errno.EINVAL, "Invalid ID")
        # explicitly exclude nested cgroups, because we do not support them
        if "/" in self.id:
            raise Error(errno.EINVAL, "Nested entities are not supported")
        # check that the cgroup exists
        if not os.path.exists(self.__path()):
            raise Error(errno.ENOENT, "Entity does not exist")

        self.mem_usage = 0
        self.mem_unused = 0
        self.mem_reservation = 0

    def __path(self):
        return os.path.join(config.MEMCG__ROOT_PATH, self.id)

    def __read(self, name):
        filepath = os.path.join(self.__path(), name)
        try:
            with open(filepath, 'r') as f:
                ret = f.read()
        except IOError as err:
            raise Error(errno.EIO, "Failed to read %s: %s" % (name, err))
        return ret

    def __read_int(self, name):
        try:
            return int(self.__read(name))
        except ValueError as err:
            raise Error(errno.EIO, "Failed to parse %s: %s" % (name, err))

    def __write(self, name, val):
        filepath = os.path.join(self.__path(), name)
        try:
            with open(filepath, 'w') as f:
                f.write(str(val))
        except IOError as err:
            raise Error(errno.EIO, "Failed to write %s: %s" % (name, err))

    def __write_limit(self, name, val):
        val = min(val, self.MAX_LIMIT)
        self.__write(name, val)

    def __read_mem_usage(self):
        return self.__read_int('memory.usage_in_bytes')

    def __write_mem_low(self, val):
        self.__write_limit('memory.low', val)

    def __write_mem_high(self, val):
        self.__write_limit('memory.high', val)

    def __write_mem_limit(self, val):
        self.__write_limit('memory.limit_in_bytes', val)

    def __read_memsw_limit(self):
        return self.__read_int('memory.memsw.limit_in_bytes')

    def __write_memsw_limit(self, val):
        self.__write_limit('memory.memsw.limit_in_bytes', val)

    def __write_tcp_limit(self, val):
        self.__write_limit('memory.kmem.tcp.limit_in_bytes', val)

    def __write_udp_limit(self, val):
        self.__write_limit('memory.kmem.udp.limit_in_bytes', val)

    def __do_set_config(self, cfg):
        memsw_limit = cfg.limit + cfg.swap_limit
        cur_memsw_limit = self.__read_memsw_limit()

        # Be careful: memsw.limit must always be >= mem.limit
        if memsw_limit > cur_memsw_limit:
            self.__write_memsw_limit(memsw_limit)
            self.__write_mem_limit(cfg.limit)
        else:
            self.__write_mem_limit(cfg.limit)
            self.__write_memsw_limit(memsw_limit)

        if cfg.limit < cfg.MAX_LIMIT:
            high = int(cfg.limit * config.MEMCG__HIGH)
            high = min(high, config.MEMCG__HIGH_MAX)
            high = max(cfg.limit - high, 0)
        else:
            high = cfg.MAX_LIMIT
        self.__write_mem_high(high)

        if cfg.limit < cfg.MAX_LIMIT:
            skb_limit = cfg.limit / 8
        else:
            skb_limit = cfg.MAX_LIMIT
        self.__write_tcp_limit(skb_limit)
        self.__write_udp_limit(skb_limit)

    def set_config(self, cfg):
        try:
            self.__do_set_config(cfg)
        except Error as err:
            # XXX: If we fail to revert, memcg config will be inconsistent
            self.__do_set_config(self.config)
            raise
        self.config = cfg

    def update(self):
        old_mem_usage = self.mem_usage
        old_mem_unused = self.mem_unused
        self.mem_usage = self.mem_unused = 0

        self.mem_usage = self.__read_mem_usage()

        unused_mem_estimate = self.unused_mem_estimate
        if self.id in unused_mem_estimate:
            # An estimate was recently updated, take it.
            # TODO: do not count anon if there is no swap
            self.mem_unused = min(self.mem_usage,
                                  sum(unused_mem_estimate[self.id]))
            del unused_mem_estimate[self.id]
        else:
            self.mem_unused = old_mem_unused
            # We assume that usage decrease is due to unused mem reclaim.
            if self.mem_usage < old_mem_usage:
                self.mem_unused -= old_mem_usage - self.mem_usage
                self.mem_unused = max(self.mem_unused, 0)

    def sync(self):
        self.__write_mem_low(self.mem_reservation)

    def reset(self):
        self.__write_mem_low(0)
        self.__write_mem_high(self.MAX_LIMIT)


class UnusedMemEstimator:

    SCAN_CHUNK = 32768

    ##
    # interval: interval between updates, in seconds
    # on_update: callback to run on each update
    #
    # To avoid CPU bursts, the estimator will distribute the scanning in time
    # so that a full scan fits in the given interval.

    def __init__(self, interval, on_update=None, logger=None):
        self.interval = interval
        self.on_update = on_update
        self.logger = logger or logging.getLogger(__name__)
        self.__is_shut_down = threading.Event()
        self.__should_shut_down = threading.Event()
        kpageutil.init(sysinfo.END_PFN)

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
    def __update_memcg_unused(self):
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
        MemCg.unused_mem_estimate = result
        self.logger.debug("Unused memory estimate (anon/file)): %s" %
                          "; ".join('%s: %s/%s' % (k,
                                                   util.strmemsize(v1),
                                                   util.strmemsize(v2))
                                    for k, (v1, v2) in result.iteritems()))

    def __scan_done(self):
        self.__update_memcg_unused()
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
                self.logger.warning("Memory scanner is lagging behind "
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
    # Check if the idle memory tracking feature is supported by the kernel.
    # Return true if it is, false otherwise.

    @staticmethod
    def is_available():
        return os.path.exists("/sys/kernel/mm/page_idle/bitmap")

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
        self.__should_shut_down.set()
        self.__is_shut_down.wait()


class BaseMemCgManager(AbstractLoadManager):

    LoadEntityClass = MemCg

    # Do we take into account memory guarantees?
    SUPPORTS_GUARANTEES = False

    # Start unused memory estimator?
    TRACK_UNUSED_MEM = False

    def __init__(self, *args, **kwargs):
        AbstractLoadManager.__init__(self, *args, **kwargs)

        if not self.SUPPORTS_GUARANTEES:
            self.logger.warning("Memory guarantees are not supported by "
                                "the load manager and will be ignored")

        self.unused_mem_estimator = None
        if self.TRACK_UNUSED_MEM:
            if UnusedMemEstimator.is_available():
                if config.MEMCG__MEM_INUSE_TIME != 0:
                    self.unused_mem_estimator = UnusedMemEstimator(
                        config.MEMCG__MEM_INUSE_TIME, self.update, self.logger)
            else:
                self.logger.error("Failed to activate idle memory estimator: "
                                  "Not supported by the kernel")

    def serve_forever(self):
        if self.unused_mem_estimator:
            threading.Thread(target=self.unused_mem_estimator.
                             serve_forever).start()
        AbstractLoadManager.serve_forever(self)

    def shutdown(self):
        if self.unused_mem_estimator:
            self.unused_mem_estimator.shutdown()
        AbstractLoadManager.shutdown(self)

    # Minimal logic is implemented in MemCg.set_config.
    # No need to override _do_update.


class DefaultMemCgManager(BaseMemCgManager):

    TRACK_UNUSED_MEM = True

    def _estimate_wss(self, e):
        if e.mem_unused < e.mem_usage * config.MEMCG__MIN_UNUSED_MEM:
            # memcg does not seem to have much idle memory, so give it a chance
            # to increase its share
            wss = min(e.config.limit, sysinfo.MEM_TOTAL)
        else:
            wss = e.mem_usage - e.mem_unused
        return wss

    def _calc_reservation(self, entities):
        mem_avail = max(sysinfo.MEM_TOTAL - config.CORE__SYSTEM_MEM, 0)
        mem_avail = int(config.CORE__MAX_RESERVATION * mem_avail)

        # Reservations are calculated by dividing the available memory among
        # entities proportionally to the memory demand.

        demand = {e: self._estimate_wss(e) for e in entities}

        demand_scale = min(float(mem_avail) / (sum(demand.values()) + 1), 1.0)

        for e in entities:
            e.mem_reservation = int(demand[e] * demand_scale)

    def _do_update(self, entities):
        BaseMemCgManager._do_update(self, entities)

        self._calc_reservation(entities)

        self.logger.debug("Memory consumption/unused/reservation: %s" %
                          "; ".join('%s: %s/%s/%s' %
                                    (e.id,
                                     util.strmemsize(e.mem_usage),
                                     util.strmemsize(e.mem_unused),
                                     util.strmemsize(e.mem_reservation))
                                    for e in entities))
