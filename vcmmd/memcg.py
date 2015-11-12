import errno
import logging
import numpy as np
import os.path

import config
from core import Error, LoadConfig, AbstractLoadEntity, AbstractLoadManager
import idlemem
from idlemem import ANON, FILE, NR_MEM_TYPES, MAX_AGE
import sysinfo
from util import INT64_MAX, divroundup, clamp, strmemsize


class MemCg(AbstractLoadEntity):

    MAX_LIMIT = INT64_MAX

    def __init__(self, id):
        AbstractLoadEntity.__init__(self, id)

        # exclude the root to avoid confusion
        if self.id == os.path.sep:
            raise Error(errno.EINVAL, "Invalid ID")

        self.__path = os.path.join(sysinfo.MEMCG_MOUNT,
                                   self.id.lstrip(os.path.sep))

        # check that the cgroup exists
        if not os.path.exists(self.__path):
            raise Error(errno.ENOENT, "Entity does not exist")

    def __read(self, name):
        filepath = os.path.join(self.__path, name)
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
        filepath = os.path.join(self.__path, name)
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

    def __read_stat(self):
        stat = {}
        for l in self.__read('memory.stat').split('\n'):
            try:
                k, v = l.split(' ')
                v = int(v)
            except ValueError:
                continue
            stat[k] = v
        return stat

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
            high = int(cfg.limit * config.HIGH_WMARK_RATIO)
            high = min(high, config.HIGH_WMARK_MAX)
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
        self.__reset_wss_hist()

    def __reset_wss_hist(self):
        self.wss_hist = np.empty(MAX_AGE, dtype=np.int64)
        self.wss_hist.fill(min(self.config.limit, INT64_MAX))

    def __update_wss_hist(self):
        # Update idle stats
        idle_stat_raw = idlemem.last_idle_stat.pop(self.id, None)
        if not idle_stat_raw:
            return

        # Normalize idle stats
        stat = self.__read_stat()
        total = {
            ANON: stat['total_inactive_anon'] + stat['total_active_anon'],
            FILE: stat['total_inactive_file'] + stat['total_active_file'],
        }
        scale = {
            t: total[t] / (idle_stat_raw[t][0] + 1.0)
            for t in xrange(NR_MEM_TYPES)
        }
        idle_stat = {}
        for t in xrange(NR_MEM_TYPES):
            idle_stat[t] = np.empty(MAX_AGE, dtype=np.int64)
            idle_stat[t][:] = idle_stat_raw[t][1:] * scale[t]

        # Shift idle stat arrays according to config parameters
        idle_age = {
            ANON: config.ANON_IDLE_AGE,
            FILE: config.FILE_IDLE_AGE,
        }
        idle_shift = {
            t: clamp(divroundup(idle_age[t], config.MEM_IDLE_DELAY) - 1,
                     0, MAX_AGE)
            for t in xrange(NR_MEM_TYPES)
        }
        for t in xrange(NR_MEM_TYPES):
            shift = idle_shift[t]
            if shift > 0:
                a = idle_stat[t]
                a[:-shift] = a[shift:]
                a[-shift:] = a[-1]

        # Calculate total idle memory size
        # TODO: do not count anon if there is no swap
        idle_hist = idle_stat[ANON] + idle_stat[FILE]

        # Filter too large results
        np.clip(idle_hist, 0, self.mem_usage, out=idle_hist)

        # Update wss estimate
        #
        # If relative share of idle memory is below the threshold, assume the
        # wss to be maximal possible. This will give the memcg a chance to
        # increase its share.
        #
        idle_low = idle_hist < self.mem_usage * config.MEM_IDLE_THRESH
        wss_hist = (self.mem_usage - idle_hist) * ~idle_low
        wss_hist += idle_low * min(self.config.limit, INT64_MAX)

        self.wss_hist = wss_hist

    def update(self):
        self.mem_usage = self.__read_mem_usage()
        self.__update_wss_hist()

    def sync(self):
        self.__write_mem_low(self.mem_reservation)

    def reset(self):
        self.__write_mem_low(0)
        self.__write_mem_high(self.MAX_LIMIT)


class BaseMemCgManager(AbstractLoadManager):

    LoadEntityClass = MemCg

    # Do we take into account memory guarantees?
    SUPPORTS_GUARANTEES = False

    # Start idle memory estimator?
    TRACK_IDLE_MEM = False

    def serve_forever(self):
        if not self.SUPPORTS_GUARANTEES:
            self.logger.warning("Memory guarantees are not supported by "
                                "the load manager and will be ignored")

        if self.TRACK_IDLE_MEM and config.MEM_IDLE_DELAY > 0:
            idlemem.logger = self.logger
            idlemem.start_background_scan(config.MEM_IDLE_DELAY,
                                          config.MEM_IDLE_SAMPLING_RATIO,
                                          on_update=self.update)
        AbstractLoadManager.serve_forever(self)

    def shutdown(self):
        idlemem.stop_background_scan()
        AbstractLoadManager.shutdown(self)

    # Minimal logic is implemented in MemCg.set_config.
    # No need to override _do_update.


class DefaultMemCgManager(BaseMemCgManager):

    TRACK_IDLE_MEM = True

    def _do_update(self):
        BaseMemCgManager._do_update(self)

        mem_avail = max(sysinfo.MEM_TOTAL - config.SYSTEM_MEM, 0)
        age_max = clamp(config.MEM_STALE_AGE / config.MEM_IDLE_DELAY,
                        1, MAX_AGE)
        for age in xrange(age_max, 0, -1):
            sum_demand = sum(e.wss_hist[age - 1] for e in self._entity_iter())
            if sum_demand <= mem_avail:
                break

        overcommit_ratio = float(sum_demand) / (mem_avail + 1)
        for e in self._entity_iter():
            e.mem_reservation = int(e.wss_hist[age - 1] /
                                    max(overcommit_ratio, 1))

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("entities %d avail %s demand %s "
                              "overcommit %.2f age %ds" %
                              (sum(1 for e in self._entity_iter()),
                               strmemsize(mem_avail),
                               strmemsize(sum_demand),
                               overcommit_ratio, age * config.MEM_IDLE_DELAY))
            fmt = "%-38s : %6s %6s %6s : %6s %6s"
            hdr = True
            for e in self._entity_iter():
                if hdr:
                    self.logger.debug(fmt % ("id", "guar", "mem", "swp",
                                             "usage", "rsrv"))
                    hdr = False
                self.logger.debug(fmt %
                                  (e.id,
                                   LoadConfig.strmemsize(e.config.guarantee),
                                   LoadConfig.strmemsize(e.config.limit),
                                   LoadConfig.strmemsize(e.config.swap_limit),
                                   strmemsize(e.mem_usage),
                                   strmemsize(e.mem_reservation)))
