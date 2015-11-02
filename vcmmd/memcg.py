import errno
import logging
import os.path

import config
from core import Error, LoadConfig, AbstractLoadEntity, AbstractLoadManager
import idlemem
import sysinfo
import util


class MemCg(AbstractLoadEntity):

    MAX_LIMIT = 9223372036854775807  # int64

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
        return os.path.join(config.MEMCG_MOUNT, self.id)

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

    def update(self):
        self.mem_usage = self.__read_mem_usage()

        stat = self.__read_stat()
        anon = stat['total_inactive_anon'] + stat['total_active_anon']
        file = stat['total_inactive_file'] + stat['total_active_file']

        idle_stat = idlemem.get_idle_stat(self.id)
        idle_anon = (anon * idle_stat[idlemem.ANON][1] /
                     (idle_stat[idlemem.ANON][0] + 1))
        idle_file = (file * idle_stat[idlemem.FILE][1] /
                     (idle_stat[idlemem.FILE][0] + 1))

        # TODO: do not count anon if there is no swap
        total_idle = idle_anon + idle_file
        self.mem_unused = min(self.mem_usage, total_idle)

    def sync(self):
        self.__write_mem_low(self.mem_reservation)

    def reset(self):
        self.__write_mem_low(0)
        self.__write_mem_high(self.MAX_LIMIT)


class BaseMemCgManager(AbstractLoadManager):

    LoadEntityClass = MemCg

    # Do we take into account memory guarantees?
    SUPPORTS_GUARANTEES = False

    # Start unused memory estimator?
    TRACK_UNUSED_MEM = False

    def __init__(self, *args, **kwargs):
        AbstractLoadManager.__init__(self, *args, **kwargs)
        if config.MEM_IDLE_DELAY == 0:
            self.TRACK_UNUSED_MEM = False

        if not self.SUPPORTS_GUARANTEES:
            self.logger.warning("Memory guarantees are not supported by "
                                "the load manager and will be ignored")

    def serve_forever(self):
        if self.TRACK_UNUSED_MEM:
            idlemem.logger = self.logger
            idlemem.start_background_scan(config.MEM_IDLE_DELAY,
                                          self.update)
        AbstractLoadManager.serve_forever(self)

    def shutdown(self):
        idlemem.stop_background_scan()
        AbstractLoadManager.shutdown(self)

    # Minimal logic is implemented in MemCg.set_config.
    # No need to override _do_update.


class DefaultMemCgManager(BaseMemCgManager):

    TRACK_UNUSED_MEM = True

    def _estimate_wss(self, e):
        if e.mem_unused < e.mem_usage * config.MEM_IDLE_THRESH:
            # memcg does not seem to have much idle memory, so give it a chance
            # to increase its share
            wss = min(e.config.limit, sysinfo.MEM_TOTAL)
        else:
            wss = e.mem_usage - e.mem_unused
        return wss

    def _calc_reservation(self, entities):
        mem_avail = max(sysinfo.MEM_TOTAL - config.SYSTEM_MEM, 0)

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
