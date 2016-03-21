from __future__ import absolute_import

from vcmmd.cgroup import MemoryCgroup, BlkIOCgroup
from vcmmd.ve import VE, Error, types as ve_types
from vcmmd.config import VCMMDConfig


class CT(VE):

    VE_TYPE = ve_types.CT

    def activate(self):
        # Currently, containers' cgroups are located at the first level of the
        # cgroup hierarchy.
        self._memcg = MemoryCgroup(self.name)
        self._blkcg = BlkIOCgroup(self.name)

        if not self._memcg.exists():
            raise Error('CT memory cgroup does not exist')

        if not self._blkcg.exists():
            raise Error('CT blkio cgroup does not exist')

        super(CT, self).activate()

    def idle_ratio(self, age=0):
        # Strictly speaking, this is incorrect, because we do not count pages
        # used for storing kernel data here, but it'll do for an estimate.
        return self._memcg.get_idle_mem_portion(age)

    def _fetch_mem_stats(self):
        try:
            current = self._memcg.read_mem_current()
            high = self._memcg.read_mem_high()
            stat = self._memcg.read_mem_stat()
        except IOError as err:
            raise Error(err)

        memtotal = max(high, current)
        memfree = max(high - current, 0)
        memavail = memfree + stat['cache']

        return {'rss': current,
                'actual': memtotal,
                'memtotal': memtotal,
                'memfree': memfree,
                'memavail': memavail,
                'minflt': stat.get('pgfault', -1),
                'majflt': stat.get('pgmajfault', -1)}

    def _fetch_io_stats(self):
        try:
            serviced = self._blkcg.get_io_serviced()
            service_bytes = self._blkcg.get_io_service_bytes()
        except IOError as err:
            raise Error(err)

        return {'rd_req': serviced[0],
                'rd_bytes': service_bytes[0],
                'wr_req': serviced[1],
                'wr_bytes': service_bytes[1]}

    def set_mem_protection(self, value):
        # Use memcg/memory.low to protect the CT from host pressure.
        try:
            self._memcg.write_mem_low(value)
        except IOError as err:
            raise Error(err)

    def set_mem_target(self, value):
        # XXX: Should we adjust memcg/memory.high here?
        #
        # On one hand, not adjusting it might break policy assumptions. On the
        # other hand, for containers global and local reclaim paths are
        # equivalent, so it is tempting to avoid local reclaim here, which may
        # noticeably degrade performance in case policy underestimates
        # container's demand, and rely solely on memcg/memory.low set by
        # set_mem_protection, which is kinda soft limit and only matters when
        # there is real memory shortage on the host.
        #
        # For now, let's set memory.high unless we are explicitly asked not to
        # do so via config.
        if VCMMDConfig().get_bool('VE.CT.SoftMemTarget', False):
            value = MemoryCgroup.MAX_MEM_VAL
        self._memcg.write_mem_high(value)

    def _apply_config(self, config):
        try:
            self._memcg.write_oom_guarantee(config.guarantee)
            self._memcg.write_mem_max(config.limit)
            self._memcg.write_tcp_mem_limit(config.limit / 8)
            self._memcg.write_udp_mem_limit(config.limit / 8)
            self._memcg.write_swap_max(config.swap)
        except IOError as err:
            raise Error(err)
