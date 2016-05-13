from __future__ import absolute_import

from vcmmd.cgroup import MemoryCgroup, BlkIOCgroup, BeancounterCgroup
from vcmmd.ve.base import Error, VEImpl, register_ve_impl
from vcmmd.ve_type import VE_TYPE_CT
from vcmmd.config import VCMMDConfig
from vcmmd.util.limits import PAGE_SIZE, UINT64_MAX


class CTImpl(VEImpl):

    VE_TYPE = VE_TYPE_CT

    def __init__(self, name):
        # Currently, containers' cgroups are located at the first level of the
        # cgroup hierarchy.

        self._memcg = MemoryCgroup(name)
        if not self._memcg.exists():
            raise Error("Memory cgroup not found: '%s'" % self._memcg.abs_path)

        self._blkcg = BlkIOCgroup(name)
        if not self._blkcg.exists():
            raise Error("Blkio cgroup not found: '%s'" % self._blkcg.abs_path)

        self._bccg = BeancounterCgroup(name)
        if not self._bccg.exists():
            raise Error("Beancounter cgroup not found: '%s'" % self._bccg.abs_path)

        self.mem_limit = UINT64_MAX

    def get_stats(self):
        try:
            current = self._memcg.read_mem_current()
            committed = self._bccg.get_privvmpages() * PAGE_SIZE
            stat = self._memcg.read_mem_stat()
            io_serviced = self._blkcg.get_io_serviced()
            io_service_bytes = self._blkcg.get_io_service_bytes()
        except IOError as err:
            raise Error('Cgroup read failed: %s' % err)

        memtotal = max(self.mem_limit, current)
        memfree = memtotal - current
        memavail = (memfree +
                    stat.get('active_file', 0) +
                    stat.get('inactive_file', 0) +
                    stat.get('slab_reclaimable', 0))

        return {'rss': current,
                'host_mem': current,
                'host_swap': stat.get('swap', -1),
                'actual': memtotal,
                'memfree': memfree,
                'memavail': memavail,
                'committed': committed,
                'minflt': stat.get('pgfault', -1),
                'majflt': stat.get('pgmajfault', -1),
                'rd_req': io_serviced[0],
                'rd_bytes': io_service_bytes[0],
                'wr_req': io_serviced[1],
                'wr_bytes': io_service_bytes[1]}

    def set_mem_protection(self, value):
        # Use memcg/memory.low to protect the CT from host pressure.
        try:
            self._memcg.write_mem_low(value)
        except IOError as err:
            raise Error('Cgroup write failed: %s' % err)

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
        try:
            self._memcg.write_mem_high(value)
        except IOError as err:
            raise Error('Cgroup write failed: %s' % err)

        self.mem_limit = value

    def set_config(self, config):
        try:
            self._memcg.write_oom_guarantee(config.guarantee)
            self._memcg.write_mem_max(config.limit)
            self._memcg.write_tcp_mem_limit(config.limit / 8)
            self._memcg.write_udp_mem_limit(config.limit / 8)
            self._memcg.write_swap_max(config.swap)
        except IOError as err:
            raise Error('Cgroup write failed: %s' % err)

        self.mem_limit = min(self.mem_limit, config.limit)

register_ve_impl(CTImpl)
