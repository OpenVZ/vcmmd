from __future__ import absolute_import

from multiprocessing.pool import ThreadPool

from vcmmd.cgroup import MemoryCgroup, BlkIOCgroup
from vcmmd.ve.base import Error, VEImpl, register_ve_impl
from vcmmd.ve_type import VE_TYPE_CT
from vcmmd.util.limits import PAGE_SIZE, UINT64_MAX


# The thread pool is used in order not to block the main thread while
# performing costly operations, like memory.high adjustment.
#
# XXX: Note, using threads should not really hurt parallelism, because real
# work is done from system calls, with GIL released.
_thread_pool = ThreadPool(3)


def _lookup_cgroup(klass, name):
    # A container's cgroup is located either at the top level of the cgroup
    # hierarchy or under machine.slice

    cg = klass(name)
    if cg.exists():
        return cg

    cg = klass('/machine.slice/' + name)
    if cg.exists():
        return cg

    raise Error("cgroup not found: '%s'" % cg.abs_path)


class CTImpl(VEImpl):

    VE_TYPE = VE_TYPE_CT

    def __init__(self, name):
        self._memcg = _lookup_cgroup(MemoryCgroup, name)
        self._blkcg = _lookup_cgroup(BlkIOCgroup, name)

        self.mem_limit = UINT64_MAX

    def get_stats(self):
        try:
            current = self._memcg.read_mem_current()
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
                'swapin': stat.get('pswpin', -1),
                'swapout': stat.get('pswpout', -1),
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
        # Decreasing memory.high might take long as it implies memory reclaim,
        # so do it asynchronously.
        #
        # XXX: For the sake of simplicity, we don't care about failures here.
        # It is acceptable, because adjusting memory.high may fail only if
        # the cgroup gets destroyed, which we will see and report anyway from
        # get_stats().
        _thread_pool.apply_async(self._memcg.write_mem_high, (value,))

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
