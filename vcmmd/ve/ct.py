from __future__ import absolute_import

from vcmmd.cgroup import MemoryCgroup, BlkIOCgroup
from vcmmd.ve import VE, Error, types as ve_types, MemStats, IOStats


class CgroupError(Error):
    pass


class CT(VE):

    VE_TYPE = ve_types.CT

    def activate(self):
        # Currently, containers' cgroups are located at the first level of the
        # cgroup hierarchy.
        self._memcg = MemoryCgroup(self.name)
        self._blkcg = BlkIOCgroup(self.name)

        if not self._memcg.exists():
            raise CgroupError('CT memory cgroup does not exist')

        if not self._blkcg.exists():
            raise CgroupError('CT blkio cgroup does not exist')

        super(CT, self).activate()

    def idle_ratio(self, age=0):
        # Strictly speaking, this is incorrect, because we do not count pages
        # used for storing kernel data here, but it'll do for an estimate.
        return self._memcg.get_idle_mem_portion(age)

    def _fetch_mem_stats(self):
        try:
            current = self._memcg.read_mem_current()
            stat = self._memcg.read_mem_stat()
        except IOError as err:
            raise CgroupError(err)

        return MemStats(rss=current,
                        minflt=stat.get('pgfault', -1),
                        majflt=stat.get('pgmajfault', -1))

    def _fetch_io_stats(self):
        try:
            serviced = self._blkcg.get_io_serviced()
            service_bytes = self._blkcg.get_io_service_bytes()
        except IOError as err:
            raise CgroupError(err)

        return IOStats(rd_req=serviced[0],
                       rd_bytes=service_bytes[0],
                       wr_req=serviced[1],
                       wr_bytes=service_bytes[1])

    def _set_mem_target(self, value):
        try:
            # Set memory target by adjusting memory.low. If the host is
            # experiencing memory pressure, containers exceeding the low
            # threshold are reclaimed from first, but if there is enough
            # free memory it may be breached freely.
            self._memcg.write_mem_low(value)
        except IOError as err:
            raise CgroupError(err)

    def _set_mem_max(self, value):
        try:
            self._memcg.write_mem_max(value)
            self._memcg.write_tcp_mem_limit(value / 8)
            self._memcg.write_udp_mem_limit(value / 8)
        except IOError as err:
            raise CgroupError(err)

    def _set_swap_max(self, value):
        try:
            self._memcg.write_swap_max(value)
        except IOError as err:
            raise CgroupError(err)
