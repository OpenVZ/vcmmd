from __future__ import absolute_import

import os

from vcmmd.cgroup import MemoryCgroup, BlkIOCgroup, BeancounterCgroup
from vcmmd.ve.base import Error, VEImpl, register_ve_impl
from vcmmd.ve.types import VE_TYPE_CT
from vcmmd.config import VCMMDConfig


_PAGE_SIZE = os.sysconf('SC_PAGE_SIZE')


class CTImpl(VEImpl):

    VE_TYPE = VE_TYPE_CT
    VE_TYPE_NAME = 'CT'

    def __init__(self, name):
        # Currently, containers' cgroups are located at the first level of the
        # cgroup hierarchy.
        self._memcg = MemoryCgroup(name)
        self._blkcg = BlkIOCgroup(name)
        self._bccg = BeancounterCgroup(name)

        if not self._memcg.exists():
            raise Error('CT memory cgroup does not exist')

        if not self._blkcg.exists():
            raise Error('CT blkio cgroup does not exist')

        if not self._bccg.exists():
            raise Error('CT beancounter cgroup does not exist')

    def get_mem_overhead(self):
        return 0  # containers do not have memory overhead

    def get_mem_stats(self):
        try:
            current = self._memcg.read_mem_current()
            high = self._memcg.read_mem_high()
            committed = self._bccg.get_privvmpages() * _PAGE_SIZE
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
                'committed': committed,
                'minflt': stat.get('pgfault', -1),
                'majflt': stat.get('pgmajfault', -1)}

    def get_io_stats(self):
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
        try:
            self._memcg.write_mem_high(value)
        except IOError as err:
            raise Error(err)

    def set_config(self, config):
        try:
            self._memcg.write_oom_guarantee(config.guarantee)
            self._memcg.write_mem_max(config.limit)
            self._memcg.write_tcp_mem_limit(config.limit / 8)
            self._memcg.write_udp_mem_limit(config.limit / 8)
            self._memcg.write_swap_max(config.swap)
        except IOError as err:
            raise Error(err)

register_ve_impl(CTImpl)
