from __future__ import absolute_import

from vcmmd.cgroup import MemoryCgroup
from vcmmd.ve import VE, Error, types as ve_types, MemStats


class CgroupError(Error):
    pass


class CT(VE):

    VE_TYPE = ve_types.CT
    VE_TYPE_NAME = 'CT'

    def activate(self):
        # Currently, containers' cgroups are located at the first level of the
        # cgroup hierarchy.
        self._memcg = MemoryCgroup(self.name)

        if not self._memcg.exists():
            raise CgroupError('CT memory cgroup does not exist')

        super(CT, self).activate()

    def _fetch_mem_stats(self):
        try:
            current = self._memcg.read_mem_current()
            high = self._memcg.read_mem_high()
            stat = self._memcg.read_mem_stat()
        except (IOError, ValueError) as err:
            raise CgroupError(err)

        # Since a container releases memory to the host immediately, 'rss'
        # always equals 'used'
        return MemStats(actual=max(current, high),
                        rss=current,
                        used=current,
                        minflt=stat.get('pgfault', 0),
                        majflt=stat.get('pgmajfault', 0))

    def _set_mem_low(self, value):
        try:
            self._memcg.write_mem_low(value)
        except IOError as err:
            raise CgroupError(err)

    def _set_mem_high(self, value):
        try:
            self._memcg.write_mem_high(value)
        except IOError as err:
            raise CgroupError(err)

    def _set_mem_max(self, value):
        try:
            self._memcg.write_mem_max(value)
        except IOError as err:
            raise CgroupError(err)

    def _set_swap_max(self, value):
        try:
            self._memcg.write_swap_max(value)
        except IOError as err:
            raise CgroupError(err)
