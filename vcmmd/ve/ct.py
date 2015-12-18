from vcmmd.cgroup import MemoryCgroup
from vcmmd.ve import VE, Error, types as ve_types


class CgroupError(Error):
    pass


class CT(VE):

    VE_TYPE = ve_types.CT
    VE_TYPE_NAME = 'CT'

    def __init__(self, name):
        super(CT, self).__init__(name)

        # Currently, containers' cgroups are located at the first level of the
        # cgroup hierarchy.
        self._memcg = MemoryCgroup(name)

    def set_mem_low(self, value):
        try:
            self._memcg.write_mem_low(value)
        except IOError as err:
            raise CgroupError(err)

    def set_mem_high(self, value):
        try:
            self._memcg.write_mem_high(value)
        except IOError as err:
            raise CgroupError(err)

    def set_mem_max(self, value):
        try:
            self._memcg.write_mem_max(value)
        except IOError as err:
            raise CgroupError(err)

    def set_swap_max(self, value):
        try:
            self._memcg.write_swap_max(value)
        except IOError as err:
            raise CgroupError(err)
