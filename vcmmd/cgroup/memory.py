from vcmmd.cgroup.base import Cgroup
from vcmmd.util import INT64_MAX


class MemoryCgroup(Cgroup):

    CONTROLLER = 'memory'

    _MEM_VAL_MAX = INT64_MAX

    def _write_file_mem_val(self, filename, value):
        value = min(value, self._MEM_VAL_MAX)
        self.write_file_int(filename, value)

    def read_mem_current(self):
        return self.read_file_int('memory.usage_in_bytes')

    def read_swap_current(self):
        mem = self.read_file_int('memory.usage_in_bytes')
        memsw = self.read_file_int('memory.memsw.usage_in_bytes')
        return max(memsw - mem, 0)

    def read_mem_low(self):
        return self.read_file_int('memory.low')

    def write_mem_low(self, val):
        self._write_file_mem_val('memory.low', val)

    def read_mem_high(self):
        return self.read_file_int('memory.high')

    def write_mem_high(self, val):
        self._write_file_mem_val('memory.high', val)

    def read_mem_max(self):
        return self.read_file_int('memory.limit_in_bytes')

    def write_mem_max(self, val):
        mem = self.read_file_int('memory.limit_in_bytes')
        memsw = self.read_file_int('memory.memsw.limit_in_bytes')
        swp = max(memsw - mem, 0)
        if val > mem:
            self._write_file_mem_val('memory.memsw.limit_in_bytes', val + swp)
            self._write_file_mem_val('memory.limit_in_bytes', val)
        else:
            self._write_file_mem_val('memory.limit_in_bytes', val)
            self._write_file_mem_val('memory.memsw.limit_in_bytes', val + swp)

    def read_swap_max(self):
        mem = self.read_file_int('memory.limit_in_bytes')
        memsw = self.read_file_int('memory.memsw.limit_in_bytes')
        return max(memsw - mem, 0)

    def write_swap_max(self, val):
        mem = self.read_file_int('memory.limit_in_bytes')
        self._write_file_mem_val('memory.memsw.limit_in_bytes', mem + val)
