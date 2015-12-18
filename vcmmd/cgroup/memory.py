from vcmmd.cgroup.base import Cgroup


class MemoryCgroup(Cgroup):

    CONTROLLER = 'memory'

    def read_mem_current(self):
        return self.read_file_int('memory.usage_in_bytes')

    def read_swap_current(self):
        mem = self.read_file_int('memory.usage_in_bytes')
        memsw = self.read_file_int('memory.memsw.usage_in_bytes')
        return max(memsw - mem, 0)

    def read_mem_low(self):
        return self.read_file_int('memory.low')

    def write_mem_low(self, val):
        self.write_file_int('memory.low', val)

    def read_mem_high(self):
        return self.read_file_int('memory.high')

    def write_mem_high(self, val):
        self.write_file_int('memory.high', val)

    def read_mem_max(self):
        return self.read_file_int('memory.limit_in_bytes')

    def write_mem_max(self, val):
        mem = self.read_file_int('memory.limit_in_bytes')
        memsw = self.read_file_int('memory.memsw.limit_in_bytes')
        swp = max(memsw - mem, 0)
        if val > mem:
            self.write_file_int('memory.memsw.limit_in_bytes', val + swp)
            self.write_file_int('memory.limit_in_bytes', val)
        else:
            self.write_file_int('memory.limit_in_bytes', val)
            self.write_file_int('memory.memsw.limit_in_bytes', val + swp)

    def read_swap_max(self):
        mem = self.read_file_int('memory.limit_in_bytes')
        memsw = self.read_file_int('memory.memsw.limit_in_bytes')
        return max(memsw - mem, 0)

    def write_swap_max(self, val):
        mem = self.read_file_int('memory.limit_in_bytes')
        self.write_file_int('memory.memsw.limit_in_bytes', mem + val)
