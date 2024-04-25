# Copyright (c) 2016-2017, Parallels International GmbH
# Copyright (c) 2017-2024, Virtuozzo International GmbH, All rights reserved
#
# This file is part of OpenVZ. OpenVZ is free software; you can redistribute
# it and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the License,
# or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#
# Our contact details: Virtuozzo International GmbH, Vordergasse 59, 8200
# Schaffhausen, Switzerland.

import re

from vcmmd.cgroup.base import Cgroup
from vcmmd.util.limits import INT64_MAX
from vcmmd.util.limits import PAGE_SIZE


class MemoryCgroup(Cgroup):

    CONTROLLER = 'memory'

    MAX_MEM_VAL = INT64_MAX

    def _write_file_mem_val(self, filename, value):
        if isinstance(value, int):
            value = str(min(value, self.MAX_MEM_VAL))
        self._write_file_str(filename, value)

    def read_mem_current(self):
        return self._read_file_int('usage_in_bytes')

    def read_swap_current(self):
        mem = self._read_file_int('usage_in_bytes')
        memsw = self._read_file_int('memsw.usage_in_bytes')
        return max(memsw - mem, 0)

    def read_mem_low(self):
        return self._read_file_int('low')

    def write_mem_low(self, val):
        self._write_file_mem_val('low', val)

    def read_mem_high(self):
        return self._read_file_int('high')

    def write_mem_high(self, val):
        self._write_file_mem_val('high', val)

    def write_mem_config(self, mem, sw):
        mem_old = self._read_file_int('limit_in_bytes')
        memsw = mem + sw
        if mem > mem_old:
            self.write_memsw_limit_in_bytes( memsw)
            self.write_limit_in_bytes(mem)
        else:
            self.write_limit_in_bytes(mem)
            self.write_memsw_limit_in_bytes( memsw)

    def read_mem_max(self):
        return self._read_file_int('limit_in_bytes')

    def write_limit_in_bytes(self, val):
        # Warning: also changes swap size to memsw.limit_in_bytes - val
        self._write_file_mem_val('limit_in_bytes', val)

    def read_swap_max(self):
        mem = self._read_file_int('limit_in_bytes')
        memsw = self._read_file_int('memsw.limit_in_bytes')
        return max(memsw - mem, 0)

    def write_memsw_limit_in_bytes(self, val):
        # Warning: changes swap size to val - limit_in_bytes
        self._write_file_mem_val('memsw.limit_in_bytes', val)

    def write_cache_limit_in_bytes(self, val):
        self._write_file_mem_val('cache.limit_in_bytes', val)

    def read_mem_stat(self):
        return self._read_file_kv('stat')

    def write_oom_guarantee(self, val):
        self._write_file_int('oom_guarantee', val)

    def write_swappiness(self, val):
        self._write_file_int('swappiness', val)

    def read_swappiness(self):
        return self._read_file_int('swappiness')

    def write_oom_control(self, val):
        self._write_file_int('oom_control', val)

    def write_cleancache(self, val):
        self._write_file_int('disable_cleancache', int(not val))

    def read_cache_limit_in_bytes(self):
        return self._read_file_int('cache.limit_in_bytes')

    def get_numa_stats(self):
        stats = self._read_file_str("numa_stat")
        res = {}
        for line in stats.split("\n"):
            if not line:
                continue
            name, data = re.split(" ", line, maxsplit = 1)
            name = "mem" + name.split("=")[0]
            for node, value in re.findall("N(\d+)=(\d+)", data):
                node, value = int(node), int(value)
                if node not in res:
                    res[node] = {}
                res[node][name] = value * PAGE_SIZE
        return res

    def set_node_list(self, nodes):
        self._write_file_str('numa_migrate', ','.join(map(str, nodes)))
