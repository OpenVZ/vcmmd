# Copyright (c) 2016 Parallels IP Holdings GmbH
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
# Our contact details: Parallels IP Holdings GmbH, Vordergasse 59, 8200
# Schaffhausen, Switzerland.

from __future__ import absolute_import

import re
import time
import threading

from vcmmd.cgroup import idlememscan
from vcmmd.cgroup.base import Cgroup
from vcmmd.util.limits import INT64_MAX
from vcmmd.util.singleton import Singleton
from vcmmd.util.limits import PAGE_SIZE


class _IdleMemScanner:

    __metaclass__ = Singleton

    class _StopScan(Exception):
        pass

    def __init__(self):
        self.__result = {}
        self.__sampling = 1
        self.__period = 0
        self.__scan_in_progress = False
        self.__lock = threading.Lock()

    def __do_scan_iter(self):
        if self.__restart_scan:
            with self.__lock:
                period = self.__period
                if period <= 0:
                    self.__scan_in_progress = False
                    raise self._StopScan
            idlememscan.set_sampling(self.__sampling)
            self.__scan_took = 0.0
            self.__time_left = float(period)
            self.__restart_scan = False

        start = time.time()
        iters_done, iters_left = idlememscan.iter()
        spent = time.time() - start

        self.__scan_took += spent
        self.__time_left -= spent

        scan_will_take = iters_left * self.__scan_took / iters_done
        if scan_will_take < self.__time_left:
            timeout = ((self.__time_left - scan_will_take) / iters_left
                       if iters_left > 0 else self.__time_left)
            time.sleep(timeout)
            self.__time_left -= timeout

        if iters_left == 0:
            self.__result = idlememscan.result()
            self.__restart_scan = True

    def __scan_fn(self):
        self.__restart_scan = True
        try:
            while True:
                self.__do_scan_iter()
        except self._StopScan:
            pass

    def set_sampling(self, sampling):
        if not isinstance(sampling, float):
            raise TypeError("'sampling' must be a float")
        if not 0.0 < sampling <= 1.0:
            raise ValueError("'sampling' must be in range (0.0, 1.0]")

        self.__sampling = sampling

    def set_period(self, period):
        if not isinstance(period, (int, long)):
            raise TypeError("'period' must be an integer")
        if period < 0:
            raise ValueError("'period' must be >= 0")

        start_scan = False
        with self.__lock:
            self.__period = period
            if period > 0 and not self.__scan_in_progress:
                self.__scan_in_progress = True
                start_scan = True
        if start_scan:
            t = threading.Thread(target=self.__scan_fn)
            t.daemon = True
            t.start()

    @property
    def result(self):
        return self.__result


class MemoryCgroup(Cgroup):

    CONTROLLER = 'memory'

    MAX_MEM_VAL = INT64_MAX

    def _write_file_mem_val(self, filename, value):
        value = min(value, self.MAX_MEM_VAL)
        self._write_file_int(filename, value)

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

    def read_mem_stat(self):
        return self._read_file_kv('stat')

    def write_tcp_mem_limit(self, val):
        self._write_file_mem_val('kmem.tcp.limit_in_bytes', val)

    def write_udp_mem_limit(self, val):
        self._write_file_mem_val('kmem.udp.limit_in_bytes', val)

    def write_oom_guarantee(self, val):
        self._write_file_mem_val('oom_guarantee', val)

    @staticmethod
    def set_idle_mem_period(period):
        '''Set idle memory scan period, in seconds.

        If enabled, idle memory scanner will periodically scan physical memory
        range and count pages that have not been touched since the previous
        scan. The result can be obtained with 'get_idle_factor'.

        If 'period' is 0, the scanner will be stopped.

        Note, the change will only take place after the current period
        completes.
        '''
        _IdleMemScanner().set_period(period)

    @staticmethod
    def set_idle_mem_sampling(sampling):
        '''Set idle memory sampling.

        Set the portion of memory to check while performing idle scan.

        Note, the change will only take place after the current period
        completes.
        '''
        _IdleMemScanner().set_sampling(sampling)

    def _get_idle_factor(self, mem_types):
        try:
            stat = _IdleMemScanner().result[self.path]
        except KeyError:
            # No stats yet? Assume all memory is active.
            return 0.

        total = sum(stat[i * 2] for i in mem_types)
        idle = sum(stat[i * 2 + 1] for i in mem_types)

        # avoid div/0
        return float(idle) / (total + 1)

    def get_idle_factor(self):
        '''Return the percentage of ageable memory that was not touched during
        the last scan.
        '''
        return self._get_idle_factor([0, 1])

    def get_idle_factor_anon(self):
        '''Return the percentage of anonymous memory that was not touched
        during the last scan.
        '''
        return self._get_idle_factor([0])

    def get_idle_factor_file(self):
        '''Return the percentage of memory used for storing file caches that
        was not touched during the last scan.
        '''
        return self._get_idle_factor([1])

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
        self._write_file_str('numa_migrate', ",".join(map(lambda x: str(x),nodes)))
