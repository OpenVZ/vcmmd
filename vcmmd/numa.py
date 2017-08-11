# Copyright (c) 2016-2017, Parallels International GmbH
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
# Our contact details: Parallels International GmbH, Vordergasse 59, 8200
# Schaffhausen, Switzerland.
import os

from vcmmd.env import Env
from vcmmd.util.stats import Stats
from vcmmd.util.misc import parse_range_list
from vcmmd.util.threading import update_stats_single


class NumaStats(object):


    class MemStats(Stats):
        ABSOLUTE_STATS = [
            'memtotal',
            'memusage',
            'memfree',
        ]


    class CpuStats(Stats):
        # TODO move to cumulative
        ABSOLUTE_STATS = [
            'cpuuser',
            'cpunice',
            'cpusystem',
            'cpuidle',
        ]

    def __init__(self, node_ids, cpu_list):
        self.memstats = {n: NumaStats.MemStats() for n in node_ids}
        self.cpustats = {n: {c: NumaStats.CpuStats() for c in cpu_list[n]} for n in node_ids}
        self.node_ids = node_ids

    def update_memstats(self, memstats):
        for n in self.node_ids:
            self.memstats[n]._update(**memstats.get(n, {}))

    def update_cpustats(self, cpustats):
        for n in self.node_ids:
            for c in self.cpustats[n]:
                self.cpustats[n][c]._update(**cpustats.get(c, {}))

    def report(self):
        ret = {}
        for n in self.cpustats:
            ret[n] = {'numa_memory': self.memstats[n].report()}
            for c in self.cpustats[n]:
                ret[n].update({'numa_cpus': self.cpustats[n][c].report()})
        return ret

    def __str__(self):
        return str(self.report())


class Numa(object):

    NUMA_NODE_SYS_PATH = "/sys/devices/system/node/node%d/"
    __inited = False

    def __init__(self, env):
        assert isinstance(env, Env)
        self.__env = env
        Numa.init()
        self.stats = NumaStats(self.nodes_ids, self.cpu_list)

    @classmethod
    def init(cls):
        if cls.__inited:
            return
        cls.nodes_ids = cls.get_nodes_ids()
        cls.cpu_list = {}
        for n in cls.nodes_ids[:]:
	    node_dir = cls.NUMA_NODE_SYS_PATH % n
            with open(node_dir + "cpulist") as f:
                cpu_list = parse_range_list(f.read())
                if not cpu_list:
                    cls.nodes_ids.remove(n)
                    continue
                cls.cpu_list[n] = cpu_list
        cls.__inited = True

    @staticmethod
    def get_nodes_ids():
        with open("/sys/devices/system/node/online") as node_list:
            return parse_range_list(node_list.read())

    def update_stats(self):
        cpustats = self.__env.get_cpu_stats()
        self.stats.update_cpustats(cpustats)
        memstats = self.__env.get_numa_stats()
        self.stats.update_memstats(memstats)
