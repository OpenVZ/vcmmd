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
import re, os, psutil

from vcmmd.util.singleton import Singleton
from vcmmd.util.stats import Stats
from vcmmd.util.misc import parse_range_list

class Numa(object):
    __metaclass__ = Singleton

    def __init__(self):
        self.cpus = {i: CPU(i) for i in self.get_logical_cpus_ids()}
        self.nodes = {i: Node(i) for i in self.get_nodes_ids()}

    def get_num_logical_cpus(self):
        return psutil.cpu_count(logical = True)

    def get_logical_cpus_ids(self):
        #FIXME: may be non-sequential, but numad had not cared about this.
        return range(self.get_num_logical_cpus())

    def get_nodes_ids(self):
        # FIXME: in numad ids were saved
        with open("/sys/devices/system/node/online") as node_list:
            return parse_range_list(node_list.read())

    def read_cpu_stats(self):
        stats = open("/proc/stat", "r").readlines()
        for cpu in self.cpus.values():
            cpu.update_stats(stats)

    def update_stats(self):
        self.read_cpu_stats()
        for node in self.nodes.values():
            node.update_stats()

    def __str__(self):
        res = "CPUs:\n"
        for cpu in self.cpus.values():
            res += "\t%s\n" % cpu
        res += "Nodes:\n"
        for node in self.nodes.values():
            res += "\t%s\n" % node
        return res

class NodeStats(Stats):

    ABSOLUTE_STATS = [
        'memtotal',         # total amount of physical memory on host
        'memfree',          # amount of memory left completely unused by host
        'inactivefile',     # pagecache memory that can be reclaimed without
                            #  huge performance impact
        'filepages',        # memory used for file cache
        'sreclaimable',     # amount of SLAB reclaimable memory
        'cpuidle',          # percentage of time spent in the idle task
    ]

    CUMULATIVE_STATS = []

    ALL_STATS = ABSOLUTE_STATS + CUMULATIVE_STATS

class Node(object):
    def __init__(self, node_id):
        self.id = node_id
        self.node_dir = "/sys/devices/system/node/node%d/" % self.id
        self.update_topology()
        self.stats = NodeStats()

    def update_topology(self):
        self.cpu_list = parse_range_list(open(self.node_dir + "cpulist").read())

    def update_stats(self):
        meminfo = open(self.node_dir + "meminfo").readlines()

        stats = {}
        for line in meminfo:
            line = line.split()
            # Node NUM VARIABLE: VALUE [kB]
            stats[line[2][:-1]] = int(line[3])

        self.stats._update(**{
            "memtotal" : stats["MemTotal"],
            "memfree" : stats["MemFree"],
            "inactivefile" : stats["Inactive(file)"],
            "filepages" : stats["FilePages"],
            "sreclaimable" : stats["SReclaimable"],
        # TODO: cpuidle sometimes more than 100
            "cpuidle" : sum(Numa().cpus[x].stats.idle for x in self.cpu_list)
        })

    def __str__(self):
        l = ["cpu_list", "stats"]
        return ("Node %s: %s" %
            (self.id, {x : str(self.__dict__[x]) for x in l}))

class CPUStats(Stats):

    ABSOLUTE_STATS = []

    CUMULATIVE_STATS = [
        'idle'                # percentage of time spent in the idle task
    ]

    ALL_STATS = ABSOLUTE_STATS + CUMULATIVE_STATS

    def __str__(self):
        return str({x : getattr(self, x, None) for x in self.ALL_STATS})


class CPU(object):
    def __init__(self, id):
        self.id = id
        self.stats = CPUStats()

    def update_stats(self, stat = None):
        if not stat:
            stat = open("/proc/stat", "r").readlines()
        line = [x for x in stat if ("cpu%s" % self.id) in x][0]
        idle = int(line.split()[4])
        self.stats._update(**{"idle" : idle})

    def __str__(self):
        l = ["stats"]
        return ("CPU %s:%s" % (self.id, {x : str(self.__dict__[x]) for x in l}))
