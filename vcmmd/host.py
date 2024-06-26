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

import os
import psutil
import re
import socket
from abc import ABCMeta
import multiprocessing

from vcmmd.util.singleton import Singleton
from vcmmd.util.stats import Stats
from vcmmd.util.misc import clamp
from vcmmd.util.threading import update_stats_single
from vcmmd.config import VCMMDConfig
from vcmmd.cgroup import MemoryCgroup
from vcmmd.numa import Numa as AbsNuma
from vcmmd.env import Env


class HostStats(Stats):

    ABSOLUTE_STATS = [
        'memtotal',         # total amount of physical memory on host
        'swaptotal',        # total swap size on host
        'memfree',          # amount of memory left completely unused by host
        'memavail',         # an estimate of how much memory is available for
                            # starting new applications, without swapping
        'ksm_pg_shared',    # how many shared pages are being used for ksm
        'ksm_pg_sharing',   # how many more sites are sharing them
        'ksm_pg_unshared',  # how many pages unique but repeatedly checked for merging
        'ksm_pg_volatile',  # how many pages changing too fast to be placed in a tree
        'ksm_pages_to_scan',# how many present pages to scan before ksmd goes to sleep
        'ksm_run',          # set 0 to stop ksmd from running but keep merged pages,
                            # set 1 to run ksmd,
                            # set 2 to stop ksmd and unmerge all pages currently merged.

    ]

    CUMULATIVE_STATS = [
        'ksm_full_scans',   # how many times all mergeable areas have been scanned
    ]


HostMeta = type("HostMeta", (Singleton, ABCMeta), {})


class Host(Env, metaclass=HostMeta):

    KSM_CONTROL_PATH = '/sys/kernel/mm/ksm/{}'
    THP_CONTROL_PATH = '/sys/kernel/mm/transparent_hugepage/{}'


    class Numa(AbsNuma):
        @update_stats_single
        def update_stats(self):
            super(Host.Numa, self).update_stats()

    def __init__(self):
        self.hostname = socket.gethostname()
        super(Host, self).__init__("vcmmd.host")
        self.stats = HostStats()
        self.total_mem = psutil.virtual_memory().total
        self.ve_mem = self.total_mem
        self.log_info('%d bytes available for VEs', self.ve_mem)
        if self.ve_mem < 0:
            self.log_err('Not enough memory to run VEs!')
        self.numa = Host.Numa(self)
        self._set_slice_mem('machine', -1, verbose=False)
        self._set_slice_mem('vstorage', -1, verbose=False)

    def __str__(self):
        return self.hostname

    def _mem_size_from_config(self, name, mem_total, default):
        cfg = VCMMDConfig()
        share = cfg.get_num('Host.{}.Share'.format(name),
                            default=default[0], minimum=0.0, maximum=1.0)
        min_ = cfg.get_num('Host.{}.Min'.format(name),
                           default=default[1], integer=True, minimum=0)
        max_ = cfg.get_num('Host.{}.Max'.format(name),
                           default=default[2], integer=True, minimum=0)
        return clamp(int(mem_total * share), min_, max_)

    def _set_slice_mem(self, name, value, oom_guarantee=None, verbose=True):
        if oom_guarantee is None:
            oom_guarantee = value
        if value == -1 and 'vz7' not in os.uname().release:
            value = 'max'

        memcg = MemoryCgroup(name + '.slice')
        if not memcg.exists():
            self.log_err('Memory cgroup %s.slice does not exist', name)
            return
        try:
            memcg.write_mem_low(value)
            memcg.write_oom_guarantee(oom_guarantee)
        except IOError as err:
            self.log_err('Failed to set reservation for %s slice: %s',
                              name, err)
        else:
            if verbose:
                self.log_info('Reserved %s bytes for %s slice', value, name)

    def get_slice_swap(self, name):
        memcg = MemoryCgroup(name + '.slice')
        if not memcg.exists():
            return
        try:
            return memcg.read_swap_current()
        except IOError as err:
            self.log_err('Failed to get swap usage for %s slice: %s', name, err)

    @update_stats_single
    def update_stats(self):
        '''Update host stats.
        '''
        sysfs_keys = ['full_scans', 'pages_sharing', 'pages_unshared',
                      'pages_shared', 'pages_volatile', 'pages_to_scan', 'run']

        ksm_stats = {}
        for datum in sysfs_keys:
            name = self.KSM_CONTROL_PATH.format(datum)
            try:
                with open(name, 'r') as ksm_stats_file:
                    ksm_stats[datum] = int(ksm_stats_file.read())
            except IOError as err:
                ksm_stats[datum] = -1
                self.log_err("Failed to update stat: open %s failed: %s",
                             name, err)
        mem = psutil.virtual_memory()

        stats = {'memtotal': self.total_mem,
                 'swaptotal': self.get_swap_total(),
                 'memfree': mem.free,
                 'memavail': mem.available,
                 'ksm_pg_shared': ksm_stats.get('pages_shared', -1),
                 'ksm_pg_sharing': ksm_stats.get('pages_sharing', -1),
                 'ksm_pg_unshared': ksm_stats.get('pages_unshared', -1),
                 'ksm_pg_volatile': ksm_stats.get('pages_volatile', -1),
                 'ksm_full_scans': ksm_stats.get('full_scans', -1),
                 'ksm_pages_to_scan': ksm_stats.get('pages_to_scan', -1),
                 'ksm_run': ksm_stats.get('run', -1),
                 }
        self.stats._update(**stats)

    def thptune(self, params):
        for key, val in params.items():
            try:
                with open(self.THP_CONTROL_PATH.format(key), 'w') as f:
                    f.write(str(val))
            except IOError as err:
                self.log_debug("Failed to set %r = %r",
                               self.THP_CONTROL_PATH.format(key), val)

    def ksmtune(self, params):
        for key, val in params.items():
            try:
                with open(self.KSM_CONTROL_PATH.format(key), 'w') as f:
                    f.write(str(val))
            except IOError as err:
                # few options could be not changed until page shared/sharing != 0
                # need start ksmd for update stats if it's not running.
                self.log_debug("Failed to set %r = %r",
                               self.KSM_CONTROL_PATH.format(key), val)

    def _get_numa_node_stats(self, node_id):
        node_dir = self.numa.NUMA_NODE_SYS_PATH.format(node_id)
        try:
            with open(node_dir + 'meminfo') as f:
                meminfo = dict((s[2][:-1], int(s[3])) for s in map(str.split, f.readlines()))
        except IOError as err:
            self.log_err('Failed to update memory stats: %s', err)
            return

        memtotal = meminfo.get('MemTotal', 0) << 10
        memfree = meminfo.get('MemFree', 0) << 10
        memusage = memtotal - memfree - (meminfo.get('KReclaimable', 0) << 10)

        return {'memtotal': memtotal, 'memusage': memusage, 'memfree': memfree}

    def get_numa_stats(self):
        return dict((n, self._get_numa_node_stats(n)) for n in self.numa.nodes_ids)

    def get_cpu_stats(self):
        try:
            with open("/proc/stat") as f:
                stats = f.readlines()
        except IOError as err:
            self.log_err('Failed to update CPU stats: %s', err)
            return {}

        names = ["cpuuser", "cpunice", "cpusystem", "cpuidle"]
        cpustats = {}
        for line in stats:
            if not re.search("cpu\d+", line):
                continue
            res = {}
            cpu, data = re.split(" ", line, maxsplit = 1)
            cpu = int(cpu[3:])
            for name, value in zip(names, re.findall("(\d+)", data)):
                res[name] = int(value)
            cpustats[cpu] = res

        return cpustats

    @staticmethod
    def get_cpu_count():
        if hasattr(psutil, 'cpu_count'):
             return psutil.cpu_count(logical = True)
        # Workaround for old psutil(1.2.1)
        # multiprocessing.cpu_count relies on a _SC_NPROCESSORS_ONLN
        # The values might differ with _SC_NPROCESSORS_CONF in systems with
        # advanced CPU power management functionality.
        # In some occasions multiprocessing.cpu_count may raise a
        # NotImplementedError while psutil will be able to obtain
        # the number of CPUs.
        return multiprocessing.cpu_count()

    @staticmethod
    def get_swap_total():
        return psutil.swap_memory().total

    def check_numa_complete(self) -> bool:
        """Verify that all NUMA-nodes has RAM."""
        numa_ok = True
        for node_id, stats in self.get_numa_stats().items():
            if stats['memtotal'] == 0:
                self.log_err('NUMA-node %i without RAM found', node_id)
                numa_ok = False
        return numa_ok
