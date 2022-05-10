# Copyright (c) 2016-2017, Parallels International GmbH
# Copyright (c) 2017-2022, Virtuozzo International GmbH, All rights reserved
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

import logging
import time

from vcmmd.cgroup import MemoryCgroup, BlkIOCgroup, CpuSetCgroup, CpuCgroup
from vcmmd.ve.base import Error, VEImpl, register_ve_impl
from vcmmd.ve_type import VE_TYPE_CT, VE_TYPE_SERVICE
from vcmmd.config import VCMMDConfig
from vcmmd.util.limits import INT64_MAX
from vcmmd.util.threading import run_async


logger = logging.getLogger(__name__)


def lookup_cgroup(klass, name):
    # A container's cgroup is located either at the top level of the cgroup
    # hierarchy or under machine.slice

    cg = klass(name)
    if cg.exists():
        return cg

    cg = klass("/machine.slice/" + name)
    if cg.exists():
        return cg

    # raise Error("cgroup not found: '{}'".format(cg.abs_path))
    logger.error("PSBM-140023: cgroup not found: '%s'; skipping", cg.abs_path)
    return None


class ABSVEImpl(VEImpl):

    VE_TYPE = VE_TYPE_CT

    def __init__(self, name):
        super(ABSVEImpl, self).__init__(name)
        self.mem_limit = INT64_MAX
        self._memcg = None

    def get_rss(self):
        try:
            return self._memcg.read_mem_stat()["total_rss"]
        except (KeyError, IOError) as err:
            raise Error("Cgroup read failed: {}".format(err))

    def set_mem_protection(self, value):
        """Use memcg/memory.low to protect the CT from host pressure."""
        try:
            self._memcg.write_mem_low(value)
        except IOError as err:
            raise Error("Cgroup write failed: {}".format(err))

    def set_mem_target(self, value):
        # Decreasing memory.high might take long as it implies memory reclaim,
        # so do it asynchronously.
        #
        # XXX: For the sake of simplicity, we don't care about failures here.
        # It is acceptable, because adjusting memory.high may fail only if
        # the cgroup gets destroyed, which we will see and report anyway from
        # get_stats().
        run_async(self._memcg.write_mem_high, value)
        self.mem_limit = value

    def set_config(self, config):
        try:
            self._memcg.write_oom_guarantee(config.guarantee)
            self._memcg.write_mem_config(config.limit, config.swap)
        except IOError as err:
            raise Error("Cgroup write failed: {}".format(err))

        self.mem_limit = min(self.mem_limit, config.limit)
        run_async(self._memcg.write_cache_limit_in_bytes, config.cache)


class CTImpl(ABSVEImpl):
    def __init__(self, name):
        super(CTImpl, self).__init__(name)
        self._cpusetcg = lookup_cgroup(CpuSetCgroup, name)
        self._blkcg = lookup_cgroup(BlkIOCgroup, name)
        self._cpucg = lookup_cgroup(CpuCgroup, name)
        self._memcg = lookup_cgroup(MemoryCgroup, name)

    def get_stats(self):
        try:
            current = self._memcg.read_mem_current()
            stat = self._memcg.read_mem_stat()
            io_serviced = self._blkcg.get_io_serviced()
            io_service_bytes = self._blkcg.get_io_service_bytes()
        except IOError as err:
            raise Error("Cgroup read failed: {}".format(err))

        memtotal = max(self.mem_limit, current)
        memfree = memtotal - current
        memavail = (
            memfree + stat.get("active_file", 0) +
            stat.get("inactive_file", 0) + stat.get("slab_reclaimable", 0)
        )

        return {
            "rss": current,
            "host_mem": current,
            "host_swap": stat.get("swap", -1),
            "actual": memtotal,
            "memfree": memfree,
            "memavail": memavail,
            "swapin": stat.get("pswpin", -1),
            "swapout": stat.get("pswpout", -1),
            "minflt": stat.get("pgfault", -1),
            "majflt": stat.get("pgmajfault", -1),
            "rd_req": io_serviced[0],
            "rd_bytes": io_service_bytes[0],
            "wr_req": io_serviced[1],
            "wr_bytes": io_service_bytes[1],
            "last_update": int(time.time()),
        }

    @property
    def nr_cpus(self):
        try:
            self._nr_cpus = self._cpucg.get_nr_cpus()
            return self._nr_cpus
        except IOError:
            return getattr(self, "_nr_cpus", -1)

    def get_node_list(self):
        """Get list of nodes where CT is running."""
        try:
            node_list = self._cpusetcg.get_node_list()
        except IOError as err:
            raise Error("Cgroup read failed: {}".format(err))
        return node_list

    def node_mem_migrate(self, nodes):
        try:
            self._memcg.set_node_list(nodes)
        except IOError as err:
            raise Error("Cgroup write failed: {}".format(err))

    def pin_node_mem(self, nodes):
        """Change list of memory nodes for CT.

        This function changes CT affinity for memory and migrates CT's memory
        accordingly.
        """
        try:
            self._cpusetcg.set_node_list(nodes)
        except IOError as err:
            raise Error("Cgroup write failed: {}".format(err))

    def pin_cpu_list(self, cpus):
        """Change list of CPUs for CT.

        This function changes CT affinity for CPUs.
        """
        try:
            self._cpusetcg.set_cpu_list(cpus)
        except IOError as err:
            raise Error("Cgroup write failed: {}".format(err))


class ServiceCTImpl(ABSVEImpl):

    VE_TYPE = VE_TYPE_SERVICE

    def __init__(self, name):
        super(ServiceCTImpl, self).__init__(name)
        self._memcg = lookup_cgroup(MemoryCgroup, name)
        try:
            self._cpucg = lookup_cgroup(CpuCgroup, name)
        except Error as err:
            logger.info("Skip using CPU cgroup: %s", err)
            self._cpucg = None
        self._default_cpu_share = VCMMDConfig().get_num(
            "VE.SRVC.DefaultCPUShare", default=10000,
            integer=True, minimum=1024
        )

    @property
    def nr_cpus(self):
        try:
            self._nr_cpus = self._cpucg.get_nr_cpus()
            return self._nr_cpus
        except (OSError, AttributeError):
            return -1

    def set_config(self, config):
        try:
            # protect services from OOM killer
            self._memcg.write_oom_guarantee(-1)
            self._memcg.write_mem_low(config.guarantee)
            self._memcg.write_mem_config(config.limit, config.swap)
            if not config.swap:
                self._memcg.write_swappiness(0)
            self._memcg.write_cleancache(False)
        except OSError as err:
            raise Error(f"CGroup write failed: {err}")

        run_async(self._memcg.write_cache_limit_in_bytes, config.cache)

        if self._cpucg:
            try:
                self._cpucg.write_cpu_shares(self._default_cpu_share)
            except OSError as err:
                raise Error(f"CGroup write failed: {err}")


register_ve_impl(CTImpl)
register_ve_impl(ServiceCTImpl)
