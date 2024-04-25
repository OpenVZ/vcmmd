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

import logging
import time

from multiprocessing import TimeoutError

from vcmmd.cgroup import MemoryCgroup, BlkIOCgroup, CpuSetCgroup, CpuCgroup
from vcmmd.ve.base import Error, VEImpl, register_ve_impl
from vcmmd.ve_type import VE_TYPE_NONE, VE_TYPE_SERVICE
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

    raise Error("cgroup not found: '{}'".format(cg.abs_path))


class ABSVEImpl(VEImpl):

    VE_TYPE = VE_TYPE_NONE

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

    def apply_cache_limit(self, config):
        res = run_async(self._memcg.write_cache_limit_in_bytes, config.cache)
        try:
            # Updating cache limit might take a while, hence we're doing it
            # asynchronously.  But if writing to the cgroup fails, we return
            # immediately
            res.get(timeout=2.5)
        except TimeoutError:
            pass
        except OSError as err:
            raise Error(f"CGroup write failed: {err}")

    def set_config(self, config):
        try:
            self._memcg.write_oom_guarantee(config.guarantee)
            self._memcg.write_mem_config(config.limit, config.swap)
        except IOError as err:
            raise Error("Cgroup write failed: {}".format(err))

        self.mem_limit = min(self.mem_limit, config.limit)
        self.apply_cache_limit(config)


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
            self._memcg.write_oom_guarantee(config.guarantee)
            self._memcg.write_mem_config(config.limit, config.swap)
            if not config.swap:
                self._memcg.write_swappiness(0)
            self._memcg.write_cleancache(False)
        except OSError as err:
            raise Error(f"CGroup write failed: {err}")

        self.apply_cache_limit(config)

        if self._cpucg:
            try:
                self._cpucg.write_cpu_shares(self._default_cpu_share)
            except OSError as err:
                raise Error(f"CGroup write failed: {err}")


register_ve_impl(ServiceCTImpl)
