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

import itertools

from vcmmd.error import (
    VCMMDError,
    VCMMD_ERROR_INVALID_VE_NAME,
    VCMMD_ERROR_INVALID_VE_TYPE,
    VCMMD_ERROR_INVALID_VE_CONFIG,
    VCMMD_ERROR_VE_ALREADY_ACTIVE,
    VCMMD_ERROR_VE_NOT_ACTIVE,
    VCMMD_ERROR_VE_OPERATION_FAILED,
)
from vcmmd.util.stats import Stats
from vcmmd.numa import Numa as AbsNuma
from vcmmd.env import Env
from vcmmd.util.threading import update_stats_single
from vcmmd.ve_type import (
    get_ve_type_name,
    VE_TYPE_SERVICE,
    VE_TYPE_CT,
    VE_TYPE_VM_LINUX,
    VE_TYPE_VM_WINDOWS,
    VE_TYPE_VM,
)


class Error(Exception):
    pass


class VEStats(Stats):

    ABSOLUTE_STATS = [
        "rss",          # resident set size
        "actual",       # actual amount of memory committed to the guest
                        # (RAM size - balloon size for VM, memory limit for CT)
        "host_mem",     # size of host memory used
                        # (note it can be less than rss due to shared pages)
        "host_swap",    # size of host swap used
        "memfree",      # amount of memory left completely unused by guest OS
        "memavail",     # an estimate of how much memory is available for
                        # starting new applications, without swapping
        "last_update",  # timestamp when was memory stats updated
    ]

    CUMULATIVE_STATS = [
        "swapin",    # amount of memory read in from swap space
        "swapout",   # amount of memory written out to swap space
        "minflt",    # minor page fault count
        "majflt",    # major page fault count
        "rd_req",    # number of read requests
        "rd_bytes",  # number of read bytes
        "wr_req",    # number of write requests
        "wr_bytes",  # number of written bytes
    ]


class VEImpl:
    """VE implementation.

    This class defines the interface to an underlying VE implementation
    (such as libvirt or cgroup).

    Any of the functions defined by this interface may raise Error.
    """

    def __init__(self, name):
        pass

    @staticmethod
    def mem_overhead(config_limit):
        """Return an estimate of memory overhead.

        This function is supposed to return the amount of memory beyond the
        configured limit which is required to run the VE smoothly. E.g. for
        VMs this should equal expected RSS of the emulator process.
        """
        return 0

    def get_stats(self):
        """Return stats dict {name: value}."""
        pass

    def set_mem_protection(self, value):
        """Set memory best-effort protection.

        If memory usage of a VE is below this value, the VE's memory shouldn't
        be reclaimed on host pressure if memory can be reclaimed from
        unprotected VEs.
        """
        pass

    def set_mem_target(self, value):
        """Set memory allocation target.

        This function sets memory consumption target for a VE. Note, it does
        not necessarily mean that the VE memory usage will reach the target
        instantly or even any time soon - in fact, it may not reach it at all
        in case allocation is reduced. However, reducing the value will put the
        VE under heavy local memory pressure forcing it to release its memory
        to the host.
        """
        pass

    def set_config(self, config):
        """Set new config."""
        pass

    def pin_node_mem(self, nodes):
        """
        Should be expanded when memory migration "knob" will be implemented
        in kernel.
        """
        pass

    def pin_cpu_list(self, cpus):
        """
        Should be expanded when memory migration "knob" will be implemented
        in kernel.
        """
        pass


_VE_IMPL_MAP = {}  # VE type -> VE implementation class


def register_ve_impl(ve_impl):
    assert ve_impl.VE_TYPE not in _VE_IMPL_MAP
    _VE_IMPL_MAP[ve_impl.VE_TYPE] = ve_impl


def _lookup_ve_impl(ve_type):
    try:
        return _VE_IMPL_MAP[ve_type]
    except KeyError:
        raise VCMMDError(VCMMD_ERROR_INVALID_VE_TYPE)


def _check_ve_name(name):
    if not name:
        raise VCMMDError(VCMMD_ERROR_INVALID_VE_NAME)


def _check_ve_config(config):
    if not config.is_valid():
        raise VCMMDError(VCMMD_ERROR_INVALID_VE_CONFIG)


class VE(Env):
    class Numa(AbsNuma):
        @update_stats_single
        def update_stats(self):
            super(VE.Numa, self).update_stats()

    def __init__(self, ve_type, name, config):
        super(VE, self).__init__("vcmmd.ve")
        _check_ve_name(name)
        _check_ve_config(config)

        self._impl = _lookup_ve_impl(ve_type)
        self._obj = None

        self.name = name
        self.config = config
        self.stats = VEStats()
        self.numa = VE.Numa(self)
        self.active = False
        self._overhead = self._impl.mem_overhead(config.limit)

        # Instantiate service as soon as possible in order to
        # gently prevent registering of not existing service.
        if ve_type == VE_TYPE_SERVICE:
            self._get_obj()

    def __str__(self):
        return "{} '{}'".format(get_ve_type_name(self.VE_TYPE), self.name)

    @property
    def VE_TYPE(self):
        return self._impl.VE_TYPE

    def _get_obj(self):
        if self._obj is None:
            obj = self._impl(self.name)
            self._obj = obj
            self.set_config(self.config)
        return self._obj

    def activate(self):
        """Mark VE active.

        This function is supposed to be called after a VE switched to a state,
        in which its memory allocation can be tuned.
        """
        if self.active:
            raise VCMMDError(VCMMD_ERROR_VE_ALREADY_ACTIVE)

        self.active = True
        self.log_info("Activated")

    def deactivate(self):
        """Mark VE inactive.

        This function is supposed to be called before switching a VE to a state
        in which its runtime memory parameters cannot be changed anymore (e.g.
        suspended or paused).
        """
        if not self.active:
            raise VCMMDError(VCMMD_ERROR_VE_NOT_ACTIVE)

        self.active = False
        self.log_info("Deactivated")

    def get_numa_stats(self):
        try:
            if not self.active:
                raise Error("VE is not activated")

            cg = self._get_obj()._memcg
            return cg.get_numa_stats()
        except (Error, IOError) as err:
            self.log_err("Failed to update numa stats: %s", err)
        return {}

    def get_cpu_stats(self):
        try:
            if not self.active:
                raise Error("VE is not activated")

            cg = self._get_obj()._cpucg
            return cg.get_cpu_stats()
        except (Error, IOError) as err:
            self.log_err("Failed to update CPU stats: %s", err)
        return {}

    @update_stats_single
    def update_stats(self):
        """Update VE stats."""
        try:
            if not self.active:
                raise Error("VE is not activated")

            obj = self._get_obj()
            self.stats._update(**obj.get_stats())
        except Error as err:
            self.log_err("Failed to update stats: %s", err)

    @property
    def mem_overhead(self):
        return self._overhead + self.config.vram

    @property
    def mem_min(self):
        """Return min memory size required by this VE.

        Normally, it simply returns configured guarantee plus overhead.
        However, for an inactive VE the result will never be less than RSS,
        because its allocation cannot be tuned anymore.
        """
        val = self.config.mem_min + self._overhead
        if not (self._impl.VE_TYPE == VE_TYPE_SERVICE or self.active):
            val = max(val, self.get_rss(verbose=False))
        return val

    @property
    def mem_shared(self):
        return max(0, self.stats.rss - self.stats.host_mem)

    def apply_limit_settings(self):
        """Set VE memory consumption according to VE's configuration."""
        msg = ""
        try:
            obj = self._get_obj()
            vm_types = (VE_TYPE_VM_LINUX, VE_TYPE_VM_WINDOWS, VE_TYPE_VM)
            # Don't set target memory for VM's because libvirt manages it
            if self.config.limit and obj.VE_TYPE not in vm_types:
                obj.set_mem_target(self.config.limit)
                msg = "target:{} ".format(self.config.limit)
            obj.set_mem_protection(self.mem_min)
            msg += "protection:{}".format(self.mem_min)
        except Error as err:
            self.log_err("Failed to tune allocation: %s", err)
        else:
            if msg:
                self.log_debug("set_mem: %s", msg)

    def set_config(self, config):
        """Update VE config."""
        _check_ve_config(config)
        old_config = self.config
        try:
            obj = self._get_obj()
            if obj.VE_TYPE != VE_TYPE_SERVICE:
                config.update(cpunum=obj.nr_cpus)
            self.config = config
            obj.set_config(config)
        except Error as err:
            self.log_err(f"Failed to set config: {err}")
            self.config = old_config
            obj.set_config(old_config)
            raise VCMMDError(VCMMD_ERROR_VE_OPERATION_FAILED)

        self.log_info("Config updated: %s", config)

    def get_config(self):
        config = self.config
        try:
            if self.active:
                obj = self._get_obj()
                if obj.VE_TYPE != VE_TYPE_SERVICE:
                    config.update(cpunum=obj.nr_cpus)
        except Error as err:
            self.log_err(f"Failed to get config {err}")
            raise VCMMDError(VCMMD_ERROR_INVALID_VE_CONFIG)
        return config

    def set_node_list(self, nodes):
        """Set VE NUMA binding."""
        cpus = set()
        for n in nodes:
            cpus.update(self.numa.cpu_list[n])
        self.pin_node_mem(nodes)
        self.pin_cpu_list(cpus)

    def pin_node_mem(self, nodes, migrate=True):
        """Change list of memory nodes for VE.

        This function changes VE affinity for memory and migrates VE's memory
        accordingly
        """
        try:
            obj = self._get_obj()
            obj.pin_node_mem(nodes)
            if self.VE_TYPE == VE_TYPE_CT and migrate:
                obj.node_mem_migrate(nodes)
        except Error as err:
            self.log_err("Failed to bind NUMA nodes: %s", err)
        else:
            self.log_debug("pin_node_mem: %s", nodes)

    def pin_cpu_list(self, cpus):
        """Change list of CPUs for VE

        This function changes VE affinity for CPUs
        """
        try:
            obj = self._get_obj()
            obj.pin_cpu_list(cpus)
        except Error as err:
            self.log_err("Failed to bind CPU list: %s", err)
        else:
            self.log_debug("pin_cpu_list: %s", [*cpus])

    def reset_numa_settings(self):
        """Reset all NUMA-related bindings"""
        self.pin_node_mem(self.numa.nodes_ids, migrate=False)
        self.pin_cpu_list(itertools.chain(*self.numa.cpu_list.values()))

    def numa_enforce_settings(self):
        if self.numa_configured():
            self.reset_numa_settings()
            if self.config.nodelist:
                self.pin_node_mem(self.config.nodelist)
            if self.config.cpulist:
                self.pin_cpu_list(self.config.cpulist)

    @property
    def nr_cpus(self):
        try:
            return self._get_obj().nr_cpus
        except Error as err:
            self.log_err("Failed to get number of vCPUs: %s", err)
        return -1

    def numa_configured(self):
        return self.config.nodelist or self.config.cpulist

    def get_rss(self, verbose=True):
        try:
            return self._get_obj().get_rss()
        except Error as err:
            if verbose:
                self.log_err("Failed to get RSS: %s", err)
        return -1
