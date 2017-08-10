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

from __future__ import absolute_import

import os
import logging
import threading
import importlib

from vcmmd.error import (VCMMDError,
                         VCMMD_ERROR_VE_NAME_ALREADY_IN_USE,
                         VCMMD_ERROR_VE_NOT_REGISTERED,
                         VCMMD_ERROR_UNABLE_APPLY_VE_GUARANTEE,
                         VCMMD_ERROR_TOO_MANY_REQUESTS,
                         VCMMD_ERROR_VE_NOT_ACTIVE,
                         VCMMD_ERROR_POLICY_SET_ACTIVE_VES,
                         VCMMD_ERROR_POLICY_SET_INVALID_NAME)
from vcmmd.ve_config import VEConfig, DefaultVEConfig, VCMMD_MEMGUARANTEE_AUTO
from vcmmd.ve_type import VE_TYPE_CT, VE_TYPE_SERVICE
from vcmmd.config import VCMMDConfig
from vcmmd.ve import VE
from vcmmd.host import Host


class LoadManager(object):

    FALLBACK_POLICY = 'NoOpPolicy'

    def __init__(self):
        self.logger = logging.getLogger('vcmmd.ldmgr')

        self._registered_ves = {}  # str -> VE
        self._registered_ves_lock = threading.Lock()

        self.alias = None

        self.cfg = VCMMDConfig()

        self._host = Host()

        policy_name = self.cfg.get_str('LoadManager.Policy', self.FALLBACK_POLICY)
        policy_name = self._load_alias(policy_name)
        self._load_policy(policy_name)

    def switch_policy(self, policy_name):
        if self.alias is not None and policy_name not in self.alias:
            raise VCMMDError(VCMMD_ERROR_POLICY_SET_INVALID_NAME)
        self.cfg.dump('LoadManager.Policy', policy_name)
        policy_name = self._load_alias(policy_name)
        with self._registered_ves_lock:
            for ve_name in self._registered_ves:
                ve = self._registered_ves.get(ve_name)
                if ve.VE_TYPE != VE_TYPE_SERVICE:
                    raise VCMMDError(VCMMD_ERROR_POLICY_SET_ACTIVE_VES)
            self.shutdown()
            # Load a policy
            self._load_policy(policy_name)
            for ve_name in self._registered_ves:
                ve = self._registered_ves.get(ve_name)
                self._policy.ve_registered(ve)
                if ve.active:
                    self._policy.ve_activated(ve)

    def _load_alias(self, policy_name):
        try:
            alias = importlib.import_module('vcmmd.ldmgr.policies.alias')
        except ImportError as err:
            return policy_name

        self.alias = alias.alias
        return self.alias.get(policy_name, policy_name)

    def _load_policy(self, policy_name):
        try:
            policy_module = importlib.import_module(
                'vcmmd.ldmgr.policies.' + policy_name)
        except ImportError as err:
            self.logger.error("Failed to load policy '%s': %s",
                              policy_name, err)
            # fallback on default policy
            policy_name = self.FALLBACK_POLICY
            policy_module = importlib.import_module(
                'vcmmd.ldmgr.policies.' + policy_name)
        self._policy = getattr(policy_module, policy_name)()
        self._policy.load()
        self.logger.info("Loaded policy '%s'", policy_name)

    def shutdown(self):
        self._policy.shutdown()

    def _check_guarantees(self, delta):
        mem_min = sum(ve.mem_min for ve in self._registered_ves.itervalues())
        mem_min += delta
        if mem_min > self._host.ve_mem:
            raise VCMMDError(VCMMD_ERROR_UNABLE_APPLY_VE_GUARANTEE)

    def register_ve(self, ve_name, ve_type, ve_config):
        with self._registered_ves_lock:
            if ve_name in self._registered_ves:
                raise VCMMDError(VCMMD_ERROR_VE_NAME_ALREADY_IN_USE)

            ve_config.complete(DefaultVEConfig)
            if ve_type not in (VE_TYPE_CT, VE_TYPE_SERVICE) and \
               ve_config.guarantee_type == VCMMD_MEMGUARANTEE_AUTO:
                ve_config.update(guarantee = int(ve_config.limit * self._policy.DEFAULT_VM_AUTO_GUARANTEE))
            ve = VE(ve_type, ve_name, ve_config)
            self._check_guarantees(ve.mem_min)
            ve.effective_limit = min(ve.config.limit, self._host.ve_mem)

            self._registered_ves[ve_name] = ve
            self._policy.ve_registered(ve)

            self.logger.info('Registered %s (%s)', ve, ve.config)

    def activate_ve(self, ve_name):
        with self._registered_ves_lock:
            ve = self._registered_ves.get(ve_name)
            if ve is None:
                raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

            ve.activate()
            self._policy.ve_activated(ve)

        # We need to set memory.low for machine.slice to infinity, otherwise
        # memory.low in sub-cgroups won't have any effect. We can't do it on
        # start, because machine.slice might not exist at that time (it is
        # created on demand, when the first VM starts).
        #
        # This is safe, because there is nothing running inside machine.slice
        # but VMs, each of which should have its memory.low configured
        # properly.
        # TODO need only once
        self._host._set_slice_mem('machine', -1, verbose=False)

    def update_ve_config(self, ve_name, ve_config):
        with self._registered_ves_lock:
            ve = self._registered_ves.get(ve_name)
            if ve is None:
                raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)
            if not ve.active:
                raise VCMMDError(VCMMD_ERROR_VE_NOT_ACTIVE)

            ve_config.complete(ve.config)
            if ve.VE_TYPE not in (VE_TYPE_CT, VE_TYPE_SERVICE) and \
               ve_config.guarantee_type == VCMMD_MEMGUARANTEE_AUTO:
                ve_config.update(guarantee = int(ve_config.limit * self._policy.DEFAULT_VM_AUTO_GUARANTEE))
            self._check_guarantees(ve_config.mem_min - ve.config.mem_min)

            try:
                ve.set_config(ve_config)
            except VCMMDError:
                pass
            ve.effective_limit = min(ve.config.limit, self._host.ve_mem)

            self._policy.ve_config_updated(ve)

    def deactivate_ve(self, ve_name):
        with self._registered_ves_lock:
            ve = self._registered_ves.get(ve_name)
            if ve is None:
                raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

            ve.deactivate()
            self._policy.ve_deactivated(ve)

    def unregister_ve(self, ve_name):
        with self._registered_ves_lock:
            ve = self._registered_ves.get(ve_name)
            if ve is None:
                raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

            del self._registered_ves[ve.name]

            self._policy.ve_unregistered(ve)
            if ve.active:
                self._policy.ve_deactivated(ve)
                self.logger.info('Unregistered %s', ve)

    def is_ve_active(self, ve_name):
        try:
            with self._registered_ves_lock:
                return self._registered_ves[ve_name].active
        except KeyError:
            raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

    def get_ve_config(self, ve_name):
        with self._registered_ves_lock:
            try:
                return self._registered_ves[ve_name].config.as_array()
            except KeyError:
                raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

    def get_all_registered_ves(self):
        result = []
        with self._registered_ves_lock:
            for ve in self._registered_ves.itervalues():
                result.append((ve.name, ve.VE_TYPE, ve.active,
                               ve.config.as_array()))
        return result

    def get_current_policy(self):
        cfg = self.cfg.get_str('LoadManager.Policy')
        cur = self._policy.get_name()
        if self._load_alias(cfg) == cur:
            return cfg
        else:
            return cur

    def get_policy_from_file(self):
        cfg = self.cfg.read()

        if cfg is None:
            return ""

        return cfg.get('LoadManager',{}).get('Policy',"")

    def get_stats(self, ve_name):
        with self._registered_ves_lock:
            ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)
        res = ve.stats.report().iteritems()
        return res

    def get_free(self):
        with self._registered_ves_lock:
            qemu_vram_overhead = 0
            guarantee = 0
            for ve in self._registered_ves.itervalues():
                qemu_vram_overhead += ve.mem_overhead
                guarantee += ve.protection
        reserved = self._host.host_mem + self._host.sys_mem + self._host.user_mem
        swap = self._host.get_slice_swap('machine')
        if swap is None:
            swap = 0
        available = max(self._host.total_mem - reserved - qemu_vram_overhead - guarantee, 0)
        return {'total': self._host.total_mem,
                'host reserved': reserved,
                'qemu overhead+vram': qemu_vram_overhead,
                'guarantee': guarantee,
                'swap': swap,
                'available': available}

    def get_config(self, j):
        return VCMMDConfig().report(j)

    def get_policy_counts(self, j):
        return self._policy.report(j)
