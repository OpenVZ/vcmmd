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

import Queue
import os
import logging
import threading
import time
import importlib
import psutil

from vcmmd.error import (VCMMDError,
                         VCMMD_ERROR_VE_NAME_ALREADY_IN_USE,
                         VCMMD_ERROR_VE_NOT_REGISTERED,
                         VCMMD_ERROR_UNABLE_APPLY_VE_GUARANTEE,
                         VCMMD_ERROR_TOO_MANY_REQUESTS)
from vcmmd.ve_config import VEConfig, DefaultVEConfig
from vcmmd.config import VCMMDConfig
from vcmmd.ve import VE
from vcmmd.host import Host


class RQueue:
    def __init__(self, maxsize):
        self._maxsize = maxsize
        self._reqs = []
        self._lock = threading.Lock()
        self._sort = lambda: self._reqs.sort(lambda x,y: int(x.timestemp - y.timestemp))

    def put(self, element):
        with self._lock:
            if self._maxsize > 0 and len(self._reqs) >= self._maxsize:
                raise Queue.Full
            self._reqs.append(element)
            self._sort()

    def get(self):
        with self._lock:
            if not self._reqs:
                return
            req = self._reqs[0]
            if req.timestemp <= time.time():
                return self._reqs.pop(0)


class Request(object):

    def __init__(self, fn, args = None, kwargs = None, timeout = 0):
        self.fn = fn
        self.args = args or ()
        self.kwargs = kwargs or {}
        self._ret = None
        self._err = None
        self._done = threading.Event()
        self.timestemp = timeout + time.time()

    def wait(self):
        self._done.wait()
        if self._err:
            raise self._err
        return self._ret

    def __call__(self):
        try:
            ret = self.fn(*self.args, **self.kwargs)
        except VCMMDError as err:
            self._err = err
        else:
            self._ret = ret
        self._done.set()


class LoadManager(object):

    FALLBACK_POLICY = 'NoOpPolicy'

    def __init__(self):
        self.logger = logging.getLogger('vcmmd.ldmgr')

        self._registered_ves = {}  # str -> VE
        self._registered_ves_lock = threading.Lock()

        self._host = Host()

        cfg = VCMMDConfig()

        thn = cfg.get_num('LoadManager.ThreadsNum', 5)

        self._req_queue = RQueue(maxsize = 25)
        self._workers = [threading.Thread(target=self._worker_thread_fn) for _ in range(thn)]
        self._should_stop = False
        [w.start() for w in self._workers]

        # Load a policy
        self._load_policy(cfg.get_str('LoadManager.Policy',
                                      self.FALLBACK_POLICY))

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
        self.logger.info("Loaded policy '%s'", policy_name)
        reqs = self._policy.sched_req()
        for req in reqs:
            self._queue_request(req)

    def _queue_request(self, req):
        try:
            self._req_queue.put(req)
        except Queue.Full:
            self.logger.error('Too many requests, ignore(%r)', len(self._workers))

    def _worker_thread_fn(self):
        while not self._should_stop:
            req = self._req_queue.get()
            if not req:
                time.sleep(0.1)
                continue
            req()
            new_req = req.wait()
            if new_req:
                self._queue_request(new_req)

    def _request(sync=True):
        def wrap(fn):
            def wrapped(*args, **kwargs):
                self = args[0]
                req = Request(fn, args, kwargs)
                try:
                    self._req_queue.put(req)
                except Queue.Full:
                    raise VCMMDError(VCMMD_ERROR_TOO_MANY_REQUESTS)
                if sync:
                    return req.wait()
            return wrapped
        return wrap

    @_request()
    def _do_shutdown(self):
        self._should_stop = True

    def shutdown(self):
        self._do_shutdown()
        [w.join() for w in self._workers]

    def _check_guarantees(self, delta):
        mem_min = sum(ve.mem_min for ve in self._registered_ves.itervalues())
        mem_min += delta
        if mem_min > self._host.ve_mem:
            raise VCMMDError(VCMMD_ERROR_UNABLE_APPLY_VE_GUARANTEE)

    @_request()
    def register_ve(self, ve_name, ve_type, ve_config):
        if ve_name in self._registered_ves:
            raise VCMMDError(VCMMD_ERROR_VE_NAME_ALREADY_IN_USE)

        ve_config.complete(DefaultVEConfig)
        ve = VE(ve_type, ve_name, ve_config)
        self._check_guarantees(ve.mem_min)
        ve.effective_limit = min(ve.config.limit, self._host.ve_mem)

        with self._registered_ves_lock:
            self._registered_ves[ve_name] = ve
        self._policy.ve_registered(ve)

        self.logger.info('Registered %s (%s)', ve, ve.config)

    @_request()
    def activate_ve(self, ve_name):
        ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

        ve.activate()
        self._policy.ve_activated(ve)

    @_request()
    def update_ve_config(self, ve_name, ve_config):
        ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

        ve_config.complete(ve.config)
        self._check_guarantees(ve_config.mem_min - ve.config.mem_min)

        ve.set_config(ve_config)
        ve.effective_limit = min(ve.config.limit, self._host.ve_mem)

        self._policy.ve_config_updated(ve)

    @_request()
    def deactivate_ve(self, ve_name):
        ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

        ve.deactivate()
        self._policy.ve_deactivated(ve)

    @_request()
    def unregister_ve(self, ve_name):
        ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

        with self._registered_ves_lock:
            del self._registered_ves[ve.name]
        self._policy.ve_unregistered(ve)
        if ve.active:
            self._policy.ve_deactivated(ve)

        self.logger.info('Unregistered %s', ve)

    def is_ve_active(self, ve_name):
        with self._registered_ves_lock:
            try:
                return self._registered_ves[ve_name].active
            except KeyError:
                raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

    def get_ve_config(self, ve_name):
        with self._registered_ves_lock:
            try:
                return self._registered_ves[ve_name].config.as_tuple()
            except KeyError:
                raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

    def get_all_registered_ves(self):
        result = []
        with self._registered_ves_lock:
            for ve in self._registered_ves.itervalues():
                result.append((ve.name, ve.VE_TYPE, ve.active,
                               ve.config.as_tuple()))
        return result

    def get_current_policy(self):
        return self._policy.get_name()

    def get_stats(self, ve_name):
        with self._registered_ves_lock:
            ve = self._registered_ves.get(ve_name)
            if ve is None:
                raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)
            res = ve.stats.report()
            for id, stat in ve.numa_stats.iteritems():
                res.extend([("N%s_" % id + stat, value) for stat, value in stat.report()])
            return res

    @_request()
    def get_quotas(self):
        return [(ve.name, ve.target, ve.protection)
                for ve in self._registered_ves.itervalues()
                if ve.active and ve.target is not None]
