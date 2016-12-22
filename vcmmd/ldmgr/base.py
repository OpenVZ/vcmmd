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

import os
import logging
import threading
import time
import importlib
import psutil
from Queue import Full as QueueFull, Empty as QueueEmpty

from vcmmd.error import (VCMMDError,
                         VCMMD_ERROR_VE_NAME_ALREADY_IN_USE,
                         VCMMD_ERROR_VE_NOT_REGISTERED,
                         VCMMD_ERROR_UNABLE_APPLY_VE_GUARANTEE,
                         VCMMD_ERROR_TOO_MANY_REQUESTS,
                         VCMMD_ERROR_VE_NOT_ACTIVE)
from vcmmd.ve_config import VEConfig, DefaultVEConfig, VCMMD_MEMGUARANTEE_AUTO
from vcmmd.ve_type import VE_TYPE_CT, VE_TYPE_SERVICE
from vcmmd.config import VCMMDConfig
from vcmmd.ve import VE
from vcmmd.host import Host


class RQueue(object):
    """Create a queue object with a given maximum size.
    If maxsize is <= 0, the queue size is infinite.

    Every queue item MUST have timestamp atribute.
    """
    def __init__(self, maxsize=0):
        self._maxsize = maxsize
        self._reqs = []
        self._lock = threading.Lock()
        self._sort = lambda: self._reqs.sort(lambda x, y: int(x.timestamp - y.timestamp))
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

    def put(self, item, block=True):
        """Put an item into the queue.
        If 'block' is True and 'maxsize' is reached for queue it blocks
        until the vacant position will be in a queue.
        If 'block' is false raises the Full exception.
        """
        with self._not_full:
            if self._maxsize > 0 and len(self._reqs) >= self._maxsize:
                if not block:
                    raise QueueFull
                self._not_full.wait()
            self._reqs.append(item)
            # Sort items by timestams
            self._sort()
            self._not_empty.notify()

    def put_nowait(self, item):
        """Put an item into the queue without blocking.
        Only enqueue the item if a free slot is immediately available.
        Otherwise raise the Full exception.
        """
        return self.put(item, False)

    def get(self, block=True):
        """Remove and return an item from the queue.
        If 'block' is True and 'timestamp' is not reached for first item
        in queue it blocks until timestamp or wait a new item added.
        If 'block' is false raises the Empty exception if no item was available
        right now.
        """
        with self._not_empty:
            while True:
                if not len(self._reqs):
                    if not block:
                        raise QueueEmpty
                    self._not_empty.wait()
                    continue
                req = self._reqs[0]
                remaining = max(req.timestamp - time.time(), 0)
                if not remaining:
                    break
                elif not block:
                    # TODO should we raise an exception in such case?
                    raise QueueEmpty
                self._not_empty.wait(remaining)
            req = self._reqs.pop(0)
            self._not_full.notify()
            return req

    def get_nowait(self):
        """Remove and return an item from the queue without blocking.

        Only get an item if one is immediately available. Otherwise
        raise the Empty exception.
        """
        return self.get(False)


class Request(object):
    '''Common class for all requests to LoadManager
    '''
    def __init__(self, fn, args=None, kwargs=None, timeout=0, blocker=False):
        self.fn = fn
        self.args = args or ()
        self.kwargs = kwargs or {}
        self._ret = None
        self._err = None
        self._done = threading.Event()
        # Time when request should be executed.
        self.timestamp = timeout + time.time()
        self._blocker = blocker

    def is_blocker(self):
        '''
        If request is blocker it MUST be in Load Manager request queue.
        '''
        return self._blocker

    def wait(self):
        self._done.wait()
        if self._err:
            raise self._err
        return self._ret

    def __call__(self):
        try:
            self._ret = self.fn(*self.args, **self.kwargs)
            return self._ret
        except VCMMDError as err:
            self._err = err
        finally:
            self._done.set()


class LoadManager(object):

    FALLBACK_POLICY = 'NoOpPolicy'

    class ShutdownException(Exception):
        """Raise when shutdown request is received
        """
        pass

    def __init__(self):
        self.logger = logging.getLogger('vcmmd.ldmgr')

        self._registered_ves = {}  # str -> VE
        self._registered_ves_lock = threading.Lock()

        self._host = Host()

        cfg = VCMMDConfig()

        thn = cfg.get_num('LoadManager.ThreadsNum', 5)

        self._req_queue = RQueue(maxsize=25)
        self._workers = [threading.Thread(target=self._worker_thread_fn) for _ in range(thn)]
        [w.start() for w in self._workers]

        # Load a policy
        policy_name = cfg.get_str('LoadManager.Policy', self.FALLBACK_POLICY)
        policy_name = self._load_alias(policy_name)
        self._load_policy(policy_name)

    def _load_alias(self, policy_name):
        try:
            alias = importlib.import_module('vcmmd.ldmgr.policies.alias')
        except ImportError as err:
            return policy_name

        return alias.alias.get(policy_name, policy_name)

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
            self._req_queue.put(req, req.is_blocker())
        except QueueFull:
            self.logger.error('Too many requests, ignore(%r)', len(self._workers))

    def _worker_thread_fn(self):
        while True:
            req = self._req_queue.get()
            try:
                new_req = req()
            except LoadManager.ShutdownException:
                return
            if new_req:
                self._queue_request(new_req)

    def _request(sync=True):
        def wrap(fn):
            def wrapped(*args, **kwargs):
                self = args[0]
                req = Request(fn, args, kwargs)
                try:
                    self._req_queue.put_nowait(req)
                except Queue.Full:
                    raise VCMMDError(VCMMD_ERROR_TOO_MANY_REQUESTS)
                if sync:
                    return req.wait()
            return wrapped
        return wrap

    @_request()
    def _do_shutdown(self):
        def shutdown():
            raise LoadManager.ShutdownException
        for w in self._workers:
            self._queue_request(Request(shutdown, blocker=True))

    def shutdown(self):
        self._policy.shutdown()
        self._do_shutdown()
        [w.join() for w in self._workers]

    def _check_guarantees(self, delta):
        mem_min = sum(ve.mem_min for ve in self._registered_ves.itervalues())
        mem_min += delta
        if mem_min > self._host.ve_mem:
            raise VCMMDError(VCMMD_ERROR_UNABLE_APPLY_VE_GUARANTEE)

    @_request()
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

    @_request()
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

    @_request()
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

            ve.set_config(ve_config)
            ve.effective_limit = min(ve.config.limit, self._host.ve_mem)

            self._policy.ve_config_updated(ve)

    @_request()
    def deactivate_ve(self, ve_name):
        with self._registered_ves_lock:
            ve = self._registered_ves.get(ve_name)
            if ve is None:
                raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)

            ve.deactivate()
            self._policy.ve_deactivated(ve)

    @_request()
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
        cfg = VCMMDConfig().get_str('LoadManager.Policy')
        cur = self._policy.get_name()
        if self._load_alias(cfg) == cur:
            return cfg
        else:
            return cur

    def get_stats(self, ve_name):
        with self._registered_ves_lock:
            ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise VCMMDError(VCMMD_ERROR_VE_NOT_REGISTERED)
        res = ve.stats.report().iteritems()
        return res

    def get_quotas(self):
        with self._registered_ves_lock:
            return [(ve.name, ve.target, ve.protection)
                    for ve in self._registered_ves.itervalues()
                    if ve.active and ve.target is not None]

    def get_config(self, j):
        return VCMMDConfig().report(j)

    def get_policy_counts(self, j):
        return self._policy.report(j)
