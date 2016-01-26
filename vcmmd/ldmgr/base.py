from __future__ import absolute_import

import Queue
import logging
import threading
import time
import importlib
import psutil

from vcmmd.config import VCMMDConfig
from vcmmd.cgroup import MemoryCgroup
from vcmmd.ve import VE, Config as VEConfig, Error as VEError
from vcmmd.ve.make import (make as make_ve,
                           InvalidVENameError,
                           InvalidVETypeError)
from vcmmd.ldmgr import errno as _errno


class Error(Exception):
    '''VCMMD service error.

    Possible values of self.errno are defined in vcmmd.ldmgr.errno.
    '''

    def __init__(self, errno):
        self.errno = errno

    def __str__(self):
        return _errno.strerror(self.errno)


class LoadManager(object):

    DEFAULT_POLICY = 'WeightedFeedbackBasedPolicy'

    _HOST_MEM_PCT = 5           # 5 %
    _HOST_MEM_MIN = 128 << 20   # 128 MB
    _HOST_MEM_MAX = 1 << 30     # 1 GB

    # Portion of host memory to reserve for user and system slices.
    _USER_SLICE_RSRV = 0.2
    _SYSTEM_SLICE_RSRV = 0.4

    _TSWAP_ENABLED = True
    _TCACHE_ENABLED = True

    _UPDATE_INTERVAL = 5        # seconds

    _IDLE_MEM_PERIOD = 60       # seconds
    _IDLE_MEM_SAMPLING = 0.1

    def __init__(self):
        self.logger = logging.getLogger('vcmmd.LoadManager')

        self._init_mem_avail()
        self._active_ves = []
        self._registered_ves = {}  # str -> VE
        self._registered_ves_lock = threading.Lock()

        self._req_queue = Queue.Queue()
        self._last_stats_update = 0
        self._worker = threading.Thread(target=self._worker_thread_fn)
        self._should_stop = False

        self._load_policy()
        self._init_tmem()
        self._init_system_slices()
        VE.enable_idle_mem_tracking(self._IDLE_MEM_PERIOD,
                                    self._IDLE_MEM_SAMPLING)
        self._worker.start()

    def _do_load_policy(self, policy_name):
        policy_module = importlib.import_module('vcmmd.ldmgr.policies.' +
                                                policy_name)
        self._policy = getattr(policy_module, policy_name)()

    def _load_policy(self):
        policy_name = VCMMDConfig().get_str('LoadManager.Policy',
                                            self.DEFAULT_POLICY)
        try:
            self._do_load_policy(policy_name)
        except ImportError:
            assert policy_name != self.DEFAULT_POLICY
            self.logger.error("Policy '%s' not found", policy_name)
            # Fallback on default policy
            policy_name = self.DEFAULT_POLICY
            self._do_load_policy(policy_name)
        self.logger.info("Loaded policy '%s'", policy_name)

    def _init_mem_avail(self):
        mem = psutil.virtual_memory()

        # We should leave some memory for the host. Give it some percentage of
        # total memory, but never give too little or too much.
        host_rsrv = mem.total * self._HOST_MEM_PCT / 100
        host_rsrv = max(host_rsrv, self._HOST_MEM_MIN)
        host_rsrv = min(host_rsrv, self._HOST_MEM_MAX)

        self._host_rsrv = host_rsrv
        self._mem_avail = mem.total - host_rsrv

    def _toggle_tmem(self, subsys, enable):
        try:
            with open('/sys/module/%s/parameters/active' % subsys, 'w') as f:
                f.write('Y' if enable else 'N')
        except IOError as err:
            # Don't care about failures if we don't want the feature enabled.
            if enable:
                self.logger.error('Failed to enable %s: %s', subsys, err)
        else:
            if enable:
                self.logger.info('Enabled %s', subsys)

    def _init_tmem(self):
        self._toggle_tmem('tswap', self._TSWAP_ENABLED)
        self._toggle_tmem('tcache', self._TCACHE_ENABLED)

    def _set_slice_rsrv(self, name, value, verbose=True):
        memcg = MemoryCgroup(name + '.slice')
        if not memcg.exists():
            return
        try:
            memcg.write_mem_low(value)
        except IOError as err:
            self.logger.error('Failed to set reservation for %s slice: %s',
                              name, err)
        else:
            if verbose:
                self.logger.info('Reserved %s bytes for %s slice', value, name)

    def _init_system_slices(self):
        self._set_slice_rsrv('user', int(self._host_rsrv *
                                         self._USER_SLICE_RSRV))
        self._set_slice_rsrv('system', int(self._host_rsrv *
                                           self._SYSTEM_SLICE_RSRV))

    def _queue_request(self, req):
        self._req_queue.put(req)

    def _process_request(self):
        timeout = (self._last_stats_update +
                   self._UPDATE_INTERVAL - time.time())
        block = timeout > 0
        try:
            req = self._req_queue.get(block=block, timeout=timeout)
        except Queue.Empty:
            self._balance_ves()
        else:
            req()
            self._req_queue.task_done()

    def _worker_thread_fn(self):
        while not self._should_stop:
            self._process_request()

    def _request(sync=True):

        class Request(object):

            def __init__(self, fn, args, kwargs):
                self.fn = fn
                self.args = args
                self.kwargs = kwargs
                self._ret = None
                self._err = None
                self._done = threading.Event()

            def wait(self):
                self._done.wait()
                if self._err:
                    raise self._err
                return self._ret

            def __call__(self):
                try:
                    ret = self.fn(*self.args, **self.kwargs)
                except Error as err:
                    self._err = err
                else:
                    self._ret = ret
                self._done.set()

        def wrap(fn):
            def wrapped(*args, **kwargs):
                self = args[0]
                req = Request(fn, args, kwargs)
                self._queue_request(req)
                if sync:
                    return req.wait()
            return wrapped

        return wrap

    @_request()
    def _do_shutdown(self):
        self._should_stop = True

    def shutdown(self):
        self._do_shutdown()
        self._worker.join()

    def _may_register_ve(self, new_ve):
        # Check that the sum of guarantees plus the new VE's quota fit in
        # available memory.
        sum_guar = sum(ve.config.guarantee for ve in self._active_ves)
        return sum_guar + new_ve.quota <= self._mem_avail

    def _may_update_ve(self, ve_to_update, new_config):
        # Check that the sum of guarantees still fit in available memory.
        sum_guar = sum(ve.config.guarantee for ve in self._active_ves)
        return (sum_guar - ve_to_update.config.guarantee +
                new_config.guarantee <= self._mem_avail)

    def _balance_ves(self):
        # Update VE stats if enough time has passed
        now = time.time()
        if now >= self._last_stats_update + self._UPDATE_INTERVAL:
            for ve in self._active_ves:
                try:
                    ve.update_stats()
                except VEError as err:
                    self.logger.error('Failed to update stats for %s: %s',
                                      ve, err)
            self._last_stats_update = now
            stats_updated = True
        else:
            stats_updated = False

        # Call the policy to calculate VEs' quotas.
        ve_quotas = self._policy.balance(self._active_ves, self._mem_avail,
                                         stats_updated)

        # Apply the quotas.
        for ve, quota in ve_quotas.iteritems():
            assert ve.active
            try:
                ve.set_quota(quota)
            except VEError as err:
                self.logger.error('Failed to set quota for %s: %s', ve, err)

        # We need to set memory.low for machine.slice to infinity, otherwise
        # memory.low in sub-cgroups won't have any effect. We can't do it on
        # start, because machine.slice might not exist at that time (it is
        # created on demand, when the first VM starts).
        #
        # This is safe, because there is nothing running inside machine.slice
        # but VMs, each of which should have its memory.low configured
        # properly.
        self._set_slice_rsrv('machine', -1, verbose=False)

    @_request()
    def register_ve(self, ve_name, ve_type, ve_config):
        if ve_name in self._registered_ves:
            raise Error(_errno.VE_NAME_ALREADY_IN_USE)

        try:
            ve = make_ve(ve_name, ve_type)
        except InvalidVENameError:
            raise Error(_errno.INVALID_VE_NAME)
        except InvalidVETypeError:
            raise Error(_errno.INVALID_VE_TYPE)

        try:
            ve_config = VEConfig.from_dict(ve_config)
        except ValueError:
            raise Error(_errno.VE_CONFIG_CONFLICT)

        ve.set_config(ve_config)

        if not self._may_register_ve(ve):
            raise Error(_errno.NO_SPACE)

        with self._registered_ves_lock:
            self._registered_ves[ve_name] = ve

        # Reserve memory for the inactive VE.
        self._mem_avail -= ve.quota

        self.logger.info('Registered %s %s', ve, ve_config)

        self._balance_ves()

    @_request()
    def activate_ve(self, ve_name):
        ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise Error(_errno.VE_NOT_REGISTERED)

        if ve.active:
            raise Error(_errno.VE_ALREADY_ACTIVE)

        try:
            ve.activate()
        except VEError as err:
            self.logger.error('Failed to activate %s: %s', ve, err)
            raise Error(_errno.VE_OPERATION_FAILED)

        # Update stats for the newly activated VE before calling the balance
        # procedure.
        try:
            ve.update_stats()
        except VEError as err:
            self.logger.error('Failed to update stats for %s: %s', ve, err)

        self._active_ves.append(ve)

        # Unaccount reserved memory.
        self._mem_avail += ve.quota

        self.logger.info('Activated %s', ve)

        self._balance_ves()

    @_request()
    def update_ve(self, ve_name, ve_config):
        ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise Error(_errno.VE_NOT_REGISTERED)
        if not ve.active:
            raise Error(_errno.VE_NOT_ACTIVE)

        try:
            ve_config = VEConfig.from_dict(ve_config, default=ve.config)
        except ValueError:
            raise Error(_errno.VE_CONFIG_CONFLICT)

        if not self._may_update_ve(ve, ve_config):
            raise Error(_errno.NO_SPACE)

        try:
            ve.set_config(ve_config)
        except VEError as err:
            self.logger.error('Failed to update %s: %s', ve, err)
            raise Error(_errno.VE_OPERATION_FAILED)

        self.logger.info('Updated %s %s', ve, ve_config)

        self._balance_ves()

    @_request()
    def deactivate_ve(self, ve_name):
        ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise Error(_errno.VE_NOT_REGISTERED)

        if not ve.active:
            raise Error(_errno.VE_NOT_ACTIVE)

        ve.deactivate()

        self._active_ves.remove(ve)

        # Reserve memory for the inactive VE.
        self._mem_avail -= ve.quota

        self.logger.info('Deactivated %s', ve)

        self._balance_ves()

    @_request()
    def unregister_ve(self, ve_name):
        with self._registered_ves_lock:
            ve = self._registered_ves.pop(ve_name, None)
        if ve is None:
            raise Error(_errno.VE_NOT_REGISTERED)

        if ve.active:
            self._active_ves.remove(ve)
        else:
            # Unaccount reserved memory.
            self._mem_avail += ve.quota

        self.logger.info('Unregistered %s', ve)

        self._balance_ves()

    def get_all_registered_ves(self):
        result = []
        with self._registered_ves_lock:
            for ve in self._registered_ves.itervalues():
                result.append((ve.name, ve.VE_TYPE, ve.active, ve.config))
        return result
