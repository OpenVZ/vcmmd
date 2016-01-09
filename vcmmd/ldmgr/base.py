from __future__ import absolute_import

import Queue
import logging
import threading
import time
import psutil

from vcmmd.ve import VE, Config as VEConfig, Error as VEError
from vcmmd.ve.make import (make as make_ve,
                           InvalidVENameError,
                           InvalidVETypeError)
from vcmmd.ldmgr import errno as _errno
from vcmmd.ldmgr.policies import DefaultPolicy


class Error(Exception):
    '''VCMMD service error.

    Possible values of self.errno are defined in vcmmd.ldmgr.errno.
    '''

    def __init__(self, errno):
        self.errno = errno


class LoadManager(object):

    _HOST_MEM_PCT = 5           # 5 %
    _HOST_MEM_MIN = 128 << 20   # 128 MB
    _HOST_MEM_MAX = 1 << 30     # 1 GB

    _UPDATE_INTERVAL = 5        # seconds

    _IDLE_MEM_PERIOD = 60       # seconds
    _IDLE_MEM_SAMPLING = 0.1

    def __init__(self, policy=DefaultPolicy(), logger=None):
        self.policy = policy
        self.logger = logger or logging.getLogger(__name__)

        self._init_mem_total()
        self._registered_ves = {}  # str -> VE
        self._registered_ves_lock = threading.Lock()

        self._req_queue = Queue.Queue()

        self._last_stats_update = 0

        self._worker = threading.Thread(target=self._worker_thread_fn)
        self._should_stop = False

        VE.enable_idle_mem_tracking(self._IDLE_MEM_PERIOD,
                                    self._IDLE_MEM_SAMPLING)

        self._worker.start()

    def _init_mem_total(self):
        mem = psutil.virtual_memory()

        # We should leave some memory for the host. Give it some percentage of
        # total memory, but never give too little or too much.
        host_rsrv = mem.total * self._HOST_MEM_PCT / 100
        host_rsrv = max(host_rsrv, self._HOST_MEM_MIN)
        host_rsrv = min(host_rsrv, self._HOST_MEM_MAX)

        self._mem_total = mem.total - host_rsrv

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

    def _may_register_ve(self, ve):
        return self.policy.may_register(ve, self._registered_ves.values(),
                                        self._mem_total)

    def _may_update_ve(self, ve, new_config):
        return self.policy.may_update(ve, new_config,
                                      self._registered_ves.values(),
                                      self._mem_total)

    def _balance_ves(self):
        now = time.time()
        if now >= self._last_stats_update + self._UPDATE_INTERVAL:
            self._last_stats_update = now
            update_stats = True
        else:
            update_stats = False

        # Build the list of active VEs and calculate the amount of memory
        # available for them.
        active_ves = []
        mem_avail = self._mem_total
        for ve in self._registered_ves.itervalues():
            # If a VE is inactive, we exclude it from the balance procedure.
            # To still take its load into account, we subtract the value of its
            # memory usage seen last time from the amount of available memory.
            if not ve.active:
                if ve.mem_stats.rss > 0:
                    mem_avail -= ve.mem_stats.rss
                continue

            active_ves.append(ve)

            if update_stats:
                try:
                    ve.update_stats()
                except VEError as err:
                    self.logger.error('Failed to update stats for %s: %s' %
                                      (ve, err))

        # Call the policy to calculate VEs' quotas.
        ve_quotas = self.policy.balance(active_ves, mem_avail, update_stats)

        # Apply the quotas.
        for ve, quota in ve_quotas.iteritems():
            assert ve.active
            try:
                ve.set_quota(quota)
            except VEError as err:
                self.logger.error('Failed to set quota for %s: %s' % (ve, err))

    @_request()
    def register_ve(self, ve_name, ve_type, ve_config, force=False):
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

        if not force and not self._may_register_ve(ve):
            raise Error(_errno.NO_SPACE)

        with self._registered_ves_lock:
            self._registered_ves[ve_name] = ve

        self.logger.info('Registered %s %s' % (ve, ve_config))

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
            self.logger.error('Failed to activate %s: %s' % (ve, err))
            raise Error(_errno.VE_OPERATION_FAILED)

        self.logger.info('Activated %s' % ve)

        # Update stats for the newly activated VE before calling the balance
        # procedure.
        try:
            ve.update_stats()
        except VEError as err:
            self.logger.error('Failed to update stats for %s: %s' % (ve, err))

        self._balance_ves()

    @_request()
    def update_ve(self, ve_name, ve_config, force=False):
        ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise Error(_errno.VE_NOT_REGISTERED)

        try:
            ve_config = VEConfig.from_dict(ve_config, default=ve.config)
        except ValueError:
            raise Error(_errno.VE_CONFIG_CONFLICT)

        if not force and not self._may_update_ve(ve, ve_config):
            raise Error(_errno.NO_SPACE)

        try:
            ve.set_config(ve_config)
        except VEError as err:
            self.logger.error('Failed to update %s: %s' % (ve, err))
            raise Error(_errno.VE_OPERATION_FAILED)

        self.logger.info('Updated %s %s' % (ve, ve_config))

        # No need to rebalance if config is updated for an inactive VE.
        if ve.active:
            self._balance_ves()

    @_request()
    def deactivate_ve(self, ve_name):
        ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise Error(_errno.VE_NOT_REGISTERED)

        if not ve.active:
            raise Error(_errno.VE_NOT_ACTIVE)

        # Update stats for the VE being deactivated for the last time.
        try:
            ve.update_stats()
        except VEError as err:
            self.logger.error('Failed to update stats for %s: %s' % (ve, err))

        ve.deactivate()

        self.logger.info('Deactivated %s' % ve)

        self._balance_ves()

    @_request()
    def unregister_ve(self, ve_name):
        with self._registered_ves_lock:
            ve = self._registered_ves.pop(ve_name, None)
        if ve is None:
            raise Error(_errno.VE_NOT_REGISTERED)

        self.logger.info("Unregistered %s" % ve)

        self._balance_ves()

    def get_all_registered_ves(self):
        result = []
        with self._registered_ves_lock:
            for ve in self._registered_ves.itervalues():
                result.append((ve.name, ve.VE_TYPE, ve.active, ve.config))
        return result
