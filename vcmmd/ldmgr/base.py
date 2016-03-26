from __future__ import absolute_import

import Queue
import os
import logging
import threading
import time
import importlib
import shelve
import psutil

from vcmmd.errno import *
from vcmmd.config import VCMMDConfig
from vcmmd.cgroup import MemoryCgroup
from vcmmd.ve import (VE, Config as VEConfig, Error as VEError,
                      InvalidVENameError, InvalidVETypeError)
from vcmmd.util.misc import clamp


class Error(Exception):
    '''VCMMD service error.

    Possible values of self.errno are defined in vcmmd.ldmgr.errno.
    '''

    def __init__(self, errno):
        self.errno = errno

    def __str__(self):
        return strerror(self.errno)


class LoadManager(object):

    DEFAULT_POLICY = 'WFBPolicy'

    _VE_STATE_FILE = '/var/run/vcmmd.state'

    def __init__(self):
        self.logger = logging.getLogger('vcmmd.ldmgr')

        self._registered_ves = {}  # str -> VE
        self._registered_ves_lock = threading.Lock()

        self._req_queue = Queue.Queue()
        self._last_update = 0
        self._worker = threading.Thread(target=self._worker_thread_fn)
        self._should_stop = False

        self._worker.start()

    def _load_policy(self, policy_name):
        try:
            policy_module = importlib.import_module(
                'vcmmd.ldmgr.policies.' + policy_name)
        except ImportError as err:
            self.logger.error("Failed to load policy '%s': %s",
                              policy_name, err)
            # fallback on default policy
            policy_name = self.DEFAULT_POLICY
            policy_module = importlib.import_module(
                'vcmmd.ldmgr.policies.' + policy_name)
        self._policy = self._policy = getattr(policy_module, policy_name)()
        self.logger.info("Loaded policy '%s'", policy_name)

    def _mem_size_from_config(self, name, mem_total, default):
        cfg = VCMMDConfig()
        share = cfg.get_num('LoadManager.%s.Share' % name,
                            default=default[0], minimum=0.0, maximum=1.0)
        min_ = cfg.get_num('LoadManager.%s.Min' % name,
                           default=default[1], integer=True, minimum=0)
        max_ = cfg.get_num('LoadManager.%s.Max' % name,
                           default=default[2], integer=True, minimum=0)
        return clamp(int(mem_total * share), min_, max_)

    def _set_slice_mem(self, name, value, verbose=True):
        memcg = MemoryCgroup(name + '.slice')
        if not memcg.exists():
            return
        try:
            memcg.write_mem_low(value)
            memcg.write_oom_guarantee(value)
        except IOError as err:
            self.logger.error('Failed to set reservation for %s slice: %s',
                              name, err)
        else:
            if verbose:
                self.logger.info('Reserved %s bytes for %s slice', value, name)

    def _do_init(self):
        cfg = VCMMDConfig()

        # Load a policy
        self._load_policy(cfg.get_str('LoadManager.Policy',
                                      self.DEFAULT_POLICY))

        # Configure update interval
        self._update_interval = cfg.get_num('LoadManager.UpdateInterval',
                                            default=5, integer=True, minimum=1)
        self.logger.info('Update interval set to %ds', self._update_interval)

        # Reserve memory for the system
        total_mem = psutil.virtual_memory().total
        host_mem = self._mem_size_from_config('HostMem', total_mem,
                                              (0.04, 128 << 20, 320 << 20))
        sys_mem = self._mem_size_from_config('SysMem', total_mem,
                                             (0.04, 128 << 20, 320 << 20))
        user_mem = self._mem_size_from_config('UserMem', total_mem,
                                              (0.02, 32 << 20, 128 << 20))
        self._set_slice_mem('user', user_mem)
        self._set_slice_mem('system', sys_mem)

        # Calculate size of memory available for VEs
        self._mem_avail = (total_mem - host_mem - sys_mem - user_mem)
        self.logger.info('%d bytes available for VEs', self._mem_avail)
        if self._mem_avail < 0:
            self.logger.error('Not enough memory to run VEs!')

    def _save_ve_state(self, ve):
        self._ve_state[ve.name] = {
            'type': ve.VE_TYPE,
            'active': ve.active,
            'config': ve.config._asdict(),
        }
        self._ve_state.sync()

    def _delete_ve_state(self, ve):
        del self._ve_state[ve.name]
        self._ve_state.sync()

    def _do_restore_ves(self):
        self.logger.info("Restoring VE state from file '%s'",
                         self._VE_STATE_FILE)
        self._ve_state = shelve.open(self._VE_STATE_FILE)
        stale_ves = []
        for ve_name, ve_params in self._ve_state.iteritems():
            ve = None
            try:
                ve = self._do_register_ve(ve_name,
                                          ve_params['type'],
                                          ve_params['config'])
                if ve_params['active']:
                    self._do_activate_ve(ve)
            except Error as err:
                self.logger.error("Failed to restore VE '%s': %s",
                                  ve_name, err)
                if ve is not None:
                    self._do_unregister_ve(ve)
                    stale_ves.append(ve)
            else:
                self.logger.info('Restored %s %s (%s)',
                                 'active' if ve.active else 'inactive',
                                 ve, ve.config)
        for ve in stale_ves:
            self._delete_ve_state(ve)

    def _restore_ves(self):
        # State file might be corrupted, handle this gracefully.
        try:
            self._do_restore_ves()
        except Exception as err:
            self.logger.error('Unexpected error while reading VE state file: '
                              '%s', err)
        else:
            return
        # In case of error, try to recreate the state file.
        os.remove(self._VE_STATE_FILE)
        self._ve_state = shelve.open(self._VE_STATE_FILE)
        assert not self._ve_state

    def _queue_request(self, req):
        self._req_queue.put(req)

    def _process_request(self):
        timeout = (self._last_update + self._update_interval - time.time())
        block = timeout > 0
        try:
            req = self._req_queue.get(block=block, timeout=timeout)
        except Queue.Empty:
            self._balance_ves()
        else:
            req()
            self._req_queue.task_done()

    def _worker_thread_fn(self):
        self._do_init()
        self._restore_ves()
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
        # Check that the sum of guarantees plus the new VE's guarantee fit in
        # available memory.
        mem_min = sum(ve.mem_min for ve in self._registered_ves.itervalues())
        mem_min += new_ve.mem_min
        return mem_min <= self._mem_avail

    def _may_update_ve_config(self, ve_to_update, new_config):
        # Check that the sum of guarantees still fit in available memory.
        mem_min = sum(ve.mem_min for ve in self._registered_ves.itervalues())
        mem_min += new_config.guarantee - ve_to_update.config.guarantee
        return mem_min <= self._mem_avail

    def _balance_ves(self):
        # Update VE stats if enough time has passed
        now = time.time()
        if now > self._last_update + self._update_interval:
            self._last_update = now
            need_update = True
        else:
            need_update = False

        # Calculate size of memory available for applications running inside
        # active VEs, i.e. total memory available for all VEs minus memory
        # reserved for inactive VEs minus memory overhead of active VEs.
        mem_avail = self._mem_avail
        for ve in self._registered_ves.itervalues():
            if ve.active:
                if need_update:
                    ve.update()
                    self._policy.ve_updated(ve)
                mem_avail -= ve.overhead
            else:
                mem_avail -= ve.mem_min

        mem_avail = max(mem_avail, 0)
        self.logger.debug('mem_avail:%s', mem_avail)

        # Call the policy to calculate VEs' quotas.
        ve_quotas = self._policy.balance(mem_avail)
        sum_quota = sum(ve_quotas.itervalues())

        # Apply the quotas.
        for ve, quota in ve_quotas.iteritems():
            # If sum quota calculated by the policy is less than the amount of
            # available memory, we strive to protect the whole VE allocation
            # from host pressure so as to avoid costly swapping.
            #
            # If sum quota is greater than the amount of available memory, we
            # can't do that obviously. In this case we protect as much as
            # configured guarantees.
            ve.set_mem(target=quota, protection=(quota + ve.overhead
                                                 if sum_quota <= mem_avail
                                                 else ve.mem_min))

        # We need to set memory.low for machine.slice to infinity, otherwise
        # memory.low in sub-cgroups won't have any effect. We can't do it on
        # start, because machine.slice might not exist at that time (it is
        # created on demand, when the first VM starts).
        #
        # This is safe, because there is nothing running inside machine.slice
        # but VMs, each of which should have its memory.low configured
        # properly.
        self._set_slice_mem('machine', -1, verbose=False)

    def _do_register_ve(self, ve_name, ve_type, ve_config):
        if ve_name in self._registered_ves:
            raise Error(VCMMD_ERROR_VE_NAME_ALREADY_IN_USE)

        try:
            ve_config = VEConfig.from_dict(ve_config)
        except ValueError:
            raise Error(VCMMD_ERROR_INVALID_VE_CONFIG)

        try:
            ve = VE(ve_type, ve_name, ve_config)
        except InvalidVENameError:
            raise Error(VCMMD_ERROR_INVALID_VE_NAME)
        except InvalidVETypeError:
            raise Error(VCMMD_ERROR_INVALID_VE_TYPE)
        except VEError as err:
            self.logger.error("Failed to register '%s': %s", ve_name, err)
            raise Error(VCMMD_ERROR_VE_OPERATION_FAILED)

        if not self._may_register_ve(ve):
            raise Error(VCMMD_ERROR_NO_SPACE)

        with self._registered_ves_lock:
            self._registered_ves[ve_name] = ve

        return ve

    @_request()
    def register_ve(self, ve_name, ve_type, ve_config):
        ve = self._do_register_ve(ve_name, ve_type, ve_config)
        self._save_ve_state(ve)
        self.logger.info('Registered %s (%s)', ve, ve.config)
        self._balance_ves()

    def _do_activate_ve(self, ve):
        if ve.active:
            raise Error(VCMMD_ERROR_VE_ALREADY_ACTIVE)
        if not ve.activate():
            raise Error(VCMMD_ERROR_VE_OPERATION_FAILED)

        ve.update()
        self._policy.ve_activated(ve)

    @_request()
    def activate_ve(self, ve_name):
        ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise Error(VCMMD_ERROR_VE_NOT_REGISTERED)
        self._do_activate_ve(ve)
        self._save_ve_state(ve)
        self._balance_ves()

    @_request()
    def update_ve_config(self, ve_name, ve_config):
        ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise Error(VCMMD_ERROR_VE_NOT_REGISTERED)
        if not ve.active:
            raise Error(VCMMD_ERROR_VE_NOT_ACTIVE)

        try:
            ve_config = VEConfig.from_dict(ve_config, default=ve.config)
        except ValueError:
            raise Error(VCMMD_ERROR_INVALID_VE_CONFIG)

        if not self._may_update_ve_config(ve, ve_config):
            raise Error(VCMMD_ERROR_NO_SPACE)

        if not ve.set_config(ve_config):
            raise Error(VCMMD_ERROR_VE_OPERATION_FAILED)

        self._save_ve_state(ve)
        self._policy.ve_config_updated(ve)
        self._balance_ves()

    @_request()
    def deactivate_ve(self, ve_name):
        ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise Error(VCMMD_ERROR_VE_NOT_REGISTERED)
        if not ve.active:
            raise Error(VCMMD_ERROR_VE_NOT_ACTIVE)

        # We need uptodate rss for inactive VEs - see VE.mem_min
        ve.update()

        ve.deactivate()
        self._policy.ve_deactivated(ve)

        self._save_ve_state(ve)
        self._balance_ves()

    def _do_unregister_ve(self, ve):
        with self._registered_ves_lock:
            del self._registered_ves[ve.name]
        if ve.active:
            self._policy.ve_deactivated(ve)

    @_request()
    def unregister_ve(self, ve_name):
        ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise Error(VCMMD_ERROR_VE_NOT_REGISTERED)
        self._do_unregister_ve(ve)
        self._delete_ve_state(ve)
        self.logger.info('Unregistered %s', ve)
        self._balance_ves()

    def is_ve_active(self, ve_name):
        with self._registered_ves_lock:
            try:
                return self._registered_ves[ve_name].active
            except KeyError:
                raise Error(VCMMD_ERROR_VE_NOT_REGISTERED)

    def get_ve_config(self, ve_name):
        with self._registered_ves_lock:
            try:
                return self._registered_ves[ve_name].config
            except KeyError:
                raise Error(VCMMD_ERROR_VE_NOT_REGISTERED)

    def get_all_registered_ves(self):
        result = []
        with self._registered_ves_lock:
            for ve in self._registered_ves.itervalues():
                result.append((ve.name, ve.VE_TYPE, ve.active, ve.config))
        return result
