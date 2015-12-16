import Queue

import logging
import threading

from vcmmd import Error
from vcmmd import errno as _errno
from vcmmd.ve import Config as VEConfig
from vcmmd.ve.make import make as make_ve
from vcmmd.ldmgr.policies import DefaultPolicy


class LoadManager(object):

    def __init__(self, policy=DefaultPolicy(), logger=None):
        self.policy = policy
        self.logger = logger or logging.getLogger(__name__)

        self._registered_ves = {}  # str -> VE
        self._registered_ves_lock = threading.Lock()

        self._committed_ves = []

        self._req_queue = Queue.Queue()

        self._worker = threading.Thread(target=self._worker_thread_fn)
        self._should_stop = False

        self._worker.start()

    def _queue_request(self, req):
        self._req_queue.put(req)

    def _process_request(self):
        req = self._req_queue.get()
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

    def _balance_ves(self):
        policy_setting = self.policy.balance(self._registered_ves.values())
        for ve, (low, high) in policy_setting.iteritems():
            if not ve.committed:
                continue
            try:
                ve.set_mem_range(low, high)
            except Exception as err:
                self.logger.error('Failed to apply policy setting for %s: %s' %
                                  (ve, err))

    @_request()
    def register_ve(self, ve_name, ve_type, ve_config):
        try:
            ve_config = VEConfig(*ve_config)
        except ValueError:
            raise Error(_errno.INVALID_VE_CONFIG)

        if ve_name in self._registered_ves:
            raise Error(_errno.VE_NAME_ALREADY_IN_USE)

        ve = make_ve(ve_name, ve_type)
        ve.set_config(ve_config)

        with self._registered_ves_lock:
            self._registered_ves[ve_name] = ve

        self.logger.info('Registered %s %s' % (ve, ve_config))

    @_request()
    def commit_ve(self, ve_name):
        ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise Error(_errno.VE_NOT_REGISTERED)

        if ve.committed:
            raise Error(_errno.VE_ALREADY_COMMITTED)

        try:
            ve.commit()
        except Exception as err:
            self.logger.error('Failed to commit %s: %s' % (ve, err))
            raise Error(_errno.VE_OPERATION_FAILED)

        self._committed_ves.append(ve)

        self.logger.info('Committed %s' % ve)

        self._balance_ves()

    @_request()
    def update_ve(self, ve_name, ve_config):
        try:
            ve_config = VEConfig(*ve_config)
        except ValueError:
            raise Error(_errno.INVALID_VE_CONFIG)

        ve = self._registered_ves.get(ve_name)
        if ve is None:
            raise Error(_errno.VE_NOT_REGISTERED)

        try:
            ve.set_config(ve_config)
        except Exception as err:
            self.logger.error('Failed to update %s: %s' % (ve, err))
            raise Error(_errno.VE_OPERATION_FAILED)

        self.logger.info('Updated %s %s' % (ve, ve_config))

        self._balance_ves()

    @_request()
    def unregister_ve(self, ve_name):
        with self._registered_ves_lock:
            ve = self._registered_ves.pop(ve_name, None)
        if ve is None:
            raise Error(_errno.VE_NOT_REGISTERED)

        if ve.committed:
            self._committed_ves.remove(ve)

        self.logger.info("Unregistered %s" % ve)

        self._balance_ves()

    def get_all_registered_ves(self):
        result = []
        with self._registered_ves_lock:
            for ve in self._registered_ves.itervalues():
                result.append((ve.name, ve.VE_TYPE, ve.committed, ve.config))
        return result
