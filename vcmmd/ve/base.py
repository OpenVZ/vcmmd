from __future__ import absolute_import

import logging

from vcmmd.error import (VCMMDError,
                         VCMMD_ERROR_INVALID_VE_NAME,
                         VCMMD_ERROR_INVALID_VE_TYPE)
from vcmmd.ve_type import get_ve_type_name
from vcmmd.ve.stats import Stats


class Error(Exception):
    pass


class VEImpl(object):
    '''VE implementation.

    This class defines the interface to an underlying VE implementation
    (such as libvirt or cgroup).

    Any of the functions defined by this interface may raise Error.
    '''

    def __init__(self, name):
        pass

    @classmethod
    def estimate_overhead(cls, name):
        '''Return an estimate of memory overhead.

        This function is supposed to return the amount of memory beyond the
        configured limit which is required to run the VE smoothly. For VMs this
        will be VRAM size plus emulator process RSS.

        Note, this is a class method, because it is called before a VE gets
        activated, i.e. when a VEImpl object hasn't been created yet. In
        particular this means that its implementation can only check persistent
        VE configuration.
        '''
        return 0

    def get_stats(self):
        '''Return stats dict {name: value}.
        '''
        pass

    def set_mem_protection(self, value):
        '''Set memory best-effort protection.

        If memory usage of a VE is below this value, the VE's memory shouldn't
        be reclaimed on host pressure if memory can be reclaimed from
        unprotected VEs.
        '''
        pass

    def set_mem_target(self, value):
        '''Set memory allocation target.

        This function sets memory consumption target for a VE. Note, it does
        not necessarily mean that the VE memory usage will reach the target
        instantly or even any time soon - in fact, it may not reach it at all
        in case allocation is reduced. However, reducing the value will put the
        VE under heavy local memory pressure forcing it to release its memory
        to the host.
        '''
        pass

    def set_config(self, config):
        '''Set new config.
        '''
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
    if not name or '/' in name:
        raise VCMMDError(VCMMD_ERROR_INVALID_VE_NAME)


class VE(object):

    def __init__(self, ve_type, name, config):
        _check_ve_name(name)

        self._impl = _lookup_ve_impl(ve_type)
        self._obj = None

        self._logger = logging.getLogger('vcmmd.ve')

        self.name = name
        self.config = config
        self.stats = Stats()

        self.overhead = self._impl.estimate_overhead(name)

        # Policy private data. Can be used by a load manager policy to store
        # extra information per each VE (e.g. stat averages).
        self.policy_data = None

    def __str__(self):
        return "%s '%s'" % (get_ve_type_name(self.VE_TYPE), self.name)

    def _log(self, lvl, msg, *args, **kwargs):
        self._logger.log(lvl, str(self) + ': ' + msg, *args, **kwargs)

    @property
    def VE_TYPE(self):
        return self._impl.VE_TYPE

    @property
    def active(self):
        return self._obj is not None

    def activate(self):
        '''Try to mark VE active. Return True on success, False on failure.

        This function is supposed to be called after a VE switched to a running
        state. If it succeeds, the VE's state may be updated and its runtime
        memory parameters may be reconfigured.
        '''
        assert not self.active

        try:
            obj = self._impl(self.name)
            obj.set_config(self.config)
        except Error as err:
            self._log(logging.ERROR, 'Failed to activate: %s', err)
            return False

        self._obj = obj
        self._log(logging.INFO, 'Activated')
        return True

    def deactivate(self):
        '''Mark VE inactive.

        This function is supposed to be called before switching a VE to a state
        in which its runtime memory parameters cannot be changed any more (e.g.
        suspended or paused).
        '''
        assert self.active

        self._obj = None
        self._log(logging.INFO, 'Deactivated')

    def update(self):
        '''Update VE stats.
        '''
        assert self.active

        try:
            self.stats._update(**self._obj.get_stats())
        except Error as err:
            self._log(logging.ERROR, 'Failed to update stats: %s', err)
        else:
            if self._logger.isEnabledFor(logging.DEBUG):
                self._log(logging.DEBUG, 'Stats updated: %s', self.stats)

    @property
    def mem_min(self):
        '''Return min memory size required by this VE.

        For an active VE it simply returns configured guarantee plus overhead.
        However, for an inactive VE the result is biased above the RSS, because
        its allocation cannot be tuned any more.
        '''
        val = self.config.guarantee + self.overhead
        if not self.active:
            val = max(val, self.stats.rss)
        return val

    def set_mem(self, target, protection):
        '''Set VE memory consumption target.
        '''
        assert self.active

        try:
            self._obj.set_mem_target(target)
            self._obj.set_mem_protection(protection)
        except Error as err:
            self._log(logging.ERROR, 'Failed to tune allocation: %s', err)
        else:
            self._log(logging.DEBUG, 'Allocation tuned: target:%d protection:%d',
                      target, protection)

    def set_config(self, config):
        '''Set VE config. Return True on success, False on failure.
        '''
        assert self.active

        try:
            self._obj.set_config(config)
        except Error as err:
            self._log(logging.ERROR, 'Failed to set config: %s', err)
            return False

        self.config = config
        self._log(logging.INFO, 'Config updated: %s', config)
        return True
