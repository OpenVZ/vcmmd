from __future__ import absolute_import

import logging

from vcmmd.error import (VCMMDError,
                         VCMMD_ERROR_INVALID_VE_NAME,
                         VCMMD_ERROR_INVALID_VE_TYPE,
                         VCMMD_ERROR_INVALID_VE_CONFIG,
                         VCMMD_ERROR_VE_ALREADY_ACTIVE,
                         VCMMD_ERROR_VE_NOT_ACTIVE,
                         VCMMD_ERROR_VE_OPERATION_FAILED)
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

    @staticmethod
    def mem_overhead():
        '''Return an estimate of memory overhead.

        This function is supposed to return the amount of memory beyond the
        configured limit which is required to run the VE smoothly. E.g. for
        VMs this should equal expected RSS of the emulator process.
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


def _check_ve_config(config):
    if not config.is_valid():
        raise VCMMDError(VCMMD_ERROR_INVALID_VE_CONFIG)


class VE(object):

    def __init__(self, ve_type, name, config):
        _check_ve_name(name)
        _check_ve_config(config)

        self._impl = _lookup_ve_impl(ve_type)
        self._obj = None

        self._logger = logging.getLogger('vcmmd.ve')

        self.name = name
        self.config = config
        self.stats = Stats()
        self.active = False
        self._overhead = self._impl.mem_overhead()

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

    def _get_obj(self):
        if self._obj is None:
            obj = self._impl(self.name)
            obj.set_config(self.config)
            self._obj = obj
        return self._obj

    def activate(self):
        '''Mark VE active.

        This function is supposed to be called after a VE switched to a state,
        in which its memory allocation can be tuned.
        '''
        if self.active:
            raise VCMMDError(VCMMD_ERROR_VE_ALREADY_ACTIVE)

        self.active = True
        self._log(logging.INFO, 'Activated')
        self.update_stats()

    def deactivate(self):
        '''Mark VE inactive.

        This function is supposed to be called before switching a VE to a state
        in which its runtime memory parameters cannot be changed any more (e.g.
        suspended or paused).
        '''
        if not self.active:
            raise VCMMDError(VCMMD_ERROR_VE_NOT_ACTIVE)

        # We need uptodate rss for inactive VEs - see VE.mem_min
        self.update_stats()

        self.active = False
        self._log(logging.INFO, 'Deactivated')

    def update_stats(self):
        '''Update VE stats.
        '''
        assert self.active

        try:
            obj = self._get_obj()
            self.stats._update(**obj.get_stats())
        except Error as err:
            self._log(logging.ERROR, 'Failed to update stats: %s', err)
        else:
            if self._logger.isEnabledFor(logging.DEBUG):
                self._log(logging.DEBUG, 'update_stats: %s', self.stats)

    @property
    def mem_overhead(self):
        return self._overhead + self.config.vram

    @property
    def mem_min(self):
        '''Return min memory size required by this VE.

        Normally, it simply returns configured guarantee plus overhead.
        However, for an inactive VE the result will never be less than RSS,
        because its allocation cannot be tuned any more.
        '''
        val = self.config.mem_min + self._overhead
        if not self.active:
            val = max(val, self.stats.rss)
        return val

    def set_mem(self, target, protection):
        '''Set VE memory consumption target.
        '''
        assert self.active

        try:
            obj = self._get_obj()
            obj.set_mem_target(target)
            obj.set_mem_protection(protection)
        except Error as err:
            self._log(logging.ERROR, 'Failed to tune allocation: %s', err)
        else:
            self._log(logging.DEBUG, 'set_mem: target:%d protection:%d',
                      target, protection)

    def set_config(self, config):
        '''Update VE config.
        '''
        _check_ve_config(config)

        if not self.active:
            raise VCMMDError(VCMMD_ERROR_VE_NOT_ACTIVE)

        try:
            obj = self._get_obj()
            obj.set_config(config)
        except Error as err:
            self._log(logging.ERROR, 'Failed to set config: %s', err)
            raise VCMMDError(VCMMD_ERROR_VE_OPERATION_FAILED)

        self.config = config
        self._log(logging.INFO, 'Config updated: %s', config)
