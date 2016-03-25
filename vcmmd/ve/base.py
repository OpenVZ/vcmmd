from __future__ import absolute_import

import logging
import psutil
from collections import namedtuple

from vcmmd.config import VCMMDConfig
from vcmmd.cgroup import MemoryCgroup
from vcmmd.ve.stats import MemStats, IOStats
from vcmmd.util.limits import UINT64_MAX

_MAX_EFFECTIVE_LIMIT = psutil.virtual_memory().total

_CONFIG_FIELDS = (
    'guarantee',
    'limit',
    'swap',
)


class Error(Exception):
    pass


class InvalidVENameError(Error):
    pass


class InvalidVETypeError(Error):
    pass


class Config(namedtuple('Config', _CONFIG_FIELDS)):
    '''Represents a VE's memory configuration.

    guarantee:      VE memory guarantee

                    A VE should be always given at least as much memory as
                    specified by this parameter.

    limit:          VE memory limit

                    Maximal size of host memory that can be used by a VE.
                    Must be >= guarantee.

    swap:           VE swap limit

                    Maximal size of host swap that can be used by a VE.

    All values are in bytes.
    '''

    def __init__(self, *args, **kwargs):
        super(Config, self).__init__(*args, **kwargs)

        if self.guarantee > self.limit:
            raise ValueError('guarantee must be <= limit')

    def __str__(self):
        return 'guar:%s limit:%s swap:%s' % self

    @property
    def effective_limit(self):
        return min(self.limit, _MAX_EFFECTIVE_LIMIT)

    @staticmethod
    def from_dict(dict_, default=None):
        '''Make a Config from a dict.

        Some fields may be omitted in which case values given in 'default' will
        be used. 'default' must be an instance of a Config or None. If it is
        None, global default values will be used for omitted fields.
        '''
        if default is None:
            default = DEFAULT_CONFIG
        kv = default._asdict()
        kv.update(dict_)
        return Config(**kv)

DEFAULT_CONFIG = Config(guarantee=0,
                        limit=UINT64_MAX,
                        swap=UINT64_MAX)


class VEImpl(object):
    '''VE implementation.

    This class defines the interface to an underlying VE implementation
    (such as libvirt or cgroup).

    Any of the functions defined by this interface may raise Error.
    '''

    VE_TYPE = -1
    VE_TYPE_NAME = 'VE'

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

    def get_mem_stats(self):
        '''Return memory stats dict {name: value}.
        '''
        pass

    def get_io_stats(self):
        '''Return io stats dict {name: value}.
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
        raise InvalidVETypeError


def _check_ve_name(name):
    if not name or '/' in name:
        raise InvalidVENameError


class VE(object):

    def __init__(self, ve_type, name, config):
        _check_ve_name(name)

        self._impl = _lookup_ve_impl(ve_type)
        self._obj = None

        self._logger = logging.getLogger('vcmmd.ve')

        self.name = name
        self.config = config

        self.overhead = self._impl.estimate_overhead(name)

        self.mem_stats = MemStats()
        self.io_stats = IOStats()

        # Policy private data. Can be used by a load manager policy to store
        # extra information per each VE (e.g. stat averages).
        self.policy_data = None

    def __str__(self):
        return "%s '%s'" % (self.VE_TYPE_NAME, self.name)

    def _log(self, lvl, msg, *args, **kwargs):
        self._logger.log(lvl, str(self) + ': ' + msg, *args, **kwargs)

    @property
    def VE_TYPE(self):
        return self._impl.VE_TYPE

    @property
    def VE_TYPE_NAME(self):
        return self._impl.VE_TYPE_NAME

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
            self.mem_stats._update(**self._obj.get_mem_stats())
            self.io_stats._update(**self._obj.get_io_stats())
        except Error as err:
            self._log(logging.ERROR, 'Failed to update stats: %s', err)
        else:
            if self._logger.isEnabledFor(logging.DEBUG):
                self._log(logging.DEBUG, 'Stats updated: %s %s',
                          self.mem_stats, self.io_stats)

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
