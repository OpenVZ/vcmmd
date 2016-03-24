from __future__ import absolute_import

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
        return '(guarantee=%s, limit=%s, swap=%s)' % self

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

    def get_mem_overhead(self):
        '''Return memory overhead.

        This function is supposed to return the amount of memory beyond the
        configured limit which is required to run the VE smoothly. For VMs this
        will be VRAM size plus emulator process RSS.
        '''
        pass

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

        self.name = name
        self.config = config

        self.mem_overhead = 0
        self.mem_stats = MemStats()
        self.io_stats = IOStats()

        self.policy_priv = None

    def __str__(self):
        return "%s '%s'" % (self.VE_TYPE_NAME, self.name)

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
        '''Activate VE.

        This function marks a VE as active. It also tries to apply the VE
        config. The latter may fail hence this function may throw Error.

        This function is supposed to be called after a VE has been started or
        resumed.
        '''
        obj = self._impl(self.name)
        obj.set_config(self.config)
        self._obj = obj

    def deactivate(self):
        '''Deactivate VE.

        This function marks a VE as inactive. It never raises an exception.

        This function is supposed to be called before pausing or suspending a
        VE.
        '''
        self._obj = None

    def update(self):
        '''Update VE state.
        '''
        self.mem_overhead = self._obj.get_mem_overhead()
        self.mem_stats._update(**self._obj.get_mem_stats())
        self.io_stats._update(**self._obj.get_io_stats())

    def set_mem(self, target, protection):
        '''Set VE memory consumption target.
        '''
        self._obj.set_mem_target(target)
        self._obj.set_mem_protection(protection)

    def set_config(self, config):
        '''Set VE config.
        '''
        self._obj.set_config(config)
        self.config = config
