from __future__ import absolute_import

import psutil

from vcmmd.util.limits import UINT64_MAX


_MAX_EFFECTIVE_LIMIT = psutil.virtual_memory().total


_VEConfigFields = [     # tag
    'guarantee',        # 0
    'limit',            # 1
    'swap',             # 2
]


class VEConfig(object):
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

    Every field is tagged as follows:

    guarantee:      0
    limit:          1
    swap:           2

    The tags are used for converting the config to a tuple/array and back.
    '''

    def __init__(self, **kv):
        self._kv = {}
        for k, v in kv.iteritems():
            if k not in _VEConfigFields:
                raise TypeError("unexpected keyword argument '%s'" % k)
            self._kv[str(k)] = int(v)

    def __getattr__(self, name):
        try:
            return self._kv[name]
        except KeyError:
            raise AttributeError

    def __str__(self):
        return ' '.join('%s:%d' % (k, self._kv[k])
                        for k in _VEConfigFields if k in self._kv)

    @property
    def effective_limit(self):
        return min(self.limit, _MAX_EFFECTIVE_LIMIT)

    def is_valid(self):
        '''Check that the config has all fields initialized and its values pass
        all sanity checks.
        '''
        return (set(self._kv) == set(_VEConfigFields) and
                self.guarantee <= self.limit)

    def complete(self, config):
        '''Initialize absent fields with values from a given config.
        '''
        for k, v in config._kv.iteritems():
            if k not in self._kv:
                self._kv[k] = v

    def as_dict(self):
        '''Convert to a dictionary.
        '''
        return dict(self._kv)

    def as_tuple(self):
        '''Convert to a tuple. Values in the tuple are ordered by tag. If a
        field is absent, the corresponding element of the tuple will be set
        to None.
        '''
        return tuple(self._kv.get(k, None) for k in _VEConfigFields)

    @staticmethod
    def from_tuple(tupl):
        '''Make a config from a tuple. Tuple indices are taken for config tags.
        If the tuple's length is less than the number of tags, missing fields
        will be left unset. If the tuple's length is greater than the number of
        tags, the superfluous values will be silently ignored.
        '''
        return VEConfig(**dict(zip(_VEConfigFields, tupl)))

    def as_array(self):
        '''Convert to an array of (tag, value) pairs.
        '''
        arr = []
        for tag, name in zip(xrange(len(_VEConfigFields)), _VEConfigFields):
            try:
                val = self._kv[name]
            except KeyError:
                continue
            arr.append((tag, val))
        return arr

    @staticmethod
    def from_array(arr):
        '''Make a config from an array of (tag, value) pairs. Unknown tags are
        silently ignored.
        '''
        kv = {}
        for tag, val in arr:
            try:
                name = _VEConfigFields[tag]
            except IndexError:
                continue
            kv[name] = val
        return VEConfig(**kv)


DefaultVEConfig = VEConfig(guarantee=0,
                           limit=UINT64_MAX,
                           swap=UINT64_MAX)
