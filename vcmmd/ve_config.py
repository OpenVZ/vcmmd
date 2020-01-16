# Copyright (c) 2016-2017, Parallels International GmbH
# Copyright (c) 2017-2019, Virtuozzo International GmbH, All rights reserved
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
# Our contact details: Virtuozzo International GmbH, Vordergasse 59, 8200
# Schaffhausen, Switzerland.

from vcmmd.util.limits import UINT64_MAX
from vcmmd.util.misc import parse_range_list


_VEConfigFields = [     # tag
    'guarantee',        # 0
    'limit',            # 1
    'swap',             # 2
    'vram',             # 3
    'nodelist',         # 4
    'cpulist',          # 5
    'guarantee_type',   # 6
]

_VEConfigFields_string = [
    'nodelist',         # 4
    'cpulist',          # 5
]

VCMMD_MEMGUARANTEE_AUTO = 0
VCMMD_MEMGUARANTEE_PERCENTS = 1

VCMMD_EMPTY_MASK = ''


class VEConfig:
    '''Represents a VE's memory configuration.

    guarantee:      VE memory guarantee

                    A VE should be always given at least as much memory as
                    specified by this parameter.

    limit:          VE memory limit

                    Maximal size of host memory that can be used by a VE.
                    Must be >= guarantee.

    swap:           VE swap limit

                    Maximal size of host swap that can be used by a VE.

    vram:           Video RAM size

                    Amount of memory that should be reserved for a VE's
                    graphic card.

    nodelist:       NUMA node list

                    Bitmask of NUMA nodes on the physical server to use for
                    executing the virtual environment process.

    cpulist:        CPU list

                    Bitmask of CPUs on the physical server to use for executing
                    the virtual environment process.

    guarantee_type: Default ve memory guarantee type "auto" or in percent.

    Every field is tagged as follows:

    guarantee:      0
    limit:          1
    swap:           2
    vram:           3
    nodelist:       4
    cpulist:        5
    guarantee_type: 6

    The tags are used for converting the config to a tuple/array and back.
    '''

    def __init__(self, **kv):
        self._kv = {}
        for k, v in kv.items():
            if k not in _VEConfigFields:
                raise TypeError("unexpected keyword argument '%s'" % k)
            if k not in _VEConfigFields_string:
                self._kv[str(k)] = int(v)
            else:
                if k in ['nodelist', 'cpulist']:
                    self._kv[str(k)] = parse_range_list(str(v))
                    continue
                self._kv[str(k)] = str(v)

    def __getattr__(self, name):
        try:
            return self._kv[name]
        except KeyError:
            raise AttributeError

    def __str__(self):
        return ' '.join('%s:%s' % (k, self._kv[k])
                        for k in _VEConfigFields if k in self._kv)

    @property
    def mem_min(self):
        '''The minimal amount of memory required by this configuration.
        '''
        return self.guarantee + self.vram

    def is_valid(self):
        '''Check that the config has all fields initialized and its values pass
        all sanity checks.
        '''
        return (set(self._kv) == set(_VEConfigFields) and
                self.guarantee <= self.limit)

    def complete(self, config):
        '''Initialize absent fields with values from a given config.
        '''
        for k, v in config._kv.items():
            if k not in self._kv:
                self._kv[k] = v

    def as_array(self):
        '''Convert to an array of (tag, value, string) turples.
        '''
        arr = []
        for tag, name in zip(xrange(len(_VEConfigFields)), _VEConfigFields):
            try:
                if name in _VEConfigFields_string:
                    val = 0
                    if isinstance(self._kv[name], list):
                        string = ",".join(map(str,self._kv[name]))
                    else:
                        string = str(self._kv[name])
                else:
                    val = self._kv[name]
                    string = ""
            except KeyError:
                continue
            arr.append((tag, val, string))
        return arr

    @staticmethod
    def from_array(arr):
        '''Make a config from an array of (tag, value, string) turples. Unknown
        tags are silently ignored.
        '''
        kv = {}
        for tag, val, string in arr:
            try:
                name = _VEConfigFields[tag]
            except IndexError:
                continue
            if name in _VEConfigFields_string:
                kv[name] = str(string)
            else:
                kv[name] = int(val)
        return VEConfig(**kv)

    def update(self, guarantee):
        self._kv['guarantee'] = guarantee


DefaultVEConfig = VEConfig(guarantee=0,
                           limit=UINT64_MAX,
                           swap=UINT64_MAX,
                           vram=0,
                           nodelist="",
                           cpulist="",
                           guarantee_type=VCMMD_MEMGUARANTEE_AUTO)
