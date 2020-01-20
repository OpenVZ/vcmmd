# Copyright (c) 2016-2017, Parallels International GmbH
# Copyright (c) 2017-2021, Virtuozzo International GmbH, All rights reserved
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

import json
import logging
import os
import copy

from vcmmd.util.singleton import Singleton


class VCMMDConfig(metaclass=Singleton):
    '''VCMMD config loader.

    This is a singleton class that provides methods for loading VCMMD
    configuration from a file and getting option values by name.
    '''

    def __init__(self, filename):
        self.logger = logging.getLogger('vcmmd.config')
        self._filename = filename
        self._data = None
        self._cache = {}

    def load(self):
        '''Load config from a file.

        The file must be in json format.
        '''
        self._data = None
        self._cache = {}

        self.logger.info("Loading config from file '%s'", self._filename)
        self._data = self.read()

    def _get_legacy_storage_limits(self):
        if os.path.isfile('/etc/vz/vstorage-limits.conf'):
            try:
                with open('/etc/vz/vstorage-limits.conf') as f:
                    return json.load(f)
            except (IOError, ValueError) as err:
                self.logger.error('Error reading vstorage-limits.conf: %s', err)
        return {}

    def read(self):
        '''Read config from a file.

        The file must be in json format.
        '''
        try:
            with open(self._filename, 'r') as f:
                data = json.load(f)
        except IOError as err:
            self.logger.error('Error reading config file: %s', err)
            return None
        except ValueError as err:
            self.logger.error('Error parsing config file: %s', err)
            return None
        limits_config = self._get_default_limits_config()
        if 'Limits' in data:
            limits_config = self._update(limits_config, data['Limits'])
        limits_config = self._update(limits_config, self._get_legacy_storage_limits())
        data['Limits'] = limits_config
        return data

    def dump(self, name, val):
        self._data = self.read()

        try:
            _data = self._data
            key = name.split('.')[-1]
            path = name.split('.')[:-1]
            for k in path:
                _data = _data[k]
            _data[key] = val
        except KeyError as err:
            self.logger.error('Error parsing config file: %s', err)
            return

        try:
            with open(self._filename, 'w') as f:
                f.write(json.dumps(self._data, indent=8))
        except IOError as err:
            self.logger.error('Error writing config file: %s', err)

        self._cache[name] = val

    @classmethod
    def _update(cls, a_dict, b_dict):
        """
        Update a_dict with b_dict values hierarchically.
        """
        for k, v in b_dict.items():
            if isinstance(v, dict):
                a_dict[k] = cls._update(a_dict.get(k, {}), v)
            else:
                a_dict[k] = v
        return a_dict

    @classmethod
    def _exclude(cls, a_dict, b_dict):
        """
        Remove from a_dict items storing under the same hierarchy in b_dict and having the same value.
        """
        for k, v in a_dict.items():
            if k in b_dict:
                if isinstance(b_dict[k], dict) and isinstance(a_dict[k], dict):
                    cls._exclude(a_dict[k], b_dict[k])
                    if not a_dict[k]:
                        del a_dict[k]
                else:
                    if v == b_dict[k]:
                        del a_dict[k]

    @staticmethod
    def _get_default_limits_config():
        def _expand(min, max, share):
            return {"Min": min, "Max": max, "Share": share}

        return {
            "System": {
                "Path": "system.slice",
                "Limit": _expand(0, -1, 1),
                "Guarantee": _expand(320 << 20, 700 << 20, 0.04),
                "Swap": _expand(0, 0, 0),
            },
            "User": {
                "Path": "user.slice",
                "Limit": _expand(0, -1, 1),
                "Guarantee": _expand(32 << 20, 128 << 20, 0.02),
                "Swap": _expand(0, 0, 0),
            },
            "VStorage": {
                "Path": "vstorage.slice/vstorage-services.slice",
                "Limit": _expand(0, -1, 0.7),
                "Guarantee": _expand(0, 0, 0.25),
                "Swap": _expand(0, 0, 0),
            },
        }

    def _get(self, name):
        d = self._data
        for k in name.split('.'):
            if not isinstance(d, dict) or k not in d:
                raise KeyError
            d = d[k]
        return d

    def get(self, name, default=None, checkfn=None):
        '''Get the value of a config option.

        This function lookups a config option by name and returns its value.
        To lookup an option in a sub-section, use dot, e.g. 'section.option'.
        In case the option does not exist, the value of 'default' is returned.

        Unless 'checkfn' is not None, no checks is performed upon the retrieved
        value. To assure the value meets specific requirements, use get_str,
        get_bool, and get_num methods.

        If 'checkfn' argument is not None, it must be a function taking exactly
        one argument. The function will be called to check the retrieved value.
        It may raise ValueError or TypeError, in which case the retrieved value
        will be discarded and 'default' will be returned.

        Note, the value returned by this function is cached, meaning that the
        next call to it with the same 'name' will return the same value
        bypassing any checks.
        '''
        # First, check if we've already fetched the requested value. If this is
        # the case, bypass any checks and return the cached value.
        # TODO: replace caching with @lru_cache after upgrading to Py3
        try:
            return self._cache[name]
        except KeyError:
            pass
        try:
            val = self._get(name)
            if checkfn is not None:
                checkfn(val)
        except (KeyError, TypeError, ValueError) as err:
            # do not complain if the option is absent
            if not isinstance(err, KeyError):
                self.logger.warn("Invalid value for config option '%s': %s",
                                 name, err)
            val = default
        # Save the value to speed up following retrievals and avoid spewing
        # warnings if any over and over again.
        self._cache[name] = val
        self.logger.debug('%s = %r', name, val)
        return val

    def get_str(self, name, default=None):
        def checkfn(val):
            t = type(val)
            if t != str:
                raise TypeError("expected string, got '{}'".format(t.__name__))
        return self.get(name, default, checkfn)

    def get_bool(self, name, default=None):
        def checkfn(val):
            t = type(val)
            if t != bool:
                raise TypeError("expected boolean, got '{}'".format(t.__name__))
        return self.get(name, default, checkfn)

    def get_num(self, name, default=None,
                integer=False, minimum=None, maximum=None):
        def checkfn(val):
            t = type(val)
            if t not in (int, float) or (integer and t == float):
                raise TypeError("expected {}, got "
                                "'{}'".format('integer' if integer else 'number',
                                              t.__name__))
            if minimum is not None and val < minimum:
                raise ValueError("must be >= {}, got {}".format(minimum, val))
            if maximum is not None and val > maximum:
                raise ValueError("must be <= {}, got {}".format(maximum, val))
        return self.get(name, default, checkfn)

    def get_choice(self, name, choices, default=None):
        def checkfn(val):
            t = type(val)
            if t != str:
                raise TypeError("expected string, got '{}'".format(t.__name__))
            if val not in choices:
                raise ValueError("must be one of {}, got "
                                 "{}".format(tuple(choices), str(val)))
        return self.get(name, default, checkfn)

    def _make_config(self):
        cfg_dict = {}
        for name in self._cache:
            x = cfg_dict
            path = name.split('.')
            key = path[-1]
            path = path[:-1]
            for section in path:
                if section not in x:
                    x[section] = {}
                x = x[section]
            x[key] = self._cache[name]
        return cfg_dict

    def report(self, full_config=False):
        config = copy.deepcopy(self._make_config())
        if not full_config and 'Limits' in config:
            default_limits = self._get_default_limits_config()
            self._exclude(config['Limits'], default_limits)
            if not config['Limits']:
                del config['Limits']
        return json.dumps(config)
