# Copyright (c) 2016 Parallels IP Holdings GmbH
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
# Our contact details: Parallels IP Holdings GmbH, Vordergasse 59, 8200
# Schaffhausen, Switzerland.

import json
import logging
import pprint

from vcmmd.util.singleton import Singleton
from vcmmd.util.misc import print_dict


class VCMMDConfig(object):
    '''VCMMD config loader.

    This is a singleton class that provides methods for loading VCMMD
    configuration from a file and getting option values by name.
    '''

    __metaclass__ = Singleton

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

    def read(self):
        '''Read config from a file.

        The file must be in json format.
        '''
        try:
            with open(self._filename, 'r') as f:
                return json.load(f)
        except IOError as err:
            self.logger.error('Error reading config file: %s', err)
        except ValueError as err:
            self.logger.error('Error parsing config file: %s', err)

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
            if t not in (str, unicode):
                raise TypeError("expected string, got '%s'" % t.__name__)
        return self.get(name, default, checkfn)

    def get_bool(self, name, default=None):
        def checkfn(val):
            t = type(val)
            if t != bool:
                raise TypeError("expected boolean, got '%s'" % t.__name__)
        return self.get(name, default, checkfn)

    def get_num(self, name, default=None,
                integer=False, minimum=None, maximum=None):
        def checkfn(val):
            t = type(val)
            if t not in (int, long, float) or (integer and t == float):
                raise TypeError("expected %s, got '%s'" %
                                ('integer' if integer
                                 else 'number', t.__name__))
            if minimum is not None and val < minimum:
                raise ValueError("must be >= %s, got %s" % (minimum, val))
            if maximum is not None and val > maximum:
                raise ValueError("must be <= %s, got %s" % (maximum, val))
        return self.get(name, default, checkfn)

    def get_choice(self, name, choices, default=None):
        def checkfn(val):
            t = type(val)
            if t not in (str, unicode):
                raise TypeError("expected string, got '%s'" % t.__name__)
            if val not in choices:
                raise ValueError("must be one of %s, got %r" %
                                 (tuple(choices), str(val)))
        return self.get(name, default, checkfn)

    def report(self, j=False):
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

        return print_dict(cfg_dict, j)
