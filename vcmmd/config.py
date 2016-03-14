import json
import logging

from vcmmd.util.singleton import Singleton


class VCMMDConfig(object):
    '''VCMMD config loader.

    This is a singleton class that provides methods for loading VCMMD
    configuration from a file and getting option values by name.
    '''

    __metaclass__ = Singleton

    def __init__(self):
        self.logger = logging.getLogger('vcmmd.config')
        self._data = None
        self._cache = {}

    def load(self, filename):
        '''Load config from a file.

        The file must be in json format.
        '''
        self._data = None
        self._cache = {}

        self.logger.info("Loading config from file '%s'", filename)
        try:
            with open(filename, 'r') as f:
                self._data = json.load(f)
        except IOError as err:
            self.logger.error('Error reading config file: %s', err)
        except ValueError as err:
            self.logger.error('Error parsing config file: %s', err)

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
        self.logger.info('%s = %r', name, val)
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
