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

    def load(self, filename):
        '''Load config from a file.

        The file must be in json format.
        '''

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

    def get(self, name, default=None):
        '''Get the value of a config option.

        This function lookups a config option by name and returns its value.
        To lookup an option in a sub-section, use dot, e.g. 'section.option'.
        In case the option does not exist, the value of 'default' is returned.

        This function does not perform any type checks. To assure the value is
        of a specific type, use get_str, get_bool, and get_num methods.
        '''
        try:
            return self._get(name)
        except KeyError:
            return default

    def get_str(self, name, default=None):
        try:
            val = self._get(name)
        except KeyError:
            return default

        t = type(val)
        if t not in (str, unicode):
            self.logger.warn("Invalid value for config option '%s': "
                             "Expected string, got '%s'", name, t.__name__)
            return default

        return val

    def get_bool(self, name, default=None):
        try:
            val = self._get(name)
        except KeyError:
            return default

        t = type(val)
        if t != bool:
            self.logger.warn("Invalid value for config option '%s': "
                             "Expected boolean, got '%s'", name, t.__name__)
            return default

        return val

    def get_num(self, name, default=None,
                integer=False, minimum=None, maximum=None):
        try:
            val = self._get(name)
        except KeyError:
            return default

        t = type(val)
        if t not in (int, long, float) or (integer and t == float):
            self.logger.warn("Invalid value for config option '%s': "
                             "Expected %s, got '%s'", name,
                             'integer' if integer else 'number', t.__name__)
            return default

        if minimum is not None and val < minimum:
            self.logger.warn("Config option '%s' must be >= %s, got %s",
                             name, minimum, val)
            return default

        if maximum is not None and val > maximum:
            self.logger.warn("Config option '%s' must be <= %s, got %s",
                             name, maximum, val)
            return default

        return val
