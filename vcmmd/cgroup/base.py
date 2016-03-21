from __future__ import absolute_import

import os.path


class Cgroup(object):

    _CGROUP_DIR = '/sys/fs/cgroup'

    CONTROLLER = None

    def __init__(self, path):
        path = path.strip('/')
        self._path = '/' + path
        self._abs_path = '/'.join([self._CGROUP_DIR, self.CONTROLLER, path])
        self._file_fmt = '/'.join([self._abs_path, '%s.%%s' % self.CONTROLLER])

    def _file_path(self, name):
        return self._file_fmt % name

    def exists(self):
        return os.path.isdir(self._abs_path)

    def _read_file_str(self, filename):
        with open(self._file_path(filename), 'r') as f:
            return f.read()

    def _write_file_str(self, filename, val):
        with open(self._file_path(filename), 'w') as f:
            f.write(val)

    def _read_file_int(self, filename):
        return int(self._read_file_str(filename))

    def _write_file_int(self, filename, val):
        self._write_file_str(filename, str(val))

    def _read_file_kv(self, filename):
        kv = {}
        with open(self._file_path(filename), 'r') as f:
            for l in f.readlines():
                k, v = l.rsplit(' ', 1)
                kv[k] = int(v)
        return kv
