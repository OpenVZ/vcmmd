# Copyright (c) 2016-2017, Parallels International GmbH
# Copyright (c) 2017-2020, Virtuozzo International GmbH, All rights reserved
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

from __future__ import absolute_import

import os.path


class Cgroup(object):

    _CGROUP_DIR = '/sys/fs/cgroup'

    CONTROLLER = None

    def __init__(self, path):
        path = path.strip('/')
        self.path = '/' + path
        self.abs_path = '/'.join([self._CGROUP_DIR, self.CONTROLLER, path])
        self._file_fmt = '/'.join([self.abs_path, '%s.%%s' % self.CONTROLLER])

    def _file_path(self, name):
        return self._file_fmt % name

    def exists(self):
        return os.path.isdir(self.abs_path)

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


def pid_cgroup(pid):
    '''Get the cgroup which 'pid' is attached to.

    Returns a dictionary mapping cgroup subsystem name to the relative path of
    the cgroup which 'pid' is attached to.
    '''
    cgroup = {}
    with open('/proc/%d/cgroup' % pid) as f:
        for l in f.read().splitlines():
            _, subsys, path = l.split(':')
            subsys = subsys.split(',')
            for s in subsys:
                cgroup[s] = path
    return cgroup
