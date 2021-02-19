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

from __future__ import absolute_import

from vcmmd.cgroup.base import Cgroup


class BlkIOCgroup(Cgroup):

    CONTROLLER = 'blkio'

    def _get_io_stats(self, filename, keys):
        result = {k: 0 for k in keys}
        kv = self._read_file_kv(filename)
        for k, v in kv.iteritems():
            try:
                result[k.split()[-1]] += v
            except KeyError:
                continue
        return tuple(result[k] for k in keys)

    def get_io_serviced(self):
        '''Return a tuple containing the total number of read and write
        requests issued by this cgroup.
        '''
        return self._get_io_stats('io_serviced', ('Read', 'Write'))

    def get_io_service_bytes(self):
        '''Return a tuple containing the total number of bytes read and written
        by this cgroup.
        '''
        return self._get_io_stats('io_service_bytes', ('Read', 'Write'))
