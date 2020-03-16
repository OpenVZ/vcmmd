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

from vcmmd.cgroup.base import Cgroup
from vcmmd.util.misc import parse_range_list

class CpuSetCgroup(Cgroup):

    CONTROLLER = 'cpuset'

    def get_cpu_list(self):
        return parse_range_list(self._read_file_str("cpus"))

    def get_node_list(self):
        return parse_range_list(self._read_file_str("mems"))

    def set_memory_migrate(self, val):
        self._write_file_str("memory_migrate", str(int(val)))

    def set_cpu_list(self, cpus):
        self._write_file_str("cpus",",".join(map(str,cpus)))

    def set_node_list(self, nodes):
        self._write_file_str("mems", ",".join(map(str, nodes)))
