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

from __future__ import absolute_import
import re

from vcmmd.cgroup.base import Cgroup

class CpuCgroup(Cgroup):

    CONTROLLER = 'cpu'

    def get_cpu_stats(self):
        names = ["cpuuser", "cpunice", "cpusystem", "cpuidle"]
        stats = self._read_file_str("proc.stat")
        res = {}
        for line in stats.splitlines():
            if not re.search("cpu\d+", line):
                continue
            cpu, data = re.split(" ", line, maxsplit = 1)
            cpu = int(cpu[3:])
            for name, value in zip(names, re.findall("(\d+)", data)):
                if cpu not in res:
                    res[cpu] = {}
                res[cpu][name] = int(value)
        return res

    def get_nr_cpus(self):
        return self._read_file_int("nr_cpus")
