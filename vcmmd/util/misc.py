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

from itertools import chain
import json
import pprint
import psutil


def print_dict(d, j=False):
    if j:
        return json.dumps(d, sort_keys=True, indent=4)
    return pprint.pformat(d)


def roundup(v, t):
    return v if (v % t) == 0 else v + t - (v % t)


def clamp(v, l, h):
    return max(l, min(v, h))


def parse_range(rng):
    """Produce list of integers which fall in range described in input string."""
    if not rng or rng.isspace():
        return []
    parts = rng.split('-')
    if len(parts) > 2:
        raise ValueError("Bad range: '{}'".format(rng))
    parts = [int(i) for i in parts]
    start = parts[0]
    end = start if len(parts) == 1 else parts[1]
    if start > end:
        end, start = start, end
    return [i for i in range(start, end + 1)]


def parse_range_list(rngs):
    """Produce list of integers which fall in comma separated range description."""
    return sorted(set(chain(*[parse_range(rng) for rng in rngs.split(',')])))


def get_cs_num():
    """Get number of running vstorage CSes on the node."""
    cs_num = 0
    name = '/usr/bin/csd'
    for process in psutil.process_iter():
        try:
            cmd = process.cmdline()
            if cmd and cmd[0] == name:
                cs_num += 1
        except psutil.NoSuchProcess:
            pass
    return cs_num
