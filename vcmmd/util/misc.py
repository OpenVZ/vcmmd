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
from itertools import chain

def roundup(v, t):
    return v if (v % t) == 0 else v + t - (v % t)


def clamp(v, l, h):
    return max(l, min(v, h))


def sorted_by_val(d):
    return sorted(d, key=lambda k: d[k])

def parse_range(rng):
    '''Function produces list of integers which fall in range described in input string
    i.e. "1-9" -> [1,2,3,4,5,6,7,8,9] or "1" -> [1]
    '''
    parts = rng.split('-')
    if 1 > len(parts) > 2:
        raise ValueError("Bad range: '%s'" % (rng,))
    parts = [int(i) for i in parts]
    start = parts[0]
    end = start if len(parts) == 1 else parts[1]
    if start > end:
        end, start = start, end
    return range(start, end + 1)

def parse_range_list(rngs):
    '''Function produces list of integers which fall in commaseparated range description
    i.e. "1-3,5,4-8,9" -> [1,2,3,4,5,6,7,8,9]
    '''
    return sorted(set(chain(*[parse_range(rng) for rng in rngs.split(',')])))
