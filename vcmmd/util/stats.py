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

from __future__ import absolute_import

import time


class Stats(object):

    ABSOLUTE_STATS = []

    CUMULATIVE_STATS = []

    ALL_STATS = ABSOLUTE_STATS + CUMULATIVE_STATS

    def __init__(self):
        self._stats = {k: -1 for k in self.ALL_STATS}
        self._raw_stats = {}
        self._last_update = 0

    def __getattr__(self, name):
        try:
            return self._stats[name]
        except KeyError:
            raise AttributeError

    def __str__(self):
        return ' '.join('%s:%d' % (k, self._stats[k]) for k in self.ALL_STATS)

    def _update(self, **stats):
        prev_stats = self._raw_stats
        self._raw_stats = stats

        for k in self.ABSOLUTE_STATS:
            v = stats.get(k, -1)
            if v < 0:  # stat unavailable => return -1
                v = -1
            self._stats[k] = v

        now = time.time()
        delta_t = now - self._last_update
        self._last_update = now

        for k in self.CUMULATIVE_STATS:
            cur, prev = stats.get(k, -1), prev_stats.get(k, -1)
            if cur < 0 or prev < 0:  # stat unavailable => return -1
                delta = -1
            else:
                delta = int((cur - prev) / delta_t)
            self._stats[k] = delta

    def report(self):
        return [(s, self._stats[s]) for s in self.ALL_STATS]
