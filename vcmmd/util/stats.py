# Copyright (c) 2016-2017, Parallels International GmbH
# Copyright (c) 2017-2024, Virtuozzo International GmbH, All rights reserved
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

import time


class Stats:

    ABSOLUTE_STATS = []

    CUMULATIVE_STATS = []

    def __init__(self):
        self.ALL_STATS = self.ABSOLUTE_STATS + self.CUMULATIVE_STATS
        self.__stats = {k: -1 for k in self.ALL_STATS}
        self.__raw_stats = {}
        self.__last_update = 0

    def __getattr__(self, name):
        try:
            return self.__stats[name]
        except KeyError:
            raise AttributeError

    def __str__(self):
        return str(self.__stats)

    def _update(self, **stats):
        prev_stats = self.__raw_stats
        self.__raw_stats = stats
        __stats = {}

        for k in self.ABSOLUTE_STATS:
            v = stats.get(k, -1)
            if v < 0:  # stat unavailable => return -1
                v = -1
            __stats[k] = v

        now = time.time()
        self.delta_t = now - self.__last_update
        self.__last_update = now

        for k in self.CUMULATIVE_STATS:
            cur, prev = stats.get(k, -1), prev_stats.get(k, -1)
            if cur < 0 or prev < 0:  # stat unavailable => return -1
                delta = -1
            else:
                delta = int((cur - prev) / self.delta_t)
            __stats[k] = delta
        # stats update should be thread-safe
        self.__stats = __stats

    def report(self):
        return self.__stats.copy()
