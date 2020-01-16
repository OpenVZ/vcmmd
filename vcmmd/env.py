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

import logging
from abc import ABCMeta, abstractmethod


class Env:
    __metaclass__ = ABCMeta

    @abstractmethod
    def update_stats(self):
        pass

    @abstractmethod
    def get_cpu_stats(self):
        pass

    @abstractmethod
    def get_numa_stats(self):
        pass

    def __init__(self, name):
        self.__logger = logging.getLogger(name)

    def __log(self, lvl, msg, *args, **kwargs):
        self.__logger.log(lvl, str(self) + ': ' + msg, *args, **kwargs)

    def log_err(self, *args, **kwargs):
        self.__log(logging.ERROR, *args, **kwargs)

    def log_info(self, *args, **kwargs):
        self.__log(logging.INFO, *args, **kwargs)

    def log_debug(self, *args, **kwargs):
        # Debugging is unlikely to be enabled.
        # Avoid evaluating args if it is not.
        if self.__logger.isEnabledFor(logging.DEBUG):
            self.__log(logging.DEBUG, *args, **kwargs)
