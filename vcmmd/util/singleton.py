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

class Singleton(type):
    '''Singleton metaclass.

    Usage:

    class MySingleton(metaclass=Singleton):
        ...
    '''

    _instances = {}

    def __call__(cls, *args, **kwargs):
        try:
            inst = cls._instances[cls]
        except KeyError:
            inst = super(Singleton, cls).__call__(*args, **kwargs)
            cls._instances[cls] = inst
        return inst
