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

VE_TYPE_CT = 0
VE_TYPE_VM = 1
VE_TYPE_VM_LINUX = 2
VE_TYPE_VM_WINDOWS = 3
VE_TYPE_SERVICE = 4

_TYPE_NAME = {
    VE_TYPE_CT: 'CT',
    VE_TYPE_VM: 'VM',
    VE_TYPE_VM_LINUX: 'VM_LIN',
    VE_TYPE_VM_WINDOWS: 'VM_WIN',
    VE_TYPE_SERVICE: 'SRVC',
}

_NAME_TYPE = {v: k for k, v in _TYPE_NAME.items()}


def get_ve_type_name(t):
    return _TYPE_NAME[t]


def lookup_ve_type_by_name(s):
    return _NAME_TYPE[s]


def get_all_ve_type_names():
    return sorted(_NAME_TYPE.keys())
