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

VCMMD_ERROR_SUCCESS = 0
VCMMD_ERROR_INVALID_VE_NAME = 1
VCMMD_ERROR_INVALID_VE_TYPE = 2
VCMMD_ERROR_INVALID_VE_CONFIG = 3
VCMMD_ERROR_VE_NAME_ALREADY_IN_USE = 4
VCMMD_ERROR_VE_NOT_REGISTERED = 5
VCMMD_ERROR_VE_ALREADY_ACTIVE = 6
VCMMD_ERROR_VE_OPERATION_FAILED = 7
VCMMD_ERROR_UNABLE_APPLY_VE_GUARANTEE = 8
VCMMD_ERROR_VE_NOT_ACTIVE = 9
VCMMD_ERROR_TOO_MANY_REQUESTS = 10
VCMMD_ERROR_POLICY_SET_ACTIVE_VES = 11
VCMMD_ERROR_POLICY_SET_INVALID_NAME = 12


_ERRSTR = {
    0: 'Success',
    1: 'Invalid VE name',
    2: 'Invalid VE type',
    3: 'Invalid VE configuration',
    4: 'VE name already in use',
    5: 'VE not registered',
    6: 'VE already active',
    7: 'VE operation failed',
    8: 'Unable to apply VE guarantee',
    9: 'VE not active',
    10: 'Too many requests',
    11: 'Set policy failed(you have to shutdown all VEs before policy switching)',
    12: 'Set policy failed(policy name is invalid)',
}


class VCMMDError(Exception):

    def __init__(self, errno):
        self.errno = errno

    def __str__(self):
        return _ERRSTR.get(self.errno, 'Unknown error')
