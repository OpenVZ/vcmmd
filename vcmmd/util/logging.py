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

# hardcode logging constants to avoid importing the logging module
LOG_LEVELS = {
    'debug': 10,
    'info': 20,
    'warn': 30,
    'error': 40,
    'critical': 50,
}


class LoggerWriter:
    '''Helper for redirecting stdout/stderr to a logger.

    Usage example:

    logger = logging.getLogger()
    sys.stdout = LoggerWriter(logger, logging.INFO)
    sys.stderr = LoggerWriter(logger, logging.CRITICAL)
    '''

    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
        self._buf = ''

    def write(self, message):
        l = message.split('\n')
        l[0] = self._buf + l[0]
        for s in l[:-1]:
            self.logger.log(self.level, s)
        self._buf = l[-1]

    def flush(self):
        pass
