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

import copy
import optparse


# borrowed from chromium
class OptionWithMemsize(optparse.Option):

    @staticmethod
    def _CheckMemsize(option, opt, value):
        # Note: purposely no 'b' suffix, since that makes 0x12b ambiguous.
        multiplier_table = [
            ('g', 1024 * 1024 * 1024),
            ('m', 1024 * 1024),
            ('k', 1024),
            ('', 1),
        ]
        for (suffix, multiplier) in multiplier_table:
            if value.lower().endswith(suffix):
                new_value = value
                if suffix:
                    new_value = new_value[:-len(suffix)]
                try:
                    # Convert w/ base 0 (handles hex, binary, octal, etc)
                    return int(new_value, 0) * multiplier
                except ValueError:
                    # Pass and try other suffixes; not useful now, but may be
                    # useful later if we ever allow B vs. GB vs. GiB.
                    pass
        raise optparse.OptionValueError("option {}: invalid memsize value: "
                                        "{}".format(opt, value))

    TYPES = optparse.Option.TYPES + ('memsize',)
    TYPE_CHECKER = copy.copy(optparse.Option.TYPE_CHECKER)

OptionWithMemsize.TYPE_CHECKER['memsize'] = OptionWithMemsize._CheckMemsize
