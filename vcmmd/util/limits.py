from __future__ import absolute_import

from os import sysconf

INT64_MAX = int(2 ** 63 - 1)
UINT64_MAX = int(2 ** 64 - 1)

PAGE_SIZE = sysconf('SC_PAGE_SIZE')
