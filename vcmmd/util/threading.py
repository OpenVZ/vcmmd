# Copyright (c) 2016-2017, Parallels International GmbH
# Copyright (c) 2017-2022, Virtuozzo International GmbH, All rights reserved
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

import sys
import threading
from multiprocessing.pool import ThreadPool


def setup_thread_excepthook():
    """
    Workaround for `sys.excepthook` thread bug from:
    http://bugs.python.org/issue1230540

    Call once from the main thread before creating any threads.
    """

    init_original = threading.Thread.__init__

    def init(self, *args, **kwargs):
        init_original(self, *args, **kwargs)
        run_original = self.run

        def run_with_except_hook(*args2, **kwargs2):
            try:
                run_original(*args2, **kwargs2)
            except Exception:
                sys.excepthook(*sys.exc_info())

        self.run = run_with_except_hook

    threading.Thread.__init__ = init


def update_stats_single(fn):
    """
    Special decorator for update stats methods.
    Such methods should not be running in parallel for single object
    """
    lock = threading.Lock()

    def wrapped(*args, **kwargs):
        if lock.locked():
            # looks like some one update this stats right now,
            # so let's just wait until it finished.
            with lock:
                return

        with lock:
            # update stats methods should not return anything
            assert not fn(*args, **kwargs)
    wrapped.__lock = lock
    return wrapped


# The thread pool is used in order not to block the main thread while
# performing costly operations, like memory.high adjustment, or avoid deadlock
# with libvirtd.

# XXX: Note, using threads should not really hurt parallelism, because real
# work is done from system calls, with GIL released.

_thread_pool = ThreadPool(3)


def run_async(func, *args, **kwargs):
    return _thread_pool.apply_async(func, args, kwargs)
