from __future__ import absolute_import

from vcmmd.cgroup.base import Cgroup


class BeancounterCgroup(Cgroup):

    CONTROLLER = 'beancounter'

    def get_privvmpages(self):
        return self._read_file_int('privvmpages.held')
