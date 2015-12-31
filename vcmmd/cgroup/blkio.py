from __future__ import absolute_import

from vcmmd.cgroup.base import Cgroup


class BlkIOCgroup(Cgroup):

    CONTROLLER = 'blkio'

    def _get_io_stats(self, filename, keys):
        result = {k: 0 for k in keys}
        kv = self._read_file_kv(filename)
        for k, v in kv.iteritems():
            try:
                result[k.split()[-1]] += v
            except KeyError:
                continue
        return tuple(result[k] for k in keys)

    def get_io_serviced(self):
        '''Return a tuple containing the total number of read and write
        requests issued by this cgroup.
        '''
        return self._get_io_stats('blkio.io_serviced', ('Read', 'Write'))

    def get_io_service_bytes(self):
        '''Return a tuple containing the total number of bytes read and written
        by this cgroup.
        '''
        return self._get_io_stats('blkio.io_service_bytes', ('Read', 'Write'))
