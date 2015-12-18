class Cgroup(object):

    _CGROUP_DIR = '/sys/fs/cgroup'

    CONTROLLER = None

    def __init__(self, path):
        self.__abs_path = '/'.join([self._CGROUP_DIR,
                                    self.CONTROLLER,
                                    path.strip('/')])

    def _file_path(self, name):
        return '/'.join([self.__abs_path, name])

    def read_file_str(self, filename):
        with open(self._file_path(filename), 'r') as f:
            return f.read()

    def write_file_str(self, filename, val):
        with open(self._file_path(filename), 'w') as f:
            f.write(val)

    def read_file_int(self, filename):
        return int(self.read_file_str(filename))

    def write_file_int(self, filename, val):
        self.write_file_str(filename, str(val))
