import os
import subprocess
import sys

from distutils.core import setup, Extension
from distutils.dist import Distribution as _Distribution
from distutils.command.build_py import build_py as _build_py

with open('Makefile.version') as f:
    version = f.read().strip()


def error(msg):
    sys.stderr.write(msg + "\n")
    sys.exit(1)


def generate_proto(source, out_dir='.'):
    if not os.path.exists(source):
        error("File not found: %s" % source)
    output = os.path.join(out_dir, source.replace('.proto', '_pb2.py'))
    if (not os.path.exists(output) or
            (os.path.getmtime(source) > os.path.getmtime(output))):
        protoc_command = ['protoc', '-I.', '--python_out=%s' % out_dir, source]
        print ' '.join(protoc_command)
        if subprocess.call(protoc_command) != 0:
            error("protoc failed")


class build_py(_build_py):

    def initialize_options(self):
        _build_py.initialize_options(self)
        self.protofiles = None

    def finalize_options(self):
        _build_py.finalize_options(self)
        self.protofiles = self.distribution.protofiles

    def run(self):
        _build_py.run(self)
        for proto in self.protofiles or []:
            generate_proto(proto, self.build_lib)


class Distribution(_Distribution):

    def __init__(self, attrs=None):
        self.protofiles = None
        _Distribution.__init__(self, attrs)

setup(name='vcmmd',
      description='Virtuozzo memory management daemon',
      version=version,
      license='GPLv2',
      packages=['vcmmd'],
      ext_modules=[Extension('vcmmd.idlememscan',
                             ['vcmmd/idlememscan.cpp'],
                             extra_compile_args=['-std=c++11'])],
      protofiles=['vcmmd/rpc.proto'],
      data_files=[('/etc/vz', ['vcmmd.conf']),
                  ('/etc/logrotate.d', ['logrotate/vcmmd'])],
      scripts=['bin/vcmmd', 'bin/vcmmdctl'],
      distclass=Distribution,
      cmdclass={'build_py': build_py},
      )
