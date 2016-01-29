from distutils.core import setup, Extension

with open('Makefile.version') as f:
    version = f.read().strip()

setup(name='vcmmd',
      description='Virtuozzo memory management daemon',
      version=version,
      license='GPLv2',
      packages=['vcmmd',
                'vcmmd.util',
                'vcmmd.cgroup',
                'vcmmd.rpc',
                'vcmmd.rpc.dbus',
                'vcmmd.ldmgr',
                'vcmmd.ldmgr.policies',
                'vcmmd.ve'],
      ext_modules=[Extension('vcmmd.cgroup.idlememscan',
                             ['vcmmd/cgroup/idlememscan.cpp'],
                             extra_compile_args=['-std=c++11'])],
      data_files=[('/etc/dbus-1/system.d', ['dbus/com.virtuozzo.vcmmd.conf']),
                  ('/etc/logrotate.d', ['logrotate/vcmmd']),
                  ('/etc/vz', ['vcmmd.conf'])],
      scripts=['bin/vcmmd', 'bin/vcmmdctl'])
