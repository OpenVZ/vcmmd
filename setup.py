from distutils.core import setup

with open('Makefile.version') as f:
    version = f.read().strip()

setup(name='vcmmd',
      description='Virtuozzo memory management daemon',
      version=version,
      license='GPLv2',
      packages=['vcmmd',
                'vcmmd.util',
                'vcmmd.cgroup',
                'vcmmd.ldmgr',
                'vcmmd.ldmgr.policies',
                'vcmmd.ve'],
      data_files=[('/etc/dbus-1/system.d', ['dbus/com.virtuozzo.vcmmd.conf']),
                  ('/etc/logrotate.d', ['logrotate/vcmmd'])],
      scripts=['bin/vcmmd', 'bin/vcmmdctl'])
