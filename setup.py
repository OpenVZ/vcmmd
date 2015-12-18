from distutils.core import setup

setup(name='vcmmd',
      description='Virtuozzo memory management daemon',
      version='0.1',
      license='GPLv2',
      packages=['vcmmd',
                'vcmmd.ldmgr',
                'vcmmd.ldmgr.policies',
                'vcmmd.ve',
                'vcmmd.cgroup'],
      data_files=[('/etc/dbus-1/system.d', ['dbus/com.virtuozzo.vcmmd.conf']),
                  ('/etc/logrotate.d', ['logrotate/vcmmd'])],
      scripts=['bin/vcmmd', 'bin/vcmmdctl'])
