from distutils.core import setup, Extension
from distutils.command.install import install

import sys
import brand
import re
import os

with open('Makefile.version') as f:
    version = f.read().strip()
    f.close()

servicefile = 'vcmmd.service'
systemd_unitdir = '/usr/lib/systemd/system'
systemd_unit = systemd_unitdir + "/" + servicefile

setup(name='vcmmd',
      description='%s memory management daemon' % brand.PRODUCT_NAME_SHORT,
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
                  ('/etc/vz', ['vcmmd.conf', 'vstorage-limits.conf']),
                  ('/etc/vz/vcmmd.d', ['scripts/vz']),
                  ('/usr/lib/tmpfiles.d/', ['vcmmd-tmpfiles.conf']),
                  (systemd_unitdir, ['systemd/%s' % servicefile])],
      scripts=['bin/vcmmd', 'bin/vcmmdctl'])

if len(sys.argv) < 2 or (len(sys.argv) > 1 and  sys.argv[1] != "install"):
    sys.exit(0)

def get_tmp_fname(fl):
    return fl + "_tmp"

if '--root' in sys.argv:
    try:
        systemd_unit = sys.argv[sys.argv.index('--root') + 1] + "/" + systemd_unit
    except:
        pass

try:
    with open(systemd_unit, "r") as f:
        fw = open(get_tmp_fname(systemd_unit), 'w')
        for line in f.readlines():
            fw.write(re.sub("@PRODUCT_NAME_SHORT@", brand.PRODUCT_NAME_SHORT, line))
        f.close()
        fw.close()
    os.rename(get_tmp_fname(systemd_unit), systemd_unit)
except:
    print "Branding failed"
    sys.exit(1)
