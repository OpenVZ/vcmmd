from __future__ import absolute_import

import subprocess


class Error(Exception):
    pass


def escape_unit_name(string, suffix=None):
    '''Escape string for usage in systemd unit names.

    This function uses `systemd-escape` utility. If the latter fails, it will
    raise Error.
    '''
    args = ['systemd-escape']
    if suffix is not None:
        args.append('--suffix')
        args.append(suffix)
    args.append(string)

    try:
        p = subprocess.Popen(args,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
    except OSError as err:
        raise Error('Failed to call `systemd-escape`: %s' % err)

    if p.returncode != 0:
        raise Error('`systemd-escape` failed with return code %d: %s' %
                    (p.returncode, stderr))

    return stdout.rstrip('\n')
