# Copyright (c) 2016-2017, Parallels International GmbH
# Copyright (c) 2017-2021, Virtuozzo International GmbH, All rights reserved
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

import os
import json


def get_features():
    """Return CPU features (parsed 'flags' property from /proc/cpuinfo)."""
    with open('/proc/cpuinfo') as fp:
        cpuinfo = {
            name.strip(): value.strip()
            for name, value in (
                line.split(':') for line in fp.readlines() if line.strip())
        }
    return set(cpuinfo.get('flags', '').split())


_VLN_LIST = ['ibrs', 'pti', 'retp', 'ssbd']
_VLN_CONFIG_PATH = '/sys/kernel/debug/x86'
_VLN_STORE_FILEPATH = '/tmp/vcmmd-vln.private'


def _vln_mit_path(vln):
    return os.path.join(_VLN_CONFIG_PATH, '{}_enabled'.format(vln))


def _set_vln_mitigation(vln, mitigation):
    try:
        with open(_vln_mit_path(vln), 'w') as fp:
            fp.write(str(mitigation))
    except IOError:
        pass


def get_vln_mitigations():
    """Return known vulnerabilities mitigations."""
    mitigations = {}
    for vln in _VLN_LIST:
        try:
            with open(_vln_mit_path(vln)) as fp:
                mit = int(fp.read())
                if mit:
                    mitigations[vln] = mit
        except IOError:
            pass
    return mitigations


def set_vln_mitigations(mitigations):
    """Set known vulnerabilities mitigations."""
    for vln, mitigation in mitigations.items():
        if vln not in _VLN_LIST:
            raise ValueError('Unknown vulnerability "{}"'.format(vln))
        _set_vln_mitigation(vln, mitigation)


def is_vln_mitigations_enabled():
    """Check if the mitigations is enabled."""
    x = any(get_vln_mitigations().values())
    return x and not os.path.isfile(_VLN_STORE_FILEPATH)


def disable_vln_mitigations():
    """Disable all managed vulnerabilities mitigations."""
    mitigations = get_vln_mitigations()
    if not os.path.isfile(_VLN_STORE_FILEPATH):
        with open(_VLN_STORE_FILEPATH, 'w') as fp:
            json.dump(mitigations, fp)
    for vln in mitigations.keys():
        _set_vln_mitigation(vln, 0)


def enable_vln_mitigations():
    """Enable all manage vulnerabilities mitigations."""
    if os.path.isfile(_VLN_STORE_FILEPATH):
        with open(_VLN_STORE_FILEPATH) as fp:
            mitigations = json.load(fp)
        for vln, mitigation in mitigations.items():
            if mitigation:
                _set_vln_mitigation(vln, mitigation)
        os.remove(_VLN_STORE_FILEPATH)


def is_vln_mitigations_supported():
    return 'vz7' in os.uname().release
