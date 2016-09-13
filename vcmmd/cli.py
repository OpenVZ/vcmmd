# Copyright (c) 2016 Parallels IP Holdings GmbH
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
# Our contact details: Parallels IP Holdings GmbH, Vordergasse 59, 8200
# Schaffhausen, Switzerland.

from __future__ import absolute_import

import sys
from optparse import OptionParser
from dbus import DBusException

from vcmmd.error import VCMMDError
from vcmmd.ve_type import (get_ve_type_name,
                           lookup_ve_type_by_name,
                           get_all_ve_type_names)
from vcmmd.ve_config import VEConfig
from vcmmd.rpc.dbus.client import RPCProxy
from vcmmd.util.limits import INT64_MAX
from vcmmd.util.optparse import OptionWithMemsize
from vcmmd.util.logging import LOG_LEVELS
from vcmmd.util.misc import sorted_by_val


def _fail(msg, fail=True):
    sys.stderr.write(msg)
    sys.stderr.write('\n')
    if(fail):
        sys.exit(1)


def _add_ve_config_options(parser):
    parser.add_option('--guarantee', type='memsize',
                      help='VE memory guarantee')
    parser.add_option('--limit', type='memsize',
                      help='Max memory allocation available to VE')
    parser.add_option('--swap', type='memsize',
                      help='Size of host swap space that may be used by VE')


def _add_memval_config_options(parser):
    parser.add_option('-b', '--bytes', action='store_true',
                      help='show output in bytes')
    parser.add_option('-k', '--kilo', action='store_true',
                      help='show output in kilobytes')
    parser.add_option('-m', '--mega', action='store_true',
                      help='show output in megabytes')
    parser.add_option('-g', '--giga', action='store_true',
                      help='show output in gigabytes')
    parser.add_option('-h', '--human', action='store_true',
                      help='show human-readable output')
    parser.add_option('--si', action='store_true',
                      help='use powers of 1000 not 1024')


def _ve_config_from_options(options):
    kv = {}
    if options.guarantee is not None:
        kv['guarantee'] = options.guarantee
    if options.limit is not None:
        kv['limit'] = options.limit
    if options.swap is not None:
        kv['swap'] = options.swap
    return VEConfig(**kv)


def _handle_register(args):
    parser = OptionParser('Usage: %%prog register {%s} <VE name> [options]' %
                          '|'.join(get_all_ve_type_names()),
                          description='Register a VE with the VCMMD service.',
                          option_class=OptionWithMemsize)
    _add_ve_config_options(parser)

    (options, args) = parser.parse_args(args)
    if len(args) > 2:
        parser.error('superfluous arguments')

    if len(args) < 1:
        parser.error('VE type not specified')
    try:
        ve_type = lookup_ve_type_by_name(args[0])
    except KeyError:
        parser.error('invalid VE type')

    if len(args) < 2:
        parser.error('VE name not specified')
    ve_name = args[1]

    RPCProxy().register_ve(ve_name, ve_type,
                           _ve_config_from_options(options), 0)


def _handle_activate(args):
    parser = OptionParser('Usage: %prog activate <VE name>',
                          description='Notify VCMMD that a registered VE can '
                          'now be managed.')

    (options, args) = parser.parse_args(args)
    if len(args) > 1:
        parser.error('superfluous arguments')

    if len(args) < 1:
        parser.error('VE name not specified')
    ve_name = args[0]

    RPCProxy().activate_ve(ve_name, 0)


def _handle_update(args):
    parser = OptionParser('Usage: %prog update <VE name> [options]',
                          description='Request VCMMD to update a VE\'s '
                          'configuration.',
                          option_class=OptionWithMemsize)
    _add_ve_config_options(parser)

    (options, args) = parser.parse_args(args)
    if len(args) > 1:
        parser.error('superfluous arguments')

    if len(args) < 1:
        parser.error('VE name not specified')
    ve_name = args[0]

    RPCProxy().update_ve_config(ve_name, _ve_config_from_options(options), 0)


def _handle_deactivate(args):
    parser = OptionParser('Usage: %prog deactivate <VE name>',
                          description='Notify VCMMD that a registered VE must '
                          'not be managed any longer.')

    (options, args) = parser.parse_args(args)
    if len(args) > 1:
        parser.error('superfluous arguments')

    if len(args) < 1:
        parser.error('VE name not specified')
    ve_name = args[0]

    RPCProxy().deactivate_ve(ve_name)


def _handle_unregister(args):
    parser = OptionParser('Usage: %prog unregister <VE name>',
                          description='Make VCMMD forget about a VE.')

    (options, args) = parser.parse_args(args)
    if len(args) > 1:
        parser.error('superfluous arguments')

    if len(args) < 1:
        parser.error('VE name not specified')
    ve_name = args[0]

    RPCProxy().unregister_ve(ve_name)


def _str_memval(val, opts):
    if val >= INT64_MAX:
        return 'max'

    divisor_base = 1000 if opts.si else 1024
    kilo_base = divisor_base
    mega_base = divisor_base ** 2
    giga_base = divisor_base ** 3

    if not opts.human:
        if opts.bytes:
            divisor = 1
        elif opts.kilo:
            divisor = kilo_base
        elif opts.mega:
            divisor = mega_base
        elif opts.giga:
            divisor = giga_base
        else:  # kB by default
            divisor = kilo_base
        val = val / divisor
        return str(val)
    else:
        if val >= giga_base:
            divisor = giga_base
            suffix = 'G'
        elif val >= mega_base:
            divisor = mega_base
            suffix = 'M'
        elif val >= kilo_base:
            divisor = kilo_base
            suffix = 'K'
        else:
            divisor = 1
            suffix = 'B'
        val = float(val) / divisor
        return '%.{0}f%s'.format(1 if val < 10 else 0) % (val, suffix)


def _handle_list(args):
    parser = OptionParser('Usage: %prog list [options]',
                          description='List all VEs known to VCMMD along with '
                          'their state and configuration. By default, '
                          'all memory values are reported in kB.',
                          conflict_handler='resolve')
    _add_memval_config_options(parser)

    (options, args) = parser.parse_args(args)
    if len(args) > 0:
        parser.error('superfluous arguments')

    proxy = RPCProxy()
    ve_list = proxy.get_all_registered_ves()

    fmt = '%-36s %6s %6s %{0}s %{0}s %{0}s'.format(11 if options.bytes else 9)
    print fmt % ('name', 'type', 'active', 'guarantee', 'limit', 'swap')
    for ve_name, ve_type, ve_active, ve_config in sorted(ve_list):
        try:
            ve_type_name = get_ve_type_name(ve_type)
        except KeyError:
            ve_type_name = '?'
        print fmt % (ve_name, ve_type_name,
                     'yes' if ve_active else 'no',
                     _str_memval(ve_config.guarantee, options),
                     _str_memval(ve_config.limit, options),
                     _str_memval(ve_config.swap, options))


def _handle_log_level(args):
    parser = OptionParser('Usage: %%prog set-log-level {%s}' %
                          '|'.join(sorted_by_val(LOG_LEVELS)),
                          description='Set VCMMD logging level.')

    (options, args) = parser.parse_args(args)
    if len(args) > 1:
        parser.error('superfluous arguments')

    if len(args) < 1:
        parser.error('logging level not specified')

    try:
        lvl = LOG_LEVELS[args[0]]
    except KeyError:
        parser.error('invalid value for logging level')

    RPCProxy().set_log_level(lvl)


def _handle_current_policy(args):
    parser = OptionParser('Usage: %%prog get-current-policy',
                          description='Print current VCMMD policy.')

    (options, args) = parser.parse_args(args)
    if len(args) > 0:
        parser.error('superfluous arguments')

    print RPCProxy().get_current_policy()

def _handle_get_config(args):
    parser = OptionParser('Usage: %%prog config',
                          description='Print current VCMMD config.')

    (options, args) = parser.parse_args(args)
    if len(args) > 0:
        parser.error('superfluous arguments')

    print RPCProxy().get_config()

def _handle_get_format_stats(parser, args, prettify):
    (options, args) = parser.parse_args(args)
    proxy = RPCProxy()
    if len(args) == 0:
        ve_names = [vm[0] for vm in proxy.get_all_registered_ves()]
    else:
        ve_names = args
    for ve in ve_names:
        try:
            print ve + ": " + prettify(proxy.get_stats(ve))
        except VCMMDError as err:
            _fail(ve + ': VCMMD returned error: %s' % err, fail=False)


def _handle_get_stats(args):
    parser = OptionParser('Usage: %%prog get-stats [VE] ...',
                          description='Print statistics for the specified VEs '
                          'or for all registered VEs if arguments are omitted.')
    _handle_get_format_stats(parser, args,
                             lambda stats: " ".join('='.join(map(str, s)) for s in stats))


def _handle_get_missing_stats(args):
    parser = OptionParser('Usage: %%prog get-stats [VE] ...',
                          description='Print missing statistics for the '
                          'specified VEs or for all registered VEs if '
                          'arguments are omitted.')
    _handle_get_format_stats(parser, args,
                             lambda stats: " ".join(str(s[0]) for s in stats if s[1] == -1))


def _handle_get_quotas(args):
    parser = OptionParser('Usage: %%prog get-quotas',
                          description='Print current quotas for all VEs.',
                          conflict_handler='resolve')
    _add_memval_config_options(parser)

    (options, args) = parser.parse_args(args)
    if len(args) > 0:
        parser.error('superfluous arguments')

    quotas = RPCProxy().get_quotas()

    fmt = '%-36s %13s %13s'
    print fmt % ('name', 'target', 'protection')
    for ve_name, target, protection in sorted(quotas):
        print fmt % (ve_name,
                     _str_memval(target, options),
                     _str_memval(protection, options))


def main():
    parser = OptionParser('Usage: %prog <command> <args>...\n'
                          'command := register | activate | update | '
                          'deactivate | unregister | list | set-log-level | '
                          'get-current-policy | get-stats | '
                          'get-missing-stats | get-quotas | config',
                          description='Call a command on the VCMMD service. '
                          'See \'%prog <command> --help\' to read about a '
                          'specific subcommand.',
                          option_class=OptionWithMemsize)
    parser.disable_interspersed_args()

    (options, args) = parser.parse_args()

    if len(args) < 1:
        parser.error('command not specified')

    try:
        handler = {
            'register': _handle_register,
            'activate': _handle_activate,
            'update': _handle_update,
            'deactivate': _handle_deactivate,
            'unregister': _handle_unregister,
            'list': _handle_list,
            'set-log-level': _handle_log_level,
            'get-current-policy': _handle_current_policy,
            'config': _handle_get_config,
            'get-stats': _handle_get_stats,
            'get-missing-stats': _handle_get_missing_stats,
            'get-quotas': _handle_get_quotas,
        }[args[0]]
    except KeyError:
        parser.error('invalid command')

    try:
        handler(args[1:])
    except VCMMDError as err:
        _fail('VCMMD returned error: %s' % err)
    except DBusException as err:
        _fail('Failed to connect to VCMMD: %s' % err)

if __name__ == "__main__":
    main()
