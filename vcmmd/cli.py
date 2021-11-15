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

import json
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


def _fail(msg, fail=True):
    sys.stderr.write(msg)
    sys.stderr.write('\n')
    if fail:
        sys.exit(1)


def _print_json(data):
    d = data
    if isinstance(data, str):  # some APIs already return JSON string
        d = json.loads(data)
    print(json.dumps(d, sort_keys=True, indent=4))


def _add_json_format_option(parser):
    parser.add_option('-j', action='store_true', help='JSON output format')


def _add_ve_config_options(parser):
    parser.add_option('--guarantee', type='memsize',
                      help='VE memory guarantee')
    parser.add_option('--limit', type='memsize',
                      help='Max memory allocation available to VE')
    parser.add_option('--swap', type='memsize',
                      help='Size of host swap space that may be used by VE')
    parser.add_option('--cache', type='memsize',
                      help='Max cache size available to VE')
    parser.add_option('--cpunum', type='int',
                      help='Max VCPUs available to VE')


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
    if options.cache is not None:
        kv['cache'] = options.cache
    if options.cpunum is not None:
        kv['cpunum'] = options.cpunum
    return VEConfig(**kv)


def _handle_register(args):
    parser = OptionParser('Usage: %prog register {{{}}} <VE name> '
                          '[options]'.format('|'.join(get_all_ve_type_names())),
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
        val = val // divisor
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
        return '{{:.{0}f}}{1}'.format(1 if val < 10 else 0, suffix).format(val)


def _handle_list(args):
    parser = OptionParser('Usage: %prog list [options]',
                          description='List all VEs known to VCMMD along with '
                          'their state and configuration. By default, '
                          'all memory values are reported in kB.',
                          conflict_handler='resolve')
    _add_memval_config_options(parser)
    _add_json_format_option(parser)

    (options, args) = parser.parse_args(args)
    if len(args) > 0:
        parser.error('superfluous arguments')

    proxy = RPCProxy()
    ve_list = proxy.get_all_registered_ves()

    max_name_len = max(len(ve[0]) for ve in ve_list) if ve_list else 12
    fields = (
        'name', 'type', 'active', 'guarantee', 'limit', 'swap', 'cache',
        'cpunum')

    fmt = '%-{0}s %6s %6s %{1}s %{1}s %{1}s %{1}s %{1}s'.format(
        max_name_len, 11 if options.bytes else 9)
    if not options.j:
        print(fmt % fields)

    data = []

    for ve_name, ve_type, ve_active, ve_config in sorted(ve_list):
        try:
            ve_type_name = get_ve_type_name(ve_type)
        except KeyError:
            ve_type_name = '?'
        if options.j:
            data.append(dict(zip(
                fields, (ve_name, ve_type_name, ve_active, ve_config.guarantee,
                         ve_config.limit, ve_config.swap, ve_config.cache,
                         ve_config.cpunum))))
            continue
        print(fmt % (ve_name, ve_type_name,
                     'yes' if ve_active else 'no',
                     _str_memval(ve_config.guarantee, options),
                     _str_memval(ve_config.limit, options),
                     _str_memval(ve_config.swap, options),
                     _str_memval(ve_config.cache, options),
                     ve_config.cpunum))
    if options.j:
        _print_json(data)


def _handle_log_level(args):
    log_levels = '|'.join(sorted(LOG_LEVELS, key=LOG_LEVELS.get))
    parser = OptionParser('Usage: %prog set-log-level {}'.format(log_levels),
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
    parser = OptionParser('Usage: %prog current-policy [--file]',
                          description='Print current VCMMD policy.')

    parser.add_option('-f', '--file', action='store_true', dest='file',
                      help='read value from config file')

    (options, args) = parser.parse_args(args)
    if len(args) > 0:
        parser.error('superfluous arguments')

    if options.file:
        print(RPCProxy().get_policy_from_file())
    else:
        print(RPCProxy().get_current_policy())


def _handle_switch_policy(args):
    parser = OptionParser('Usage: %prog switch-policy policy-name',
                          description='Switch current VCMMD policy.')

    (options, args) = parser.parse_args(args)
    if len(args) > 1:
        parser.error('superfluous arguments')

    if len(args) < 1:
        parser.error('logging level not specified')

    RPCProxy().switch_policy(args[0])


def _handle_policy_counts(args):
    parser = OptionParser('Usage: %prog policy-count',
                          description='Print policy counts.')
    _, args = parser.parse_args(args)
    if len(args) > 0:
        parser.error('superfluous arguments')
    _print_json(RPCProxy().get_policy_counts())


def _handle_get_config(args):
    parser = OptionParser('Usage: %prog config',
                          description='Print current VCMMD config.')
    parser.add_option('-f', action='store_true',
                      help='print full configuration with default values')
    options, args = parser.parse_args(args)
    if len(args) > 0:
        parser.error('superfluous arguments')
    _print_json(RPCProxy().get_config(bool(options.f)))


def _handle_get_format_stats(parser, args, prettify):
    (options, args) = parser.parse_args(args)
    proxy = RPCProxy()
    if len(args) == 0:
        ve_names = [vm[0] for vm in proxy.get_all_registered_ves()]
    else:
        ve_names = args
    for ve in ve_names:
        try:
            print(ve + ": " + prettify(proxy.get_stats(ve)))
        except VCMMDError as err:
            _fail(ve + ': VCMMD returned error: {}'.format(err), fail=False)


def _handle_get_stats(args):
    parser = OptionParser('Usage: %prog stats [VE] ...',
                          description='Print statistics for the specified VEs '
                          'or for all registered VEs if arguments are omitted.')
    _handle_get_format_stats(parser, args,
                             lambda stats: " ".join('='.join(map(str, s)) for s in stats))


def _handle_get_missing_stats(args):
    parser = OptionParser('Usage: %prog stats [VE] ...',
                          description='Print missing statistics for the '
                          'specified VEs or for all registered VEs if '
                          'arguments are omitted.')
    _handle_get_format_stats(parser, args,
                             lambda stats: " ".join(str(s[0]) for s in stats if s[1] == -1))


def _handle_free(args):
    parser = OptionParser('Usage: %prog free',
                          description='Print current memory usage.',
                          conflict_handler='resolve')
    _add_memval_config_options(parser)
    _add_json_format_option(parser)

    options, args = parser.parse_args(args)
    if len(args) > 0:
        parser.error('superfluous arguments')

    free = RPCProxy().get_free()
    head = [*map(str, free.keys())]
    vals = [_str_memval(v, options) for v in free.values()]
    if options.j:
        _print_json(dict(zip(head, vals)))
    else:
        fmt = ' '.join(['{:'+str(len(v)+3)+'}' for v in free.keys()])
        for s in head, vals:
            print(fmt.format(*s))


def main():
    parser = OptionParser('Usage: %prog <command> <args>...\n'
                          'command := register | activate | update | '
                          'deactivate | unregister | list | set-log-level | '
                          'current-policy | stats | '
                          'free | config | policy-counts',
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
            'current-policy': _handle_current_policy,
            'set-policy': _handle_switch_policy,
            'config': _handle_get_config,
            'policy-counts': _handle_policy_counts,
            'stats': _handle_get_stats,
            'get-missing-stats': _handle_get_missing_stats,
            'free': _handle_free,
        }[args[0]]
    except KeyError:
        parser.error('invalid command')

    try:
        handler(args[1:])
    except VCMMDError as err:
        _fail('VCMMD returned error: {}'.format(err))
    except DBusException as err:
        _fail('Failed to connect to VCMMD: {}'.format(err))


if __name__ == "__main__":
    main()
