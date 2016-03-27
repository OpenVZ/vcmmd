from __future__ import absolute_import

import sys
from optparse import OptionParser, OptionGroup

from vcmmd.error import VCMMDError
from vcmmd.ve_type import (get_ve_type_name,
                           lookup_ve_type_by_name,
                           get_all_ve_type_names)
from vcmmd.ve_config import VEConfig
from vcmmd.rpc.dbus.client import RPCProxy
from vcmmd.util.limits import INT64_MAX
from vcmmd.util.optparse import OptionWithMemsize
from vcmmd.util.logging import LOG_LEVELS


def _add_ve_config_options(parser):
    group = OptionGroup(parser, 'VE config options')
    group.add_option('--guarantee', type='memsize',
                     help='VE memory guarantee')
    group.add_option('--limit', type='memsize',
                     help='Max memory allocation available to VE')
    group.add_option('--swap', type='memsize',
                     help='Size of host swap space that may be used by VE')
    parser.add_option_group(group)


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
    parser = OptionParser('Usage: %%prog register {%s} <VE name> '
                          '[VE config options]' %
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

    RPCProxy().register_ve(ve_name, ve_type, _ve_config_from_options(options))


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

    RPCProxy().activate_ve(ve_name)


def _handle_update(args):
    parser = OptionParser('Usage: %prog update <VE name> '
                          '[VE config options]',
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

    RPCProxy().update_ve_config(ve_name, _ve_config_from_options(options))


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

    (options, args) = parser.parse_args(args)
    if len(args) > 0:
        parser.error('superfluous arguments')

    proxy = RPCProxy()
    ve_list = proxy.get_all_registered_ves()

    fmt = '%-36s %4s %6s %{0}s %{0}s %{0}s'.format(11 if options.bytes else 9)
    print fmt % ('name', 'type', 'active', 'guarantee', 'limit', 'swap')
    for ve_name, ve_type, ve_active, ve_config in ve_list:
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
                          '|'.join(LOG_LEVELS),
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


def main():
    parser = OptionParser('Usage: %prog <command> <args>...\n'
                          'command := register | activate | update | '
                          'deactivate | unregister | list | set-log-level',
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
        }[args[0]]
    except KeyError:
        parser.error('invalid command')

    try:
        handler(args[1:])
    except VCMMDError as err:
        sys.stderr.write('VCMMD returned error: %s\n' % err)
        sys.exit(1)

if __name__ == "__main__":
    main()
