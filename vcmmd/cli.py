import sys
import dbus
from optparse import OptionParser, OptionGroup

from vcmmd.util.limits import UINT64_MAX
from vcmmd.util.optparse import OptionWithMemsize


def _get_proxy():
    bus = dbus.SystemBus()
    obj = bus.get_object('com.virtuozzo.vcmmd', '/LoadManager')
    iface = dbus.Interface(obj, 'com.virtuozzo.vcmmd.LoadManager')
    return iface


def _report_service_error(err):
    try:
        errmsg = {
            0: 'Success',
            1: 'Invalid VE name',
            2: 'Invalid VE type',
            3: 'Conflicting VE config parameters',
            4: 'VE name already in use',
            5: 'VE not registered',
            6: 'VE already active',
            7: 'VE operation failed',
            8: 'Unable to meet VE requirements',
            9: 'VE not active',
        }[err]
    except KeyError:
        errmsg = 'Unknown error %d' % err

    sys.stderr.write('VCMMD returned error: %s\n' % errmsg)
    sys.exit(1)


def _add_ve_config_options(parser):
    group = OptionGroup(parser, 'VE config options')
    group.add_option('--guar', type='memsize',
                     help='VE memory guarantee')
    group.add_option('--limit', type='memsize',
                     help='Max memory allocation available to VE')
    group.add_option('--swap', type='memsize',
                     help='Size of host swap space that may be used by VE')
    group.add_option('-f', '--force', action='store_true',
                     help='Apply VE config even if there is not enough memory '
                     'on the host to meet VE\'s requirements')
    parser.add_option_group(group)


def _ve_config_from_options(options):
    ve_config = []
    if options.guar is not None:
        ve_config.append((0, options.guar))
    if options.limit is not None:
        ve_config.append((1, options.limit))
    if options.swap is not None:
        ve_config.append((2, options.swap))
    return tuple(ve_config)


def _handle_register(args):
    parser = OptionParser('Usage: %prog register {CT|VM} <VE name> '
                          '[VE config options]',
                          description='Register a VE with the VCMMD service.',
                          option_class=OptionWithMemsize)
    _add_ve_config_options(parser)

    (options, args) = parser.parse_args(args)
    if len(args) > 2:
        parser.error('superfluous arguments')

    if len(args) < 1:
        parser.error('VE type not specified')
    try:
        ve_type = {
            'CT': 0,
            'VM': 1,
        }[args[0]]
    except KeyError:
        parser.error('VE type must be either CT or VM')

    if len(args) < 2:
        parser.error('VE name not specified')
    ve_name = args[1]

    proxy = _get_proxy()
    err = proxy.RegisterVE(ve_name, ve_type, _ve_config_from_options(options),
                           options.force)
    if err:
        _report_service_error(err)


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

    proxy = _get_proxy()
    err = proxy.ActivateVE(ve_name)
    if err:
        _report_service_error(err)


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

    proxy = _get_proxy()
    err = proxy.UpdateVE(ve_name, _ve_config_from_options(options),
                         options.force)
    if err:
        _report_service_error(err)


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

    proxy = _get_proxy()
    err = proxy.DeactivateVE(ve_name)
    if err:
        _report_service_error(err)


def _handle_unregister(args):
    parser = OptionParser('Usage: %prog unregister <VE name>',
                          description='Make VCMMD forget about a VE.')

    (options, args) = parser.parse_args(args)
    if len(args) > 1:
        parser.error('superfluous arguments')

    if len(args) < 1:
        parser.error('VE name not specified')
    ve_name = args[0]

    proxy = _get_proxy()
    err = proxy.UnregisterVE(ve_name)
    if err:
        _report_service_error(err)


def _str_memval(val):
    if val >= UINT64_MAX:
        return 'max'
    return str(val >> 10)


def _handle_list(args):
    parser = OptionParser('Usage: %prog list',
                          description='List all VEs known to VCMMD along with '
                          'their state and configuration. All memory values '
                          'are reported in kB.')

    (options, args) = parser.parse_args(args)
    if len(args) > 0:
        parser.error('superfluous arguments')

    proxy = _get_proxy()
    ve_list = proxy.GetAllRegisteredVEs()

    fmt = '%-16s %4s %6s : %8s %8s %8s'
    print fmt % ('name', 'type', 'active', 'guar', 'limit', 'swap')
    for ve_name, ve_type, ve_active, ve_config in ve_list:
        try:
            ve_type_str = {
                0: 'CT',
                1: 'VM',
            }[ve_type]
        except KeyError:
            ve_type_str = '?'
        print fmt % (ve_name,
                     ve_type_str,
                     'yes' if ve_active else 'no',
                     _str_memval(ve_config[0]),
                     _str_memval(ve_config[1]),
                     _str_memval(ve_config[2]))


def main():
    parser = OptionParser('Usage: %prog <command> <args>...\n'
                          'command := register | activate | update | '
                          'deactivate | unregister | list',
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
        }[args[0]]
    except KeyError:
        parser.error('invalid command')

    handler(args[1:])

if __name__ == "__main__":
    main()
