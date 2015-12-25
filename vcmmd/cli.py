import sys
import dbus


class _ArgParser:

    def __init__(self, args):
        self.args = args

    def _error(self, errmsg):
        progname = self.args[0]
        sys.stderr.write('%s: %s\n' % (progname, errmsg))
        if self._helpmsg:
            sys.stderr.write((self._helpmsg % progname) + '\n')
        sys.exit(1)

    def _next_str(self, argname):
        try:
            val = self.args[self._idx]
        except IndexError:
            self._error('no value for <%s>' % argname)
        self._idx += 1
        return val

    def _next_int(self, argname):
        val = self._next_str(argname)
        try:
            return int(val)
        except ValueError:
            self._error('invalid value for <%s>' % argname)

    def _next_config(self):
        return ((0, self._next_int('guarantee')),
                (1, self._next_int('limit')),
                (2, self._next_int('swap')),)

    def _parse_flags(self, flags_list):
        flags_dict = {flg: False for flg in flags_list}
        while self._idx < len(self.args):
            arg = self.args[self._idx]
            if not arg.startswith('--'):
                break
            flg = arg[2:]
            if flg not in flags_dict:
                self._error('unknown flag: %s' % arg)
            flags_dict[flg] = True
            self._idx += 1
        return flags_dict

    def _handle_register(self):
        self._helpmsg = ('Usage: %s register [--force] <name> <type> '
                         '<guarantee> <limit> <swap>')
        flags = self._parse_flags(['force'])
        return (self._next_str('name'),
                self._next_int('type'),
                self._next_config(),
                flags['force'])

    def _handle_commit(self):
        self._helpmsg = 'Usage: %s commit <name>'
        return (self._next_str('name'),)

    def _handle_update(self):
        self._helpmsg = ('Usage: %s update [--force] <name>'
                         '<guarantee> <limit> <swap>')
        flags = self._parse_flags(['force'])
        return (self._next_str('name'),
                self._next_config(),
                flags['force'])

    def _handle_unregister(self):
        self._helpmsg = 'Usage: %s unregister <name>'
        return (self._next_str('name'),)

    def _handle_list(self):
        self._helpmsg = 'Usage: %s list'
        return ()

    _command_handlers = {
        'register':     _handle_register,
        'commit':       _handle_commit,
        'update':       _handle_update,
        'unregister':   _handle_unregister,
        'list':         _handle_list,
    }

    def parse(self):
        self._idx = 1
        self._helpmsg = ('Usage: %s <command> <args>...\n'
                         'command := register | commit | update | '
                         'unregister | list')
        cmd = self._next_str('command')
        handler = self._command_handlers.get(cmd)
        if not handler:
            self._error('unknown command')
        args = handler(self)
        if self._idx < len(self.args):
            self._error('superfluous arguments')
        return (cmd, args)


def _print_ves(ve_list):
    fmt = '%-16s %2s %2s : config %8s %8s %8s %8s'
    for name, type, committed, config in ve_list:
        print fmt % (name, type, 'c' if committed else '-',
                     config[0], config[1], config[2], config[3])


def main():
    cmd, args = _ArgParser(sys.argv).parse()

    bus = dbus.SystemBus()
    obj = bus.get_object('com.virtuozzo.vcmmd', '/LoadManager')
    iface = dbus.Interface(obj, 'com.virtuozzo.vcmmd.LoadManager')

    if cmd == 'register':
        err = iface.RegisterVE(*args)
    elif cmd == 'commit':
        err = iface.CommitVE(*args)
    elif cmd == 'update':
        err = iface.UpdateVE(*args)
    elif cmd == 'unregister':
        err = iface.UnregisterVE(*args)
    elif cmd == 'list':
        ve_list = iface.GetAllRegisteredVEs()
        _print_ves(ve_list)
        err = 0

    if err:
        print 'vcmmd service returned error: %s' % err

if __name__ == "__main__":
    main()
