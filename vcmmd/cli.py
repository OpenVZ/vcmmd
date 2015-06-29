import copy
import optparse
import sys
import socket

from core import Error, LoadConfig
from rpc import RPCError, RPCProxy

DEFAULT_SOCKET = "/var/run/vcmmd.socket"


# borrowed from chromium
class _OptionWithMemsize(optparse.Option):

    @staticmethod
    def _CheckMemsize(option, opt, value):
        # Note: purposely no 'b' suffix, since that makes 0x12b ambiguous.
        multiplier_table = [
            ('g', 1024 * 1024 * 1024),
            ('m', 1024 * 1024),
            ('k', 1024),
            ('', 1),
        ]
        for (suffix, multiplier) in multiplier_table:
            if value.lower().endswith(suffix):
                new_value = value
                if suffix:
                    new_value = new_value[:-len(suffix)]
                try:
                    # Convert w/ base 0 (handles hex, binary, octal, etc)
                    return int(new_value, 0) * multiplier
                except ValueError:
                    # Pass and try other suffixes; not useful now, but may be
                    # useful later if we ever allow B vs. GB vs. GiB.
                    pass
        raise optparse.OptionValueError("option %s: invalid memsize value: %r"
                                        % (opt, value))

    TYPES = optparse.Option.TYPES + ('memsize',)
    TYPE_CHECKER = copy.copy(optparse.Option.TYPE_CHECKER)

_OptionWithMemsize.TYPE_CHECKER['memsize'] = _OptionWithMemsize._CheckMemsize


def _error(msg):
    sys.stderr.write(msg + "\n")
    sys.exit(1)


def _config_from_opts(options, default=None):
    if not default:
        default = LoadConfig()
    for param_name in ("guarantee", "limit", "swap_limit"):
        if options.__dict__[param_name] is None:
            options.__dict__[param_name] = default.__dict__[param_name]
    return LoadConfig(options.guarantee, options.limit, options.swap_limit)


class _CmdHandler:

    def __handle_register(self):
        self.proxy.register_entity(self.options.id,
                                   _config_from_opts(self.options))

    def __handle_unregister(self):
        self.proxy.unregister_entity(self.options.id)

    def __handle_set_config(self):
        cfg = self.proxy.get_entity_config(self.options.id)
        self.proxy.set_entity_config(self.options.id,
                                     _config_from_opts(self.options,
                                                       default=cfg))

    def __handle_get_config(self):
        print self.proxy.get_entity_config(self.options.id)

    def __handle_list(self):
        for id, cfg in self.proxy.get_entities():
            print "%s <%s>" % (id, cfg)

    __handlers = {
        "register":     __handle_register,
        "unregister":   __handle_unregister,
        "set_config":   __handle_set_config,
        "get_config":   __handle_get_config,
        "list":         __handle_list,
    }

    def __init__(self, cmdname, options):
        self.proxy = RPCProxy()
        self.cmdname = cmdname
        self.options = options

    def __nonzero__(self):
        return self.cmdname in self.__handlers

    def handle(self):
        self.proxy.connect(self.options.socket)
        try:
            self.__handlers[self.cmdname](self)
        finally:
            self.proxy.disconnect()


def main():
    parser = optparse.OptionParser("Usage: %prog [options] command [args]\n"
                                   "command := register | unregister | "
                                   "set_config | get_config | list",
                                   option_class=_OptionWithMemsize)
    parser.add_option("-s", "--socket", default=DEFAULT_SOCKET,
                      help="use SOCKET to connect to daemon")
    group = optparse.OptionGroup(parser, "Command arguments")
    group.add_option("--id", default="", help="entity ID")
    group.add_option("--guarantee", type="memsize",
                     help="memory guarantee")
    group.add_option("--limit", type="memsize",
                     help="memory limit")
    group.add_option("--swap_limit", type="memsize",
                     help="swap limit")
    parser.add_option_group(group)

    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("incorrect number of arguments")

    handler = _CmdHandler(args[0], options)
    if not handler:
        parser.error("unknown command")

    try:
        handler.handle()
    except socket.error as err:
        _error("Transport error: %s" % err)
    except RPCError as err:
        _error("Protocol error: %s" % err)
    except Error as err:
        _error("Command failed: %s" % err)

if __name__ == "__main__":
    main()
