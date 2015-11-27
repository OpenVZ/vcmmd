import errno
import logging
import os
import socket
from SocketServer import UnixStreamServer, ThreadingMixIn, BaseRequestHandler

import rpc_pb2 as rpc_proto
from base import Error, LoadConfig

MAX_MSG_SIZE = 8192


def _make_request(type,
                  id="",
                  config=LoadConfig()):
    req = rpc_proto.Request()
    req.type = type
    req.id = id
    req.config.guarantee = config.guarantee
    req.config.limit = config.limit
    req.config.swap_limit = config.swap_limit
    return req


def _make_response(type,
                   errcode=0,
                   errmsg="",
                   id="",
                   config=LoadConfig(),
                   list_len=0):
    resp = rpc_proto.Response()
    resp.type = type
    resp.errcode = errcode
    resp.errmsg = errmsg
    resp.id = id
    resp.config.guarantee = config.guarantee
    resp.config.limit = config.limit
    resp.config.swap_limit = config.swap_limit
    resp.list_len = list_len
    return resp


def _config_from_msg(msg):
    return LoadConfig(msg.config.guarantee,
                      msg.config.limit,
                      msg.config.swap_limit)


class RPCError(Exception):
    pass


class RPCRequestHandler(BaseRequestHandler):

    def __handle_register(self, req):
        self.server.load_manager.\
            register_entity(req.id, _config_from_msg(req))

    def __handle_unregister(self, req):
        self.server.load_manager.unregister_entity(req.id)

    def __handle_set_config(self, req):
        self.server.load_manager.\
            set_entity_config(req.id, _config_from_msg(req))

    def __handle_get_config(self, req):
        cfg = self.server.load_manager.get_entity_config(req.id)
        return _make_response(type=rpc_proto.RESP_CONFIG,
                              id=req.id, config=cfg)

    def __handle_list(self, req):
        resp_list = []
        entities = self.server.load_manager.get_entities()
        resp_list.append(_make_response(type=rpc_proto.RESP_LIST,
                                        list_len=len(entities)))
        for id, cfg in entities:
            resp_list.append(_make_response(type=rpc_proto.RESP_CONFIG,
                                            id=id, config=cfg))
        return resp_list

    __handlers = {
        rpc_proto.REQ_REGISTER:   __handle_register,
        rpc_proto.REQ_UNREGISTER: __handle_unregister,
        rpc_proto.REQ_SET_CONFIG: __handle_set_config,
        rpc_proto.REQ_GET_CONFIG: __handle_get_config,
        rpc_proto.REQ_LIST:       __handle_list,
    }

    def handle(self):
        logger = self.server.logger

        while True:
            try:
                req_raw = self.request.recv(MAX_MSG_SIZE)
            except socket.error as err:
                logger.warning("Failed to receive request: %s" % err)
                break

            if not req_raw:
                break

            req = rpc_proto.Request()
            try:
                req.ParseFromString(req_raw)
            except:
                logger.warning("Invalid request")
                break

            handler = self.__handlers.get(req.type)

            try:
                if not handler:
                    raise Error(errno.EINVAL, "Invalid request type")
                resp = handler(self, req)
                if not resp:
                    resp = rpc_proto.Response()
                    resp.type = rpc_proto.RESP_EMPTY
            except Error as err:
                resp = rpc_proto.Response()
                resp.type = rpc_proto.RESP_ERROR
                resp.errcode = err.errcode
                resp.errmsg = err.errmsg

            resp_list = resp if isinstance(resp, list) else [resp]

            try:
                for resp in resp_list:
                    self.request.send(resp.SerializeToString())
            except socket.error as err:
                logger.warning("Failed to send response: %s" % err)
                break


class RPCServer(ThreadingMixIn, UnixStreamServer):

    socket_type = socket.SOCK_SEQPACKET

    @staticmethod
    def __remove_socket(path):
        try:
            os.remove(path)
        except OSError:  # no such file?
            pass

    def __init__(self, load_manager, logger=None, **kwargs):
        self.load_manager = load_manager
        self.logger = logger or logging.getLogger(__name__)
        UnixStreamServer.__init__(self, RequestHandlerClass=RPCRequestHandler,
                                  **kwargs)

    def server_bind(self):
        self.__remove_socket(self.server_address)
        UnixStreamServer.server_bind(self)

    def shutdown(self):
        UnixStreamServer.shutdown(self)
        self.__remove_socket(self.server_address)


class RPCProxy:

    def __init__(self, sock_path=None):
        self.socket = None
        if sock_path:
            self.connect(sock_path)

    def connect(self, sock_path):
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.socket.connect(sock_path)

    def disconnect(self):
        self.socket.close()
        self.socket = None

    def __send_request(self, req):
        assert self.socket, "Not connected"
        self.socket.send(req.SerializeToString())

    def __recv_response(self, expected_type):
        assert self.socket, "Not connected"
        resp_raw = self.socket.recv(MAX_MSG_SIZE)
        resp = rpc_proto.Response()
        try:
            resp.ParseFromString(resp_raw)
        except:
            raise RPCError("Failed to parse response")
        if resp.type == rpc_proto.RESP_ERROR:
            raise Error(resp.errcode, resp.errmsg)
        if resp.type != expected_type:
            raise RPCError("Unexpected response type "
                           "(received %s, expected %s)" %
                           (resp.type, expected_type))
        return resp

    def register_entity(self, id, cfg):
        self.__send_request(_make_request(type=rpc_proto.REQ_REGISTER,
                                          id=id, config=cfg))
        self.__recv_response(rpc_proto.RESP_EMPTY)

    def unregister_entity(self, id):
        self.__send_request(_make_request(type=rpc_proto.REQ_UNREGISTER,
                                          id=id))
        self.__recv_response(rpc_proto.RESP_EMPTY)

    def set_entity_config(self, id, cfg):
        self.__send_request(_make_request(type=rpc_proto.REQ_SET_CONFIG,
                                          id=id, config=cfg))
        self.__recv_response(rpc_proto.RESP_EMPTY)

    def get_entity_config(self, id):
        self.__send_request(_make_request(type=rpc_proto.REQ_GET_CONFIG,
                                          id=id))
        resp = self.__recv_response(rpc_proto.RESP_CONFIG)
        return _config_from_msg(resp)

    def get_entities(self):
        self.__send_request(_make_request(type=rpc_proto.REQ_LIST))
        resp = self.__recv_response(rpc_proto.RESP_LIST)
        lst = []
        for i in range(resp.list_len):
            resp = self.__recv_response(rpc_proto.RESP_CONFIG)
            lst.append((resp.id, _config_from_msg(resp)))
        return lst
