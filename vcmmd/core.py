import errno
import logging
import os
import os.path
import struct
import threading

import util


class Error(Exception):

    def __init__(self, errcode, errmsg):
        Exception.__init__(self)
        self.errcode = errcode
        self.errmsg = errmsg

    def __str__(self):
        return "%s (errcode %d)" % (self.errmsg, self.errcode)


class LoadConfig:

    MAX_LIMIT = 18446744073709547520

    @staticmethod
    def __sanitize(val):
        val = max(val, 0)
        val = min(val, LoadConfig.MAX_LIMIT)
        return val

    def __init__(self, guarantee=0, limit=MAX_LIMIT,
                 swap_limit=MAX_LIMIT):
        self.guarantee = self.__sanitize(guarantee)
        self.limit = self.__sanitize(limit)
        self.swap_limit = self.__sanitize(swap_limit)

    @staticmethod
    def __strmemsize(val):
        if val == LoadConfig.MAX_LIMIT:
            return "unlim"
        return util.strmemsize(val)

    def __str__(self):
        return ("guar:%s mem:%s swp:%s" %
                (self.__strmemsize(self.guarantee),
                 self.__strmemsize(self.limit),
                 self.__strmemsize(self.swap_limit)))


class AbstractLoadEntity:

    def __init__(self, id):
        self.id = id
        self.config = None

    ##
    # Set config. May be overridden. May raise Error.

    def set_config(self, cfg):
        self.config = cfg

    ##
    # Update entity's runtime stats (such as memory usage). This function is
    # called by the load manager before calculating entities' runtime
    # configuration. May raise Error.

    def update(self):
        pass

    ##
    # Propagate entities' runtime configuration calculated by the load manager
    # to the underlying subsystem. May raise Error.

    def sync(self):
        pass

    ##
    # This function is called whenever the manager stops managing an entity. It
    # is supposed to be overridden by an inheriting class so that the latter
    # can do implementation defined cleanup. It is also called on each entity
    # on the manager shutdown. May raise Error.

    def reset(self):
        pass


class AbstractLoadManager:

    LoadEntityClass = None

    def __init__(self, state_filename=None, logger=None):
        self.state_filename = state_filename
        self.logger = logger or logging.getLogger(__name__)
        self.__entities = {}
        self.__lock = threading.RLock()
        self.__need_update = threading.Condition(self.__lock)
        self.__is_shut_down = threading.Event()
        self.__shutdown_request = False

    # To avoid loosing load configurations in case the service is restarted
    # (e.g. due to a failure), we store them in a file before stopping the
    # service loop. Since this file's lifetime is limited by the lifetime of
    # the system, we do not care about the file format.

    @staticmethod
    def __save_entity_state(fp, id, cfg):
        fp.write(struct.pack("=I%dsQQQ" % len(id), len(id), id,
                             cfg.guarantee, cfg.limit, cfg.swap_limit))

    @staticmethod
    def __load_entity_state(fp):
        fmt = "=I"
        s = fp.read(struct.calcsize(fmt))
        if not s:
            raise EOFError
        (id_len,) = struct.unpack(fmt, s)
        fmt = "=%ds" % id_len
        (id,) = struct.unpack(fmt, fp.read(struct.calcsize(fmt)))
        fmt = "=QQQ"
        (guarantee, limit, swap_limit) = struct.unpack(
            fmt, fp.read(struct.calcsize(fmt)))
        return id, LoadConfig(guarantee, limit, swap_limit)

    __STATE_TMPFILE_SUFFIX = ".tmp"

    def __save_state(self):
        filename = self.state_filename
        if not filename:
            return
        self.logger.debug("Saving manager state to file '%s'" % filename)
        try:
            # We can die while writing the state file. To avoid losing the
            # state in this case, save the new state to a temporary file, then
            # move it to the final location.
            tmpfile = filename + self.__STATE_TMPFILE_SUFFIX
            with open(tmpfile, 'wb') as fp:
                for id, cfg in self.get_entities():
                    self.__save_entity_state(fp, id, cfg)
            if os.path.exists(filename):
                os.remove(filename)
            os.rename(tmpfile, filename)
        except (IOError, OSError) as err:
            self.logger.error("Failed to save manager state: %s" % err)

    def __load_state(self):
        filename = self.state_filename
        if not filename:
            return
        if not os.path.exists(filename):
            # We could be killed after removing the old state file, but before
            # moving the temporary file storing the new state to the final
            # location (see __save_state). If so, move it.
            tmpfile = filename + self.__STATE_TMPFILE_SUFFIX
            try:
                os.rename(tmpfile, filename)
            except OSError:  # no tmpfile?
                return
        self.logger.debug("Loading manager state from file '%s'" % filename)
        entities = []
        try:
            with open(filename, 'rb') as fp:
                while True:
                    entities.append(self.__load_entity_state(fp))
        except EOFError:
            pass
        except IOError as err:
            self.logger.error("Failed to load manager state: %s" % err)
            return
        except struct.error:
            self.logger.error("Failed to load manager state: Invalid format")
            return
        if not entities:
            return
        self.logger.info("Restoring entities from the previous run:")
        n = 0
        for id, cfg in entities:
            try:
                self.__do_register_entity(id, cfg)
            except Error as err:
                self.logger.warning("Failed to register entity %s <%s>: %s" %
                                    (id, cfg, err))
                continue
            n += 1
        self.logger.info("%s entity(s) restored" % n)

    def __for_each_entity(self, method, errmsg):
        for e in self.__entities.values():
            try:
                method(e)
            except Error as err:
                self.logger.error((errmsg + ": %s") % (e.id, err))
                self.unregister_entity(e.id)

    ##
    # Placeholder for the load manager logic. It is supposed to set load
    # entities' internal parameters in accordance with their current demands
    # and the load manager policy. It is called under the load manager lock and
    # passed the list of registered entities. May be overridden. Must not raise
    # exceptions.

    def _do_update(self, entities):
        self.logger.debug("Managed entities: %s" %
                          "; ".join('%s <%s>' % (e.id, e.config)
                                    for e in entities))

    ##
    # Handle requests until shutdown.

    def serve_forever(self):
        self.__need_update.acquire()
        self.__is_shut_down.clear()
        try:
            self.__load_state()
            while not self.__shutdown_request:
                self.__for_each_entity(self.LoadEntityClass.update,
                                       "Failed to update entity %s")
                self._do_update(self.__entities.values())
                self.__for_each_entity(self.LoadEntityClass.sync,
                                       "Failed to sync entity %s")
                self.__need_update.wait()
            self.__save_state()
            self.__for_each_entity(self.LoadEntityClass.reset,
                                   "Failed to reset entity %s")
        finally:
            self.__shutdown_request = False
            self.__is_shut_down.set()
            self.__need_update.release()

    ##
    # Trigger load update.

    def update(self):
        with self.__lock:
            self.__need_update.notify()

    ##
    # Stop the serve_forever loop and wait until it exits.

    def shutdown(self):
        with self.__lock:
            self.__shutdown_request = True
            self.update()
        self.__is_shut_down.wait()

    def __do_register_entity(self, id, cfg):
        if id in self.__entities:
            raise Error(errno.EEXIST, "Entity already registered")
        e = self.LoadEntityClass(id)
        e.set_config(cfg)
        self.__entities[id] = e
        self.logger.info("Registered entity %s <%s>" % (e.id, e.config))

    ##
    # Register and configure a load entity.

    def register_entity(self, id, cfg):
        id = str(id)
        with self.__lock:
            self.__do_register_entity(id, cfg)
            self.__save_state()
            self.update()

    ##
    # Unregister a load entity.

    def unregister_entity(self, id):
        id = str(id)
        with self.__lock:
            e = self.__entities.get(id)
            if not e:
                raise Error(errno.ESRCH, "Entity is not registered")
            del self.__entities[id]
            try:
                e.reset()
            except Error as err:
                self.logger.warning("Failed to reset entity %s: %s" %
                                    (e.id, err))
            self.logger.info("Unregistered entity %s" % e.id)
            self.__save_state()
            self.update()

    ##
    # Update a load entity's config.

    def set_entity_config(self, id, cfg):
        id = str(id)
        with self.__lock:
            e = self.__entities.get(id)
            if not e:
                raise Error(errno.ESRCH, "Entity is not registered")
            e.set_config(cfg)
            self.logger.info("Updated entity %s <%s>" % (e.id, e.config))
            self.__save_state()
            self.update()

    ##
    # Get a load entity's config.

    def get_entity_config(self, id):
        id = str(id)
        with self.__lock:
            e = self.__entities.get(id)
            if not e:
                raise Error(errno.ESRCH, "Entity is not registered")
            cfg = self.__entities[id].config
        return cfg

    ##
    # Get a list of tuples containing id and config for all registered
    # entities.

    def get_entities(self):
        with self.__lock:
            lst = [(e.id, e.config) for e in self.__entities.values()]
        return lst
