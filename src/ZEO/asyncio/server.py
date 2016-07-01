import asyncio
import json
import logging
import os
import random
import threading
import ZODB.POSException

logger = logging.getLogger(__name__)

from ..shortrepr import short_repr

from . import base
from .marshal import server_decode

class ServerProtocol(base.Protocol):
    """asyncio low-level ZEO server interface
    """

    protocols = (b'Z5', )

    name = 'server protocol'
    methods = set(('register', ))

    unlogged_exception_types = (
        ZODB.POSException.POSKeyError,
        )

    def __init__(self, loop, addr, zeo_storage):
        """Create a server's client interface
        """
        super(ServerProtocol, self).__init__(loop, addr)
        self.zeo_storage = zeo_storage

    closed = False
    def close(self):
        logger.debug("Closing server protocol")
        if not self.closed:
            self.closed = True
            if self.transport is not None:
                self.transport.close()

    connected = None # for tests
    def connection_made(self, transport):
        self.connected = True
        super(ServerProtocol, self).connection_made(transport)
        self._write(best_protocol_version)

    def connection_lost(self, exc):
        self.connected = False
        if exc:
            logger.error("Disconnected %s:%s", exc.__class__.__name__, exc)
        self.zeo_storage.notify_disconnected()

        self.loop.stop()

    def finish_connect(self, protocol_version):

        if protocol_version == b'ruok':
            self._write(json.dumps(self.zeo_storage.ruok()).encode("ascii"))
            self.close()
        else:
            if protocol_version in self.protocols:
                logger.info("received handshake %r" % protocol_version)
                self.protocol_version = protocol_version
                self.zeo_storage.notify_connected(self)
            else:
                logger.error("bad handshake %s" % short_repr(protocol_version))
                self.close()

    def call_soon_threadsafe(self, func, *args):
        try:
            self.loop.call_soon_threadsafe(func, *args)
        except RuntimeError:
            if self.connected:
                logger.exception("call_soon_threadsafe failed while connected")

    def message_received(self, message):
        try:
            message_id, async, name, args = server_decode(message)
        except Exception:
            logger.exception("Can't deserialize message")
            self.close()

        if message_id == -1:
            return # keep-alive

        if name not in self.methods:
            logger.error('Invalid method, %r', name)
            self.close()

        try:
            result = getattr(self.zeo_storage, name)(*args)
        except Exception as exc:
            if not isinstance(exc, self.unlogged_exception_types):
                logger.exception(
                    "Bad %srequest, %r", 'async ' if async else '', name)
            if async:
                return self.close() # No way to recover/cry for help
            else:
                return self.send_error(message_id, exc)

        if not async:
            self.send_reply(message_id, result)

    def send_reply(self, message_id, result, send_error=False):
        try:
            result = self.encode(message_id, 0, '.reply', result)
        except Exception:
            if isinstance(result, Delay):
                result.set_sender(message_id, self)
                return
            else:
                logger.exception("Unpicklable response %r", result)
                if not send_error:
                    self.send_error(
                        message_id,
                        ValueError("Couldn't pickle response"),
                        True)

        self._write(result)

    def send_reply_threadsafe(self, message_id, result):
        self.loop.call_soon_threadsafe(self.reply, message_id, result)

    def send_error(self, message_id, exc, send_error=False):
        """Abstracting here so we can make this cleaner in the future
        """
        self.send_reply(message_id, (exc.__class__, exc), send_error)

    def async(self, method, *args):
        self.call_async(method, args)

best_protocol_version = os.environ.get(
    'ZEO_SERVER_PROTOCOL',
    ServerProtocol.protocols[-1].decode('utf-8')).encode('utf-8')
assert best_protocol_version in ServerProtocol.protocols

def new_connection(loop, addr, socket, zeo_storage):
    protocol = ServerProtocol(loop, addr, zeo_storage)
    cr = loop.create_connection((lambda : protocol), sock=socket)
    asyncio.async(cr, loop=loop)

class Delay:
    """Used to delay response to client for synchronous calls.

    When a synchronous call is made and the original handler returns
    without handling the call, it returns a Delay object that prevents
    the mainloop from sending a response.
    """

    msgid = protocol = sent = None

    def set_sender(self, msgid, protocol):
        self.msgid = msgid
        self.protocol = protocol

    def reply(self, obj):
        self.sent = 'reply'
        self.protocol.send_reply(self.msgid, obj)

    def error(self, exc_info):
        self.sent = 'error'
        logger.error("Error raised in delayed method", exc_info=exc_info)
        self.protocol.send_error(self.msgid, exc_info[1])

    def __repr__(self):
        return "%s[%s, %r, %r, %r]" % (
            self.__class__.__name__, id(self),
            self.msgid, self.protocol, self.sent)

    def __reduce__(self):
        raise TypeError("Can't pickle delays.")

class Result(Delay):

    def __init__(self, *args):
        self.args = args

    def set_sender(self, msgid, protocol):
        reply, callback = self.args
        protocol.send_reply(msgid, reply)
        callback()

class MTDelay(Delay):

    def __init__(self):
        self.ready = threading.Event()

    def set_sender(self, *args):
        Delay.set_sender(self, *args)
        self.ready.set()

    def reply(self, obj):
        self.ready.wait()
        self.protocol.call_soon_threadsafe(
            self.protocol.send_reply, self.msgid, obj)

    def error(self, exc_info):
        self.ready.wait()
        self.protocol.call_soon_threadsafe(Delay.error, self, exc_info)
