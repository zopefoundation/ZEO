import json
import logging
import os
import random
import threading
import ZODB.POSException

logger = logging.getLogger(__name__)

from ..shortrepr import short_repr

from . import base
from .compat import asyncio, new_event_loop
from .marshal import server_decoder, encoder, reduce_exception

class ServerProtocol(base.Protocol):
    """asyncio low-level ZEO server interface
    """

    protocols = (b'5', )

    name = 'server protocol'
    methods = set(('register', ))

    unlogged_exception_types = (
        ZODB.POSException.POSKeyError,
        )

    def __init__(self, loop, addr, zeo_storage, msgpack):
        """Create a server's client interface
        """
        super(ServerProtocol, self).__init__(loop, addr)
        self.zeo_storage = zeo_storage

        self.announce_protocol = (
          (b'M' if msgpack else b'Z') + best_protocol_version
        )

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
        self._write(self.announce_protocol)

    def connection_lost(self, exc):
        self.connected = False
        if exc:
            logger.error("Disconnected %s:%s", exc.__class__.__name__, exc)
        self.zeo_storage.notify_disconnected()
        self.stop()

    def stop(self):
        pass # Might be replaced when running a thread per client

    def finish_connect(self, protocol_version):
        if protocol_version == b'ruok':
            self._write(json.dumps(self.zeo_storage.ruok()).encode("ascii"))
            self.close()
        else:
            version = protocol_version[1:]
            if version in self.protocols:
                logger.info("received handshake %r" %
                            str(protocol_version.decode('ascii')))
                self.protocol_version = protocol_version
                self.encode = encoder(protocol_version, True)
                self.decode = server_decoder(protocol_version)
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
            message_id, async_, name, args = self.decode(message)
        except Exception:
            logger.exception("Can't deserialize message")
            self.close()
            return

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
                    "Bad %srequest, %r", 'async ' if async_ else '', name)
            if async_:
                return self.close() # No way to recover/cry for help
            else:
                return self.send_error(message_id, exc)

        if not async_:
            self.send_reply(message_id, result)

    def send_reply(self, message_id, result, send_error=False, flag=0):
        try:
            result = self.encode(message_id, flag, '.reply', result)
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
        self.send_reply(message_id, reduce_exception(exc), send_error, 2)

    def async_(self, method, *args):
        self.call_async(method, args)

    def async_threadsafe(self, method, *args):
        self.call_soon_threadsafe(self.call_async, method, args)

best_protocol_version = os.environ.get(
    'ZEO_SERVER_PROTOCOL',
    ServerProtocol.protocols[-1].decode('utf-8')).encode('utf-8')
assert best_protocol_version in ServerProtocol.protocols

def new_connection(loop, addr, socket, zeo_storage, msgpack):
    protocol = ServerProtocol(loop, addr, zeo_storage, msgpack)
    cr = loop.create_connection((lambda : protocol), sock=socket)
    asyncio.ensure_future(cr, loop=loop)

class Delay(object):
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
        if self.protocol:
            self.protocol.send_reply(self.msgid, obj)

    def error(self, exc_info):
        self.sent = 'error'
        logger.error("Error raised in delayed method", exc_info=exc_info)
        if self.protocol:
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


class Acceptor(object):

    def __init__(self, storage_server, addr, ssl, msgpack):
        self.storage_server = storage_server
        self.addr = addr
        self.ssl_context = ssl
        self.msgpack = msgpack
        self.event_loop = loop = new_event_loop()

        if isinstance(addr, tuple):
            cr = loop.create_server(self.factory, addr[0], addr[1],
                                    reuse_address=True, ssl=ssl)
        else:
            cr = loop.create_unix_server(self.factory, addr, ssl=ssl)

        f = asyncio.ensure_future(cr, loop=loop)
        server = loop.run_until_complete(f)

        self.server = server
        if isinstance(addr, tuple) and addr[1] == 0:
            addrs = [s.getsockname() for s in server.sockets]
            addrs = [a for a in addrs if len(a) == len(addr)]
            if addrs:
                self.addr = addrs[0]
            else:
                self.addr = server.sockets[0].getsockname()[:len(addr)]

        logger.info("listening on %s", str(addr))

    def factory(self):
        try:
            logger.debug("Accepted connection")
            zs = self.storage_server.create_client_handler()
            protocol = ServerProtocol(
                self.event_loop, self.addr, zs, self.msgpack)
        except Exception:
            logger.exception("Failure in protocol factory")

        return protocol

    def loop(self, timeout=None):
        self.event_loop.run_forever()
        self.event_loop.close()

    closed = False
    def close(self):
        if not self.closed:
            self.closed = True
            self.event_loop.call_soon_threadsafe(self._close)

    def _close(self):
        loop = self.event_loop

        self.server.close()

        f = asyncio.ensure_future(self.server.wait_closed(), loop=loop)
        @f.add_done_callback
        def server_closed(f):
            # stop the loop when the server closes:
            loop.call_soon(loop.stop)

        def timeout():
            logger.warning("Timed out closing asyncio.Server")
            loop.call_soon(loop.stop)

        # But if the server doesn't close in a second, stop the loop anyway.
        loop.call_later(1, timeout)
