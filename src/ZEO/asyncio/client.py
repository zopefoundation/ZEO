"""ZEO client interface implementation.

The client interface implementation is split into two parts:
``ClientRunner`` and ``ClientIO``.
``ClientRunner`` methods are executed in the calling thread,
``ClientIO`` methods in an ``asyncio`` context.

``ClientRunner`` calls ``ClientIO`` methods indirectly via
``loop.call_soon_threadsafe`` (the async calls
``call_async`` and ``call_async_iter``).
``ClientIO`` does not call ``ClientRunner`` methods; however, it
can call ``ClientStorage`` and ``ClientCache`` methods.
Those methods must be thread safe.

Logically, ``ClientIO`` represents a connection to one ZEO
server. However, initially, it can open connections to several
servers and choose one of them depending on availability
and required/provided capabilities (read_only/writable).
A server connection is represented by a ``Protocol`` instance.

The ``asyncio`` loop must be run in a separate thread.
The loop management is the responsibility of ``ClientThread``,
a tiny wrapper around ``ClientRunner``.
"""
import concurrent.futures
import functools
import logging
import random
import sys
import threading

import ZODB.event
import ZODB.POSException

import ZEO.Exceptions
from ZEO.Exceptions import ClientDisconnected, ServerException
import ZEO.interfaces

from . import base
from .compat import asyncio, new_event_loop
from .marshal import encoder, decoder

logger = logging.getLogger(__name__)

Fallback = object()

local_random = random.Random()  # use separate generator to facilitate tests

uvloop_used = "uvloop" in sys.modules


def future_generator(func):
    """Decorates a generator that generates futures
    """

    @functools.wraps(func)
    def call_generator(*args, **kw):
        gen = func(*args, **kw)
        try:
            f = next(gen)
        except StopIteration:
            gen.close()
        else:
            def store(gen, future):
                @future.add_done_callback
                def _(future):
                    try:
                        try:
                            result = future.result()
                        except Exception as exc:
                            f = gen.throw(exc)
                        else:
                            f = gen.send(result)
                    except StopIteration:
                        gen.close()
                    else:
                        store(gen, f)

            store(gen, f)

    return call_generator


class Protocol(base.ZEOBaseProtocol):
    """asyncio connection to a single ZEO server.
    """

    # All of the code in this class runs in a single dedicated
    # thread. Thus, we can mostly avoid worrying about interleaved
    # operations.

    # One place where special care was required was in cache setup on
    # connect. See finish connect below.

    protocols = b'5',

    def __init__(self, loop,
                 addr, client, storage_key, read_only, connect_poll=1,
                 heartbeat_interval=60, ssl=None, ssl_server_hostname=None,
                 credentials=None):
        """Create a server connection

        addr is either a host,port tuple or a string file name.

        client is a `ClientIO`.
        """
        super(Protocol, self).__init__(
            loop,
            "%r, %r, %r" % (addr, storage_key, read_only))
        self.addr = addr
        self.storage_key = storage_key
        self.read_only = read_only
        self.client = client
        self.connect_poll = connect_poll
        self.heartbeat_interval = heartbeat_interval
        self.futures = {}  # { message_id -> future }
        self.ssl = ssl
        self.ssl_server_hostname = ssl_server_hostname
        self.credentials = credentials

        self.connect()

    closed = False  # actively closed

    def close(self, exc=None):
        """schedule closing; register closing with the client."""
        if not self.closed:
            self.closed = True
            logger.debug('closing %s: %s', self, exc)
            connecting = not self._connecting.done()
            cancel_task(self._connecting)
            self._connecting = None  # break reference cycle
            for future in self.pop_futures():
                if not future.done():
                    future.set_exception(ClientDisconnected(exc or "Closed"))
            # Note: it is important (at least for the tests) that
            # the following call comes after the future cleanup (above).
            # The ``close`` below will close the transport which
            # will call ``connection_lost`` but without the ``exc``
            # information -- therefore, the futures would get the wrong
            # exception
            closing = super(Protocol, self).close()
            cfs = self.client.closing_protocol_futures
            cfs.add(closing)
            closing.add_done_callback(cfs.remove)
            # ``uvloop`` workaround:
            # The closing logic relies an the ``asyncio``
            # promise that ``connection_lost`` will be called
            # exactly once after ``connection_made`` has been called.
            # ``uvloop`` may not fulfill this promise when the
            # connecting process has been cancelled.
            # To avoid ``close`` to deadlock, we pretend that
            # closing has finished in this case. This might cause
            # the closing process to terminate too early and
            # leave resources (collected with the next garbage collection).
            if uvloop_used and connecting:
                if not closing.done():
                    closing.set_result(True)
            self.client = None  # break reference cycle

    def pop_futures(self):
        # Remove and return futures from self.futures.  The caller
        # will finalize them in some way and callbacks may modify
        # self.futures.
        futures = list(self.futures.values())
        self.futures.clear()
        return futures

    def connect(self):
        if isinstance(self.addr, tuple):
            host, port = self.addr
            cr = self.loop.create_connection(
                self.protocol_factory, host or '127.0.0.1', port,
                ssl=self.ssl, server_hostname=self.ssl_server_hostname)
        else:
            cr = self.loop.create_unix_connection(
                self.protocol_factory, self.addr, ssl=self.ssl)

        # an ``asyncio.Future``
        self._connecting = cr = asyncio.ensure_future(cr, loop=self.loop)

        @cr.add_done_callback
        def done_connecting(future):
            if future.cancelled():
                logger.info("Connection to %r cancelled", self.addr)
            elif future.exception() is not None:
                logger.info("Connection to %r failed, %s",
                            self.addr, future.exception())
            else:
                return

            # keep trying
            if not self.closed:
                logger.info("retry connecting %r", self.addr)
                self.loop.call_later(
                    self.connect_poll + local_random.random(),
                    self.connect,
                    )

    def connection_made(self, transport):
        logger.debug('connection_made %s', self)
        super(Protocol, self).connection_made(transport)
        self.heartbeat(write=False)

    def connection_lost(self, exc):
        logger.debug('connection_lost %s: %r', self, exc)
        super(Protocol, self).connection_lost(exc)
        assert self.closing.done()
        self.heartbeat_handle.cancel()
        if self.closed:  # ``connection_lost`` was expected
            for f in self.pop_futures():
                if not f.done():
                    f.set_exception(ClientDisconnected(exc or "Closed"))
        else:
            client = self.client
            # will set ``self.client = None``
            self.close(exc or "Connection lost")
            client.disconnected(self)

    @future_generator
    def finish_connection(self, protocol_version):
        """setup for *protocol_version* and verify the connection."""
        # the first byte of ``protocol_version`` specifies the coding type
        # the remaining bytes the version proper

        # The future implementation we use differs from
        # asyncio.Future in that callbacks are called immediately,
        # rather than using the loops call_soon.  We want to avoid a
        # race between invalidations and cache initialization. In
        # particular, after getting a response from lastTransaction or
        # getInvalidations, we want to make sure we set the cache's
        # lastTid before processing (and possibly missing) subsequent
        # invalidations.

        version = min(protocol_version[1:], self.protocols[-1])
        if version not in self.protocols:
            self.client.register_failed(
                self, ZEO.Exceptions.ProtocolError(protocol_version))
            return

        self.protocol_version = protocol_version[:1] + version
        self.encode = encoder(protocol_version)
        self.decode = decoder(protocol_version)
        self.heartbeat_bytes = self.encode(-1, 0, '.reply', None)
        self.write_message(self.protocol_version)

        credentials = (self.credentials,) if self.credentials else ()

        # We try to register with the server; if this succeeds with
        # the client.
        try:
            try:
                server_tid = yield self.fut(
                    'register', self.storage_key,
                    (self.read_only if self.read_only is not Fallback
                     else False),
                    *credentials)
            except ZODB.POSException.ReadOnlyError:
                if self.read_only is Fallback:
                    self.read_only = True
                    server_tid = yield self.fut(
                        'register', self.storage_key, True, *credentials)
                else:
                    raise
            else:
                if self.read_only is Fallback:
                    self.read_only = False
        except Exception as exc:
            self.client.register_failed(self, exc)
        else:
            self.client.registered(self, server_tid)

    exception_type_type = type(Exception)

    def message_received(self, data):
        msgid, async_, name, args = self.decode(data)
        if name == '.reply':
            future = self.futures.pop(msgid)
            if async_:  # ZEO 5 exception
                class_, args = args
                factory = exc_factories.get(class_)
                if factory:
                    exc = factory(class_, args)
                    if not isinstance(exc, unlogged_exceptions):
                        logger.error("%s from server: %s:%s",
                                     self.name, class_, args)
                else:
                    exc = ServerException(class_, args)
                future.set_exception(exc)
            elif (isinstance(args, tuple) and len(args) > 1 and
                  type(args[0]) == self.exception_type_type and
                  issubclass(args[0], Exception)
                  ):
                if not issubclass(args[0], unlogged_exceptions):
                    logger.error("%s from server: %s.%s:%s",
                                 self.name,
                                 args[0].__module__,
                                 args[0].__name__,
                                 args[1])
                future.set_exception(args[1])
            else:
                future.set_result(args)
        else:
            assert async_  # clients only get async calls
            if name in self.client_methods:
                getattr(self.client, name)(*args)
            else:
                raise AttributeError(name)

    message_id = 0

    def call(self, future, method, args):
        # The check below is important to handle a potential race
        # between a server call and ``close``.
        # In ``close``, all waiting futures get ``ClientDisconnected``
        # set, but if the call is processed after ``close``,
        # this does not happen there and needs to be handled here.
        if self.closed:
            raise ClientDisconnected("closed")
        self.message_id += 1
        self.futures[self.message_id] = future
        self.write_message(self.encode(self.message_id, False, method, args))
        return future

    def fut(self, method, *args):
        return self.call(Fut(), method, args)

    def load_before(self, oid, tid):
        # Special-case load_before, so we collapse outstanding requests
        # and update the cache
        message_id = (oid, tid)
        future = self.futures.get(message_id)
        if future is None:
            future = Fut()
            self.futures[message_id] = future
            self.write_message(
                self.encode(message_id, False, 'loadBefore', (oid, tid)))

            @future.add_done_callback
            def _(future):
                try:
                    data = future.result()
                except Exception:
                    return
                if data:
                    data, start, end = data
                    self.client.cache.store(oid, start, end, data)

        return future

    # Methods called by the server.
    # WARNING WARNING we can't call methods that call back to us
    # synchronously, as that would lead to DEADLOCK!

    client_methods = (
        'invalidateTransaction', 'info',
        'receiveBlobStart', 'receiveBlobChunk', 'receiveBlobStop',
        # plus: notify_connected, notify_disconnected
        )
    client_delegated = client_methods[1:]

    def heartbeat(self, write=True):
        if write:
            self.write_message(self.heartbeat_bytes)
        self.heartbeat_handle = self.loop.call_later(
            self.heartbeat_interval, self.heartbeat)


def create_Exception(class_, args):
    return exc_classes[class_](*args)


def create_ConflictError(class_, args):
    exc = exc_classes[class_](
        message=args['message'],
        oid=args['oid'],
        serials=args['serials'],
        )
    exc.class_name = args.get('class_name')
    return exc


def create_BTreesConflictError(class_, args):
    return ZODB.POSException.BTreesConflictError(
        p1=args['p1'],
        p2=args['p2'],
        p3=args['p3'],
        reason=args['reason'],
        )


def create_MultipleUndoErrors(class_, args):
    return ZODB.POSException.MultipleUndoErrors(args['_errs'])


exc_classes = {
    'builtins.KeyError': KeyError,
    'builtins.TypeError': TypeError,
    'exceptions.KeyError': KeyError,
    'exceptions.TypeError': TypeError,
    'ZODB.POSException.ConflictError': ZODB.POSException.ConflictError,
    'ZODB.POSException.POSKeyError': ZODB.POSException.POSKeyError,
    'ZODB.POSException.ReadConflictError': ZODB.POSException.ReadConflictError,
    'ZODB.POSException.ReadOnlyError': ZODB.POSException.ReadOnlyError,
    'ZODB.POSException.StorageTransactionError':
    ZODB.POSException.StorageTransactionError,
    }
exc_factories = {
    'builtins.KeyError': create_Exception,
    'builtins.TypeError': create_Exception,
    'exceptions.KeyError': create_Exception,
    'exceptions.TypeError': create_Exception,
    'ZODB.POSException.BTreesConflictError': create_BTreesConflictError,
    'ZODB.POSException.ConflictError': create_ConflictError,
    'ZODB.POSException.MultipleUndoErrors': create_MultipleUndoErrors,
    'ZODB.POSException.POSKeyError': create_Exception,
    'ZODB.POSException.ReadConflictError': create_ConflictError,
    'ZODB.POSException.ReadOnlyError': create_Exception,
    'ZODB.POSException.StorageTransactionError': create_Exception,
    }
unlogged_exceptions = (ZODB.POSException.POSKeyError,
                       ZODB.POSException.ConflictError)


class ClientIO(object):
    """asyncio low-level ZEO client interface."""

    # All of the code in this class runs in a single dedicated
    # thread. Thus, we can mostly avoid worrying about interleaved
    # operations.

    # One place where special care was required was in cache setup on
    # connect.

    protocol = None
    # ready can have three values:
    #   None=Never connected
    #   True=connected
    #   False=Disconnected
    # Note: ``True`` indicates only the first phase of readyness;
    #   it does not mean that we are fully ready.
    ready = None

    def __init__(self, loop,
                 addrs, client, cache, storage_key, read_only, connect_poll,
                 register_failed_poll=9,
                 ssl=None, ssl_server_hostname=None, credentials=None):
        """Create a client interface

        *addrs* specifies addresses of a set of servers which
        (essentially) serve the same data.
        Each address is either a host,port tuple or a string file name.
        The object tries to connect to each of them and
        chooses the first appropriate one.

        client is a ClientStorage. It must be thread safe.

        cache is a ZEO.interfaces.IClientCache.
        """
        self.loop = loop
        self.addrs = addrs
        self.storage_key = storage_key
        self.read_only = read_only
        self.connect_poll = connect_poll
        self.register_failed_poll = register_failed_poll
        self.client = client
        self.ssl = ssl
        self.ssl_server_hostname = ssl_server_hostname
        self.credentials = credentials
        for name in Protocol.client_delegated:
            setattr(self, name, getattr(client, name))
        self.cache = cache
        self.protocols = ()
        self.closing_protocol_futures = set()  # closing futures
        self.disconnected(None)

        # Protection against potentially odd behavior of a ZEO server: if it
        # may send invalidations for transactions later than the result of
        # getInvalidations, without queueing, client's cache might get out of
        # sync wrt data on the server.
        self.verify_invalidation_queue = []

    def new_addrs(self, addrs):
        self.addrs = addrs
        if self.trying_to_connect():
            self.disconnected(None)

    def trying_to_connect(self):
        """Return whether we're trying to connect

        Either because we're disconnected, or because we're connected
        read-only, but want a writable connection if we can get one.
        """
        return (not self.ready or
                self.is_read_only() and self.read_only is Fallback)

    closed = False

    def close(self):
        """schedule closing and return closed future."""
        if not self.closed:
            self.closed = True
            logger.debug("closing %s", self)
            self.ready = False
            self.connected.cancel()
            if self.protocol is not None:
                self.protocol.close()
            self.cache.close()
            self._clear_protocols()
        return asyncio.gather(*self.closing_protocol_futures)

    def _clear_protocols(self, protocol=None):
        for p in self.protocols:
            if p is not protocol:
                p.close()
        self.protocols = ()

    connected = None

    def manage_connected(self):
        """manage the future ``connected``.

        It is used to implement the connection timeout.
        """
        if self.connected is not None:
            self.connected.cancel()  # cancel waiters
        self.connected = base.create_future(self.loop)

    def disconnected(self, protocol=None):
        logger.debug('disconnected %r %r', self, protocol)
        if protocol is None or protocol is self.protocol:
            if protocol is self.protocol and protocol is not None:
                self.client.notify_disconnected()
            if self.ready:
                self.ready = False
            self.manage_connected()
            self.protocol = None
            self._clear_protocols()

        if all(p.closed for p in self.protocols):
            self.try_connecting()

    def upgrade(self, protocol):
        self.ready = False
        self.manage_connected()
        self.protocol.close()
        self.protocol = protocol
        self._clear_protocols(protocol)

    def try_connecting(self):
        logger.debug('try_connecting %s', self)
        if not self.closed:
            self.protocols = [
                Protocol(self.loop, addr, self,
                         self.storage_key, self.read_only, self.connect_poll,
                         ssl=self.ssl,
                         ssl_server_hostname=self.ssl_server_hostname,
                         credentials=self.credentials,
                         )
                for addr in self.addrs
                ]

    def registered(self, protocol, server_tid):
        if self.protocol is None:
            self.protocol = protocol
            if not (self.read_only is Fallback and protocol.read_only):
                # We're happy with this protocol. Tell the others to
                # stop trying.
                self._clear_protocols(protocol)
            self.verify(server_tid)
        elif (self.read_only is Fallback and not protocol.read_only and
              self.protocol.read_only):
            self.upgrade(protocol)
            self.verify(server_tid)
        else:
            protocol.close()  # too late, we went home with another

    def register_failed(self, protocol, exc):
        # A protocol failed registration. That's weird.  If they've all
        # failed, we should try again in a bit.
        if protocol is not self:
            protocol.close()
        logger.exception("Registration or cache validation failed, %s", exc)
        if self.protocol is None and \
           not any(not p.closed for p in self.protocols):
            self.loop.call_later(
                self.register_failed_poll + local_random.random(),
                self.try_connecting)

    verify_result = None  # for tests

    @future_generator
    def verify(self, server_tid):
        """cache verification and invalidation."""
        self.verify_invalidation_queue = []  # See comment in init :(

        protocol = self.protocol
        try:
            if server_tid is None:
                server_tid = yield protocol.fut('lastTransaction')

            cache = self.cache
            if cache:
                cache_tid = cache.getLastTid()
                if not cache_tid:
                    self.verify_result = "Non-empty cache w/o tid"
                    logger.error("Non-empty cache w/o tid -- clearing")
                    cache.clear()
                    self.client.invalidateCache()
                elif cache_tid > server_tid:
                    self.verify_result = "Cache newer than server"
                    logger.critical(
                        'Client cache is out of sync with the server. '
                        'Verify that this is expected and then remove '
                        'the cache file (usually a .zec file) '
                        'before restarting the server.')
                    raise AssertionError("Server behind client, %r < %r, %s",
                                         server_tid, cache_tid, protocol)
                elif cache_tid == server_tid:
                    self.verify_result = "Cache up to date"
                else:
                    vdata = yield protocol.fut('getInvalidations', cache_tid)
                    if vdata:
                        self.verify_result = "quick verification"
                        server_tid, oids = vdata
                        for oid in oids:
                            cache.invalidate(oid, None)
                        self.client.invalidateTransaction(server_tid, oids)
                    else:
                        # cache is too old
                        self.verify_result = "cache too old, clearing"
                        try:
                            ZODB.event.notify(
                                ZEO.interfaces.StaleCache(self.client))
                        except Exception:
                            logger.exception("sending StaleCache event")
                        logger.critical(
                            "%s dropping stale cache",
                            getattr(self.client, '__name__', ''),
                            )
                        self.cache.clear()
                        self.client.invalidateCache()
            else:
                self.verify_result = "empty cache"

        except Exception as exc:
            del self.protocol
            self.register_failed(protocol, exc)
        else:
            # The cache is validated and the last tid we got from the server.
            # Set ready so we apply any invalidations that follow.
            # We've been ignoring them up to this point.
            self.cache.setLastTid(server_tid)
            self.ready = True

            # See comment in __init__. :(
            for tid, oids in self.verify_invalidation_queue:
                if tid > server_tid:
                    self.invalidateTransaction(tid, oids)
            self.verify_invalidation_queue = []

            try:
                info = yield protocol.fut('get_info')
            except Exception as exc:
                # This is weird. We were connected and verified our cache, but
                # Now we errored getting info.

                # XXX Need a test for this. The lone before is what we
                # had, but it's wrong.
                self.register_failed(self, exc)

            else:
                # Note: it is important that we first inform
                # ``client`` (actually the ``ClientStorage``)
                # that we are (almost) connected
                # before we officially announce connectedness:
                # the ``notify_connected`` adds information vital
                # for storage use; the announcement
                # allows waiting threads to use the storage.
                # ``notify_connected`` can call our ``call_async``
                # but **MUST NOT** use other methods or the normal API
                # to interact with the server (deadlock or
                # ``ClientDisconnected`` would result).
                self.client.notify_connected(self, info)
                self.connected.set_result(None)  # signal full readyness

    def get_peername(self):
        return self.protocol.get_peername()

    def call_async_threadsafe(self, future, wait_ready, method, args):
        if self.ready:
            self.protocol.call_async(method, args)
            future.set_result(None)
        else:
            future.set_exception(ClientDisconnected())

    def call_async_from_same_thread(self, method, *args):
        return self.protocol.call_async(method, args)

    def call_async_iter_threadsafe(self, future, wait_ready, it):
        if self.ready:
            self.protocol.call_async_iter(it)
            future.set_result(None)
        else:
            future.set_exception(ClientDisconnected())

    def _when_ready(self, func, result_future, *args):

        if self.ready is None:
            # We started without waiting for a connection. (prob tests :( )
            result_future.set_exception(ClientDisconnected("never connected"))
        else:
            @self.connected.add_done_callback
            def done(future):
                if future.cancelled():
                    result_future.set_exception(ClientDisconnected())
                    return
                e = future.exception()
                if e is not None:
                    future.set_exception(e)
                else:
                    if self.ready:
                        func(result_future, *args)
                    else:
                        self._when_ready(func, result_future, *args)

    def call_threadsafe(self, future, wait_ready, method, args):
        if self.ready:
            self.protocol.call(future, method, args)
        elif wait_ready:
            self._when_ready(
                self.call_threadsafe, future, wait_ready, method, args)
        else:
            future.set_exception(ClientDisconnected())

    # Special methods because they update the cache.

    @future_generator
    def load_before_threadsafe(self, future, wait_ready, oid, tid):
        data = self.cache.loadBefore(oid, tid)
        if data is not None:
            future.set_result(data)
        elif self.ready:
            try:
                data = yield self.protocol.load_before(oid, tid)
            except Exception as exc:
                future.set_exception(exc)
            else:
                future.set_result(data)
        elif wait_ready:
            self._when_ready(
                self.load_before_threadsafe, future, wait_ready, oid, tid)
        else:
            future.set_exception(ClientDisconnected())

    @future_generator
    def _prefetch(self, oid, tid):
        try:
            yield self.protocol.load_before(oid, tid)
        except Exception:
            logger.exception("Exception for prefetch `%r` `%r`", oid, tid)

    def prefetch(self, future, wait_ready, oids, tid):
        if self.ready:
            for oid in oids:
                if self.cache.loadBefore(oid, tid) is None:
                    self._prefetch(oid, tid)

            future.set_result(None)
        else:
            future.set_exception(ClientDisconnected())

    @future_generator
    def tpc_finish_threadsafe(self, future, wait_ready, tid, updates, f):
        if self.ready:
            try:
                tid = yield self.protocol.fut('tpc_finish', tid)
                cache = self.cache
                # The cache invalidation here and that in
                # ``invalidateTransaction`` are both performed
                # in the IO thread. Thus there is no interference.
                # Other threads might observe a partially invalidated
                # cache. However, regular loads will access
                # object state before ``tid``; therefore,
                # partial invalidation for ``tid`` should not harm.
                for oid, data, resolved in updates:
                    cache.invalidate(oid, tid)
                    if data and not resolved:
                        cache.store(oid, tid, None, data)
                # ZODB >= 5.6 requires that ``lastTransaction`` changes
                # only after invalidation processing (performed in
                # the ``f`` call below) (for ``ZEO``, ``lastTransaction``
                # is implemented as ``cache.getLastTid()``).
                # Some tests involve ``f`` in the verification that
                # ``tpc_finish`` modifies ``lastTransaction`` and require
                # that ``cache.setLastTid`` is called before ``f``.
                # We use locking below to ensure that the
                # effect of ``setLastTid`` is observable by other
                # threads only after ``f`` has been called.
                with cache._lock:
                    cache.setLastTid(tid)
                    f(tid)
                future.set_result(tid)
            except Exception as exc:
                future.set_exception(exc)

                # At this point, our cache is in an inconsistent
                # state.  We need to reconnect in hopes of
                # recovering to a consistent state.
                self.protocol.close()
                self.disconnected(self.protocol)
        else:
            future.set_exception(ClientDisconnected())

    def close_threadsafe(self, future, _):
        # The following ``close`` deadlock''ked in isolated tests.
        # Prevent this with a timeout and log information
        # to understand the bug.
        closing = self.close()
        # the ``shield`` is necessary to keep the state unchanged
        twait = self.loop.create_task(
                    asyncio.wait_for(asyncio.shield(closing), 3))

        @twait.add_done_callback
        def _(f):
            if f.cancelled():
                future.cancel()
            elif f.exception() is not None:
                if isinstance(f.exception(), asyncio.TimeoutError):
                    from pprint import pformat
                    info = {"client": pformat(vars(self))}
                    if self.protocol is not None:
                        info["protocol"] = pformat(vars(self.protocol))
                    if self.protocols:
                        info["protocols"] = pformat([pformat(vars(p))
                                                     for p in self.protocols])
                    logger.error(
                       "closing did not finish within a reasonable time.\n"
                       "Please report this as a bug with the following info:\n"
                       "%s", pformat(info))
                future.set_exception(f.exception())
            else:
                future.set_result(None)
            # break reference cycles
            for name in Protocol.client_delegated:
                delattr(self, name)
            self.client = self.cache = None

    # server callbacks
    def invalidateTransaction(self, tid, oids):
        if self.ready:
            # see the cache related comment in ``tpc_finish_threadsafe``
            # why we think that locking is not necessary at this place
            for oid in oids:
                self.cache.invalidate(oid, tid)
            self.client.invalidateTransaction(tid, oids)
            self.cache.setLastTid(tid)
        else:
            self.verify_invalidation_queue.append((tid, oids))

    @property
    def protocol_version(self):
        return self.protocol.protocol_version

    def is_read_only(self):
        protocol = self.protocol
        return self.read_only if protocol is None else protocol.read_only


class ClientRunner(object):

    def set_options(self, addrs, wrapper, cache, storage_key, read_only,
                    timeout=30, disconnect_poll=1,
                    **kwargs):
        self.__args = (addrs, wrapper, cache, storage_key, read_only,
                       disconnect_poll)
        self.__kwargs = kwargs
        self.timeout = timeout

    def setup_delegation(self, loop):
        self.loop = loop
        self.client = ClientIO(loop, *self.__args, **self.__kwargs)
        self.call_threadsafe = self.client.call_threadsafe
        self.call_async_threadsafe = self.client.call_async_threadsafe

        from concurrent.futures import Future
        call_soon_threadsafe = loop.call_soon_threadsafe

        def io_call(meth, *args, **kw):
            timeout = kw.pop('timeout', None)
            wait = kw.pop('wait', True)
            assert not kw
            if timeout is not None:
                assert wait

            # Some explanation of the code below.
            # Timeouts on Python 2 are expensive, so we try to avoid
            # them if we're connected.  The 3rd argument below is a
            # wait_ready flag.  If false, and we're disconnected, we fail
            # immediately. If that happens, then we try again with the
            # wait flag set to True and wait with the default timeout.
            result = Future()
            if not wait:
                call_soon_threadsafe(meth, result, True, *args)
                return result

            call_soon_threadsafe(meth, result, timeout is not None, *args)
            try:
                return self.wait_for_result(result, timeout)
            except ClientDisconnected:
                if timeout is None:
                    result = Future()
                    call_soon_threadsafe(meth, result, True, *args)
                    return self.wait_for_result(result, self.timeout)
                else:
                    raise
            finally:
                del result  # break reference cycle in case of exception

        self.io_call = io_call  # creates reference cycle

    def wait_for_result(self, future, timeout):
        try:
            return future.result(timeout)
        except concurrent.futures.TimeoutError:
            if not self.client.ready:
                raise ClientDisconnected("timed out waiting for connection")
            else:
                raise

    def call(self, method, *args, **kw):
        """call method named *method* with *args*.

        Supported keywords:

          wait
             false means return future rather than wait for result
             default: ``True``

          timeout
             wait at most this long for readyness
             ``None`` is replaced by ``self.timeout`` (usually 30s)
             default: ``None``
        """
        return self.io_call(self.call_threadsafe, method, args, **kw)

    def call_future(self, method, *args):
        # for tests
        result = concurrent.futures.Future()
        self.loop.call_soon_threadsafe(
            self.call_threadsafe, result, True, method, args)
        return result

    def async_(self, method, *args):
        """call method named *method* with *args* asynchronously."""
        return self.io_call(self.call_async_threadsafe, method, args)

    def async_iter(self, it):
        return self.io_call(self.client.call_async_iter_threadsafe, it)

    def prefetch(self, oids, tid):
        return self.io_call(self.client.prefetch, oids, tid)

    def load_before(self, oid, tid):
        return self.io_call(self.client.load_before_threadsafe, oid, tid)

    def tpc_finish(self, tid, updates, f):
        return self.io_call(self.client.tpc_finish_threadsafe, tid, updates, f)

    def is_connected(self):
        return self.client.ready

    def is_read_only(self):
        protocol = self.client.protocol
        if protocol is None:
            return True
        return protocol.read_only

    # Some tests will set this to use an instrumented loop
    loop = None

    __closed = False

    def close(self):
        # Small race condition risk if ``close`` is called concurrently
        if self.__closed:
            return
        self.__closed = True
        call = self.io_call
        self.io_call = self._io_call_after_closure
        loop = self.loop
        if loop is None or loop.is_closed():  # pragma: no cover
            # this should not happen
            return
        if loop.is_running():
            call(self.client.close_threadsafe, wait=True)
        else:  # pragma: no cover
            # this should not happen
            f = base.create_future(loop)
            self.client.close_threadsafe(f, False)
            loop.run_until_complete(f)
        self.__args = None  # break reference cycle

    @staticmethod
    def _io_call_after_closure(*args, **kw):
        """auxiliary method to be used as `io_call` after closure."""
        raise ClientDisconnected('closed')

    def apply_threadsafe(self, future, wait_ready, func, *args):
        try:
            future.set_result(func(*args))
        except Exception as exc:
            future.set_exception(exc)

    def new_addrs(self, addrs):
        # This usually doesn't have an immediate effect, since the
        # addrs aren't used until the client disconnects.xs
        self.io_call(self.apply_threadsafe, self.client.new_addrs, addrs)

    def wait(self, timeout=None):
        """wait for readyness"""
        if timeout is None:
            timeout = self.timeout

        def _(future, _):
            @self.client.connected.add_done_callback
            def _(fconn):
                if fconn.cancelled():
                    future.cancel()
                elif fconn.exception() is not None:
                    future.set_exception(fconn.exception())
                else:
                    future.set_result(None)
        self.io_call(_, timeout=timeout)


class ClientThread(ClientRunner):
    """Thread wrapper for client interface

    A ClientProtocol is run in a dedicated thread.

    Calls to it are made in a thread-safe fashion.
    """

    def __init__(self, addrs, client, cache,
                 storage_key='1', read_only=False, timeout=30,
                 disconnect_poll=1, ssl=None, ssl_server_hostname=None,
                 credentials=None):
        self.set_options(addrs, client, cache, storage_key, read_only,
                         timeout, disconnect_poll,
                         ssl=ssl, ssl_server_hostname=ssl_server_hostname,
                         credentials=credentials)
        self.thread = threading.Thread(
            target=self.run_io_thread,
            name="%s zeo client networking thread" % client.__name__,
            )
        self.thread.daemon = True
        self.started = threading.Event()
        self.thread.start()
        self.started.wait()
        if self.exception:
            raise self.exception

    exception = None

    def run_io_thread(self):
        try:
            loop = self.loop if self.loop is not None else new_event_loop()
            asyncio.set_event_loop(loop)
            self.setup_delegation(loop)
            self.started.set()
            loop.run_forever()
        except Exception as exc:
            logger.exception("Client thread")
            self.exception = exc
            self.started.set()
        finally:
            if not self.__closed:
                super(ClientThread, self).close()
                logger.critical("Client loop stopped unexpectedly")
            loop.close()
            logger.debug('Stopping client thread')

    __closed = False

    def close(self):
        """close the server connection and release resources.

        ``close`` can be called at any moment; it should not
        raise an exception. Calling ``close`` again does
        not have an effect. Most other calls will raise
        a ``ClientDisconnected`` exception.
        """
        if not self.__closed:
            self.__closed = True
            loop = self.loop
            if loop is None:  # pragma no cover
                # we have never been connected
                return
            super(ClientThread, self).close()
            if loop.is_running():
                loop.call_soon_threadsafe(loop.stop)
                # ``loop`` will be closed in the IO thread
                # after stop processing
            self.thread.join(9)  # wait for the IO thread to terminate
        if self.exception:
            try:
                raise self.exception
            finally:
                self.exception = None  # break reference cycle

    def is_closed(self):
        return self.__closed


_missing = object()


class Fut(object):
    """Lightweight future that calls it's callbacks immediately ...

    rather than soon.
    """

    def __init__(self):
        self.cbv = []

    def add_done_callback(self, cb):
        self.cbv.append(cb)

    _result = _missing
    exc = None

    def set_exception(self, exc):
        self.exc = exc
        for cb in self.cbv:
            cb(self)

    def set_result(self, result):
        self._result = result
        for cb in self.cbv:
            cb(self)

    def result(self):
        if self.exc:
            raise self.exc
        else:
            return self._result

    def done(self):
        return (self._result is not _missing) or (self.exc is not None)


def cancel_task(task):
    task.cancel()
    # With Python before 3.8, cancelation is not sufficient to
    # ignore a potential exception -- eat it in a done callback
    task.add_done_callback(
        lambda future: future.cancelled() or future.exception())
