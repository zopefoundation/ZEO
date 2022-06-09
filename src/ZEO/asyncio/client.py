"""ZEO client interface implementation.

The client interface implementation is split into two parts:
``ClientRunner`` and ``ClientIo``.
``ClientRunner`` methods are executed in the calling thread,
``ClientIo`` methods in an ``asyncio`` context.

``ClientRunner`` calls ``ClientIo`` methods indirectly via
``asyncio.run_coroutine_threadsafe``; methods designed for such
calls have suffix ``_co``.
``ClientIo`` does not call ``ClientRunner`` methods; however, it
can call ``ClientStorage`` and ``ClientCache`` methods.
Those methods must be thread safe.

Logically, ``ClientIo`` represents a connection to one ZEO
server. However, initially, it can open connections to serveral
servers and choose one of them depending on availability
and required/provided capabilities (read_only/writable).
A server connection is represented by a ``Protocol`` instance.

The ``asyncio`` loop must be run in a separate thread.
The loop management is the responsibility of ``ClientThread``,
a tiny wrapper arount ``ClientRunner``.
"""

import asyncio
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
from .compat import new_event_loop
from .marshal import encoder, decoder
from .optimize import Future, AsyncTask as Task
from .optimize import run_coroutine_threadsafe

logger = logging.getLogger(__name__)

Fallback = object()

local_random = random.Random()  # use separate generator to facilitate tests

uvloop_used = "uvloop" in sys.modules


class Protocol(base.ZEOBaseProtocol):
    """asyncio connection to a single ZEO server.
    """

    # All of the code in this class runs in a single dedicated
    # thread. Thus, we can mostly avoid worrying about interleaved
    # operations.

    # One place where special care was required was in cache setup on
    # connect. See finish connect below.

    protocols = b'309', b'310', b'3101', b'4', b'5'

    def __init__(self, loop,
                 addr, client, storage_key, read_only, connect_poll=1,
                 heartbeat_interval=60, ssl=None, ssl_server_hostname=None,
                 ):
        """Create a server connection

        addr is either a host,port tuple or a string file name.

        client is a `ClientIo`
        """
        super().__init__(
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
        self.invalidations = []

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
            if self.verify_task is not None:
                cancel_task(self.verify_task)
                # even cancelled, the task retains a reference to
                # the coroutine which creates a reference cycle
                # break it
                self.verify_task = None
            for future in self.pop_futures():
                if not future.done():
                    future.set_exception(ClientDisconnected(exc or "Closed"))
            # Note: it is important (at least for the tests) that
            # the following call comes after the future cleanup (above).
            # The ``close`` below will close the transport which
            # will call ``connection_lost`` but without the ``exc``
            # information -- therefore, the futures would get the wrong
            # exception
            closing = super().close()
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
            cr = lambda: self.loop.create_connection(  # noqa: E731
                self.protocol_factory, host or '127.0.0.1', port,
                ssl=self.ssl, server_hostname=self.ssl_server_hostname)
        else:
            cr = lambda: self.loop.create_unix_connection(  # noqa: E731
                self.protocol_factory, self.addr, ssl=self.ssl)

        async def connect():
            while not self.closed:
                try:
                    return await cr()
                except ConnectionError as exc:
                    logger.info("Connection to %r failed, %r",
                                self.addr, exc)
                await asyncio.sleep(self.connect_poll + local_random.random())
                logger.info("retry connecting %r", self.addr)

        self._connecting = self.loop.create_task(connect())

    def connection_made(self, transport):
        logger.debug('connection_made %s', self)
        super().connection_made(transport)
        self.heartbeat(write=False)

    def connection_lost(self, exc):
        logger.debug('connection_lost %s: %r', self, exc)
        super().connection_lost(exc)
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

    verify_task = None

    def finish_connection(self, protocol_version):
        """setup for *protocol_version* and verify the connection."""
        # the first byte of ``protocol_version`` specifies the coding type
        # the remaining bytes the version proper
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
        self.verify_task = self.loop.create_task(self.verify_connection())

    async def verify_connection(self):
        """verify the connection -- run as task.

        We try to register with the server; if this succeeds with
        the client.
        """
        # we do not want that serveral servers concurrently
        # update the cache -- lock
        async with self.client.register_lock:
            try:
                try:
                    server_tid = await self.call_sync(
                        'register', self.storage_key,
                        self.read_only if self.read_only is not Fallback
                        else False,
                        )
                except ZODB.POSException.ReadOnlyError:
                    if self.read_only is Fallback:
                        self.read_only = True
                        server_tid = await self.call_sync(
                            'register', self.storage_key, True)
                    else:
                        raise
                else:
                    if self.read_only is Fallback:
                        self.read_only = False
            except Exception as exc:
                self.client.register_failed(self, exc)
            else:
                # from now on invalidation messages can arrive
                await self.client.register(self, server_tid)

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
            if name not in self.client_methods:
                raise AttributeError(name)
            self.process_async(name, args)

    def process_async(self, name, args):
        """process the asynchronous call *name*(*args*)."""
        if name == "invalidateTransaction" and not self.client.ready:
            # the client is not yet ready to process invalidations
            # queue them
            self.invalidations.append(args)
        else:
            getattr(self.client, name)(*args)

    message_id = 0

    def call(self, future, method, args, message_id=None):
        # The check below is important to handle a potential race
        # between a server call and ``close``.
        # In ``close``, all waiting futures get ``ClientDisconnected``
        # set, but if the call is processed after ``close``,
        # this does not happen there and needs to be handled here.
        if self.closed:
            raise ClientDisconnected("closed")
        if message_id is None:
            self.message_id += 1
            message_id = self.message_id
        self.futures[message_id] = future
        self.write_message(self.encode(message_id, False, method, args))
        return future

    def call_sync(self, method, *args, message_id=None):
        return self.call(Future(loop=self.loop), method, args, message_id)

    def load_before(self, oid, tid):
        # Special-case loadBefore, so we collapse outstanding requests
        message_id = (oid, tid)
        future = self.futures.get(message_id)
        if future is None:
            future = Future(loop=self.loop)
            # Check whether the cache contains the information.
            # I am not sure whether the cache lookup it really
            # necessary at this place (it has already been
            # done in ``ClientStorage.loadBefore`` and therefore
            # will likely fail here).
            # The lookup at this place, however, guarantees
            # (together with the folding of identical requests above)
            # that we will not store data already in the cache.
            # Maybe, this is important
            cache = self.client.cache
            data = cache.loadBefore(oid, tid)
            if data:
                future.set_result(data)
            else:
                # data not in the cache
                # ensure the cache gets updated when the answer arrives
                @future.add_done_callback
                def store(future):
                    if future.cancelled() or future.exception() is not None:
                        return
                    data = future.result()
                    if data:
                        state, start, end = data
                        cache.store(oid, start, end, state)

                self.call(future, 'loadBefore', message_id, message_id)
        return future

    # Methods called by the server.
    # WARNING WARNING we can't call methods that call back to us
    # syncronously, as that would lead to DEADLOCK!

    client_methods = (
        'invalidateTransaction', 'serialnos', 'info',
        'receiveBlobStart', 'receiveBlobChunk', 'receiveBlobStop',
        # plus: notify_connected, notify_disconnected
        )
    client_delegated = client_methods[2:]

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


class ClientIo(object):
    """asyncio low-level ZEO client interface
    """

    # All of the code in this class runs in a single dedicated
    # thread. Thus, we can mostly avoid worrying about interleaved
    # operations.

    # One place where special care was required was in cache setup on
    # connect.

    protocol = None
    protocols = ()
    client_label = None  # for some log messages
    # ready can have three values:
    #   None=Never connected
    #   True=connected
    #   False=Disconnected
    # Note: ``True`` indicates only the first phase of readyness;
    #   it does not mean that we are "fully ready".
    ready = None
    operational = False  # fully ready?

    def __init__(self, loop,
                 addrs, client, cache, storage_key, read_only, connect_poll,
                 register_failed_poll=9,
                 ssl=None, ssl_server_hostname=None):
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
        for name in Protocol.client_delegated:
            setattr(self, name, getattr(client, name))
        self.cache = cache
        self.register_lock = asyncio.Lock()
        self.closing_protocol_futures = set()  # closing futures
        self.disconnected(None)

    def trying_to_connect(self):
        """Return whether we're trying to connect

        Either because we're disconnected, or because we're connected
        read-only, but want a writable connection if we can get one.
        """
        return (not self.operational or
                self.is_read_only() and self.read_only is Fallback)

    closed = False

    def close(self):
        """schedule closing and return closed future."""
        if not self.closed:
            self.closed = True
            logger.debug("closing %s", self)
            self.ready = self.operational = False
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
        self.connected = self.loop.create_future()

    def disconnected(self, protocol=None):
        logger.debug('disconnected %r %r', self, protocol)
        if protocol is None or protocol is self.protocol:
            if protocol is self.protocol and protocol is not None:
                self.client.notify_disconnected()
            if self.ready:
                self.ready = self.operational = False
            self.manage_connected()
            self.protocol = None
            self._clear_protocols()

        if all(p.closed for p in self.protocols):
            self.try_connecting()

    def upgrade(self, protocol):
        self.ready = self.operational = False
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
                         )
                for addr in self.addrs
                ]

    async def register(self, protocol, server_tid):
        """register *protocol* -- run as task."""
        if self.protocol is None:
            self.protocol = protocol
            if not (self.read_only is Fallback and protocol.read_only):
                # We're happy with this protocol. Tell the others to
                # stop trying.
                self._clear_protocols(protocol)
            await self.verify(server_tid)
        elif (self.read_only is Fallback and not protocol.read_only and
              self.protocol.read_only):
            self.upgrade(protocol)
            await self.verify(server_tid)
        else:
            protocol.close()  # too late, we went home with another

    def register_failed(self, protocol, exc):
        # A protocol failed registration. That's weird.  If they've all
        # failed, we should try again in a bit.
        protocol.close()
        logger.exception("Registration or cache validation failed, %s", exc)
        if self.protocol is None and \
           not any(not p.closed for p in self.protocols):
            self.loop.call_later(
                self.register_failed_poll + local_random.random(),
                self.try_connecting)

    verify_result = None  # for tests

    async def verify(self, server_tid):
        """cache verification and invalidation -- run as task."""
        protocol = self.protocol
        call = protocol.call_sync
        try:
            if server_tid is None:
                server_tid = await call('lastTransaction')

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
                    vdata = await call('getInvalidations', cache_tid)
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
            # The cache is validated wrt the last tid we got from the server.
            self.cache.setLastTid(server_tid)

            # Process queued invalidations
            # Note: new invalidations will only be seen
            # after the coroutine gives up control, i.e. surely
            # after the following loop and the ``ready = True``.
            # Thus, we do no lose invalidations.
            for tid, oids in protocol.invalidations:
                if tid > server_tid:
                    self.invalidateTransaction(tid, oids)

            # Set ready so arriving invalidations are no longer queued.
            # Note: we are not yet fully ready.
            self.ready = True

            try:
                info = await call('get_info')
            except Exception as exc:
                # This is weird. We were connected and verified our cache, but
                # Now we errored getting info.

                # XXX Need a test fpr this. The lone before is what we
                # had, but it's wrong.
                self.register_failed(protocol, exc)

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
                self.operational = True

    def get_peername(self):
        return self.protocol.get_peername()

    def call_sync(self, method, *args):
        return self.protocol.call_sync(method, *args)

    def call_async(self, method, *args):
        return self.protocol.call_async(method, args)

    def call_async_iter(self, it):
        return self.protocol.call_async_iter(it)

    # methods called with ``run_coroutine_threadsafe``
    # recognizable by their ``_co`` suffix
    async def call_with_timeout_co(self, timeout, method, args=(),
                                   init_ok=False):
        """call *method* with *args* when ready.

        Wait at most *timeout* for readyness.
        Usually, we do not wait for the initial readyness
        unless *init_ok*.

        The *method* call is assumed to either return ``None``
        or a future (or raise an exception).
        If a future is returned, we wait for it and return its result.
        """
        # Quick check for the normal case (already fully connected)
        # Note: `ready` can be set before we are truely ready.
        # We check it nevertheless because this is cheaper
        # than `not self.connected.cancelled()`.
        if self.operational:
            result = method(*args)
        elif timeout and (init_ok or self.ready is not None):
            try:
                await asyncio.wait_for(asyncio.shield(self.connected), timeout)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                raise ClientDisconnected
            # potential race condition: we have been connected
            # but may meanwhile have lost the connection again.
            result = method(*args)
        else:
            raise ClientDisconnected
        try:
            return result if result is None else await result
        finally:
            del result  # avoid reference cycle in case of exception

    async def close_co(self):
        # The following ``close`` deadlocked in isolated tests.
        # Prevent this with a timeout and log information
        # to understand the bug.
        # await self.close()
        closing = self.close()
        try:
            # the ``shield`` is necessary to keep the state unchanged
            await asyncio.wait_for(asyncio.shield(closing), 3)
        except asyncio.TimeoutError:
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
            raise
        # break reference cycles
        for name in Protocol.client_delegated:
            delattr(self, name)
        self.client = self.cache = None

    async def new_addrs_co(self, addrs):
        self.addrs = addrs
        if self.trying_to_connect():
            self.disconnected(None)

    # Special methods because they update the cache.

    async def load_before_co(self, timeout, oid, tid):
        if self.operational:
            future = self.protocol.load_before(oid, tid)
        else:
            # the following cache lookup is not necessary in
            # real life: ``ClientStorage.loadBefore`` has
            # already done it (and failed); we will fail, too --
            # with high probability
            # But a test relies on this cache lookup.
            data = self.cache.loadBefore(oid, tid)
            if data:
                return data
            if timeout:
                def callback():
                    """helper to delay protocol access until readyness."""
                    return self.protocol.load_before(oid, tid)
                future = self.call_with_timeout_co(timeout, callback)
            else:
                raise ClientDisconnected
        try:
            return await future
        finally:
            del future  # avoid reference cycle in case of exception

    async def prefetch_co(self, oids, tid):
        async def prefetch(oid):
            try:
                await self.load_before_co(0, oid, tid)
            except ClientDisconnected:
                return
            except Exception:
                logger.exception("Exception for prefetch `%r` `%r`", oid, tid)
        # we could optimize by `AsyncTask` wrapping
        await asyncio.gather(*(Task(prefetch(oid), loop=self.loop)
                               for oid in oids))

    async def tpc_finish_co(self, tid, updates, f):
        if not self.operational:
            raise ClientDisconnected
        try:
            tid = await self.protocol.call_sync('tpc_finish', tid)
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
            return tid
        except Exception:
            # At this point, our cache is in an inconsistent
            # state.  We need to reconnect in hopes of
            # recovering to a consistent state.
            self.protocol.close()
            self.disconnected(self.protocol)
            raise

    # server callbacks
    def invalidateTransaction(self, tid, oids):
        # see the cache related comment in ``tpc_finish_threadsafe``
        # why we think that locking is not necessary at this place
        for oid in oids:
            self.cache.invalidate(oid, tid)
        self.client.invalidateTransaction(tid, oids)
        self.cache.setLastTid(tid)

    def serialnos(self, serials):
        # Method called by ZEO4 storage servers.

        # Before delegating, check for errors (likely ConflictErrors)
        # and invalidate the oids they're associated with.  In the
        # past, this was done by the client, but now we control the
        # cache and this is our last chance, as the client won't call
        # back into us when there's an error.
        for oid in serials:
            if isinstance(oid, bytes):
                self.cache.invalidate(oid, None)
            else:
                oid, serial = oid
                if isinstance(serial, Exception) or serial == b'rs':
                    self.cache.invalidate(oid, None)

        self.client.serialnos(serials)

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
        self.client = client = ClientIo(loop, *self.__args, **self.__kwargs)
        self.call_with_timeout = call_with_timeout = \
            client.call_with_timeout_co
        self.call_sync = client.call_sync
        self.call_async = client.call_async
        self.call_async_iter = client.call_async_iter
        run_coroutine = run_coroutine_threadsafe

        def call(meth, *args,
                 timeout=self.timeout, indirect=True, wait=True):
            """call *meth* with arguments *args*.

            If *indirect*, make the call indirectly via ``call_with_timeout``.
            In this case, *meth* must return either ``None`` or
            a future; in particular, it cannot be a coroutine function.
            If not *indirect*, *meth* must be a coroutine function.
            Coroutine functions are defined with ``async def``
            and have a ``_co`` suffix.

            If *wait*, return the result otherwise the future.
            """
            if indirect:
                args = timeout, meth, args
                meth = call_with_timeout
            future = run_coroutine(meth(*args), loop)
            return future.result() if wait else future

        self._call_ = call  # creates reference cycle

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
        return self._call_(self.call_sync, method, *args, **kw)

    def async_(self, method, *args):
        """call method named *method* with *args* asynchronously."""
        return self._call_(self.call_async, method, *args, timeout=0)

    def async_iter(self, it):
        return self._call_(self.call_async_iter, it, timeout=0)

    def prefetch(self, oids, tid):
        oids = tuple(oids)  # avoid concurrency problems
        # There is potential for a race condition here:
        # we return a future, likely immediately released by
        # the caller. A cyclic reference prevents immediate
        # finalization, but a garbage collection might finalize
        # and effectively terminate the preloading process.
        # If the IO thread is sufficiently fast it has created a
        # future representing a server response, referenced globally.
        # Such a future protects the return value from
        # the garbage collector (it is referenced via callbacks).
        # It the IO thread is not fast enough, the complete
        # structure may be released during a garbage collection.
        return self._call_(self.client.prefetch_co, oids, tid,
                           indirect=False, wait=False)

    def load_before(self, oid, tid):
        return self._call_(self.client.load_before_co, self.timeout,
                           oid, tid, indirect=False)

    def tpc_finish(self, tid, updates, f, **kw):
        # ``kw`` for test only; supported ``wait``
        return self._call_(self.client.tpc_finish_co, tid, updates, f,
                           indirect=False, **kw)

    def is_connected(self):
        return self.client.operational

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
        call = self._call_
        self._call_ = self._call_after_closure
        loop = self.loop
        if loop is None or loop.is_closed():  # pragma: no cover
            # this should not happen
            return
        if loop.is_running():
            call(self.client.close_co, indirect=False, wait=True)
        else:  # pragma: no cover
            # this should not happen
            loop.run_until_complete(self.client.close_co())
        self.__args = None  # break reference cycle

    @staticmethod
    def _call_after_closure(*args, **kw):
        """auxiliary method to be used as `_call_` after closure."""
        raise ClientDisconnected('closed')

    def new_addrs(self, addrs):
        # This usually doesn't have an immediate effect, since the
        # addrs aren't used until the client disconnects.xs
        return self._call_(self.client.new_addrs_co, addrs, indirect=False)

    def wait(self, timeout=None):
        """wait for readyness"""
        def noop():
            return
        return self._call_(self.client.call_with_timeout_co,
                           self.timeout if timeout is None else timeout,
                           noop, (), True,
                           indirect=False)


class ClientThread(ClientRunner):
    """Thread wrapper for client interface

    A ClientProtocol is run in a dedicated thread.

    Calls to it are made in a thread-safe fashion.
    """

    def __init__(self, addrs, client, cache,
                 storage_key='1', read_only=False, timeout=30,
                 disconnect_poll=1, ssl=None, ssl_server_hostname=None):
        self.set_options(addrs, client, cache, storage_key, read_only,
                         timeout, disconnect_poll,
                         ssl=ssl, ssl_server_hostname=ssl_server_hostname,
                         )
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
                super().close()
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
            super().close()
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


def cancel_task(task):
    task.cancel()
    # With Python before 3.8, cancelation is not sufficient to
    # ignore a potential exception -- eat it in a done callback
    task.add_done_callback(
        lambda future: future.cancelled() or future.exception())
