"""ZEO client interface implementation.

The client interface implementation is split into two parts:
``ClientRunner`` and ``ClientIO``.
``ClientRunner`` methods are executed in the calling thread,
``ClientIO`` methods in an ``asyncio`` context.

``ClientRunner`` calls ``ClientIO`` methods in three different ways:

  directly
     the async calls ``call_async`` and ``call_async_iter``

  via ``run_coroutine_threadsafe``
     the methods with ``_co`` suffix

  via ``run_coroutine_fast``
     the methods with ``_fco`` suffix.
     Note that those methods are partially run in the application thread
     and therefore cannot use standard ``asyncio`` functionality.

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

Some ``ClientIO` methods are called directly or via
``run_coroutine_fast``. Therefore, part of their execution happens in the
application thread. As the IO thread runs concurrently, unavoidable race
conditions can occur -- in particular races regarding connection loss.
Connection loss is by principle an external and therefore uncontrollable
event. Running part of the method code in the application thread
does not introduce a new problem but can make the problem symptom
more difficult to understand (e.g. we may get the exception
``NoneType object does not have attribute Protocol`` instead
of ``IO on a closed socket``.

When ``ClientIO`` methods are called directly, we must avoid
concurrency problems between the IO thread and the application threads.
To achieve this, the IO loop has a lock. The loop holds this lock
whenever it is not waiting (and especially when it executes callbacks).
Application threads must acquire this lock during their direct call
of ``ClientIO`` methods. This prevents concurrency between the IO thread
and IO related activity by application threads.

The optimization to call ``ClientIO`` methods directly is not
always possible. It is indicated by a true `ClientIO.direct_socket_access``.
If the optimization is not possible, the ``async*`` methods
are called via ``loop.call_soon_threadsafe``, and the ``*_fco`` methods
via ``run_coroutine_threadsafe``.
"""

import asyncio
import logging
import random
import sys
import threading
from asyncio import CancelledError
from asyncio import TimeoutError
from asyncio import get_event_loop
from itertools import count

import ZODB.event
import ZODB.POSException

import ZEO.Exceptions
from ZEO.Exceptions import ClientDisconnected, ServerException
import ZEO.interfaces

from . import base
from .compat import new_event_loop
from .futures import Future, AsyncTask as Task, \
     run_coroutine_threadsafe, run_coroutine_fast
from .marshal import encoder, decoder

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
    # connect. See finish_connection below.

    protocols = b'5',

    def __init__(self, loop,
                 addr, client, storage_key, read_only, connect_poll=1,
                 heartbeat_interval=60, ssl=None, ssl_server_hostname=None,
                 ):
        """Create a server connection

        addr is either a host,port tuple or a string file name.

        client is a `ClientIO`.
        """
        super().__init__(
            loop,
            f'{addr!r}, {storage_key!r}, {read_only!r}')
        self.addr = addr
        self.storage_key = storage_key
        self.read_only = read_only
        self.client = client
        self.connect_poll = connect_poll
        self.heartbeat_interval = heartbeat_interval
        self.futures = {}  # { message_id -> future }
        self.ssl = ssl
        self.ssl_server_hostname = ssl_server_hostname
        # received invalidations while the protocol is not yet registered with client
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
                except asyncio.CancelledError:
                    logger.info("Connection to %r cancelled", self.addr)
                    raise
                except Exception as exc:
                    logger.info("Connection to %r failed, %s",
                                self.addr, exc)
                await asyncio.sleep(self.connect_poll + local_random.random())
                logger.info("retry connecting %r", self.addr)

        # Usually, we use our optimized but feature limited tasks
        # to run coroutines. Here we use a standard task
        # because we do not know which task features the connect
        # coroutine needs.
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
            # We must first inform the client about the disconnection
            # before the pending futures are finalized in ``close``.
            # This ensures that subsequent API calls see the disconnected
            # state and can wait for a reconnection
            client.disconnected(self)
            # will set ``self.client = None``
            self.close(exc or "Connection lost")

    verify_task = None

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
        self.verify_task = Task(self.verify_connection(), self.loop)

    async def verify_connection(self):
        """verify the connection -- run as task.

        We try to register with the server; if this succeeds with
        the client.
        """
        # we do not want that several servers concurrently
        # update the cache -- lock
        async with self.client.register_lock:
            try:
                try:
                    server_tid = await self.server_call(
                        'register', self.storage_key,
                        self.read_only if self.read_only is not Fallback
                        else False,
                        )
                except ZODB.POSException.ReadOnlyError:
                    if self.read_only is Fallback:
                        self.read_only = True
                        server_tid = await self.server_call(
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
            elif name == "invalidateTransaction" and not self.client.ready:
                # the client is not yet ready to process invalidations
                # queue them
                self.invalidations.append(args)
            else:
                getattr(self.client, name)(*args)

    message_id = 0

    def call_sync(self, method, args, message_id=None, future=None):
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
        future = future or  Future(loop=self.loop)  # noqa: E271
        self.futures[message_id] = future
        self.write_message(self.encode(message_id, False, method, args))
        return future

    def server_call(self, method, *args):
        return self.call_sync(method, args)

    def load_before(self, oid, tid):
        # Special-case load_before, so we collapse outstanding requests
        # and update the cache
        message_id = (oid, tid)
        future = self.futures.get(message_id)
        if future is None:
            future = Future(loop=self.loop)
            # Check whether the cache contains the information.
            # I am not sure whether the cache lookup is really
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
                def _(future):
                    try:
                        data = future.result()
                    except Exception:
                        return
                    if data:
                        data, start, end = data
                        self.client.cache.store(oid, start, end, data)

                self.call_sync('loadBefore', message_id,
                               message_id, future)
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


class ClientIO:
    """asyncio low-level ZEO client interface."""

    # All of the code in this class runs in a single dedicated
    # thread. Thus, we can mostly avoid worrying about interleaved
    # operations.

    # One place where special care was required was in cache setup on
    # connect.

    protocol = None
    protocols = ()
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
        # check whether we want to access the socket directly
        # We cannot do that for SSL because it uses a non-thread safe
        # loop operation in ``_write_app_data``.
        self.direct_socket_access = \
              ssl is None and \
              all(hasattr(loop, m) for m in
                  ("_process_events", "_run_once",
                   "_write_to_self", "_add_writer"))
        if self.direct_socket_access and not hasattr(loop, "lock"):
            # direct socket access possible
            # patch the loop
            loop.lock = threading.RLock()
            ori_process_events = loop._process_events
            ori_run_once = loop._run_once
            ori_add_writer = loop._add_writer

            def _process_events(*args):
                loop.lock.acquire()
                ori_process_events(*args)

            def _run_once():
                ori_run_once()
                loop.lock.release()

            def _add_writer(*args):
                ori_add_writer(*args)
                try:
                    c_loop = get_event_loop()
                except Exception:
                    c_loop = None
                if c_loop is not loop:
                    loop._write_to_self()

            loop._process_events = _process_events
            loop._run_once = _run_once
            loop._add_writer = _add_writer
            logger.info("loop patched to allow direct socket access")

        self.disconnected(None)

    def new_addrs(self, addrs):
        self.addrs = addrs
        if self.trying_to_connect():
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
        self.connected = Future(self.loop)

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
        call = protocol.server_call
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
            protocol.invalidations = []

            # Set ready so arriving invalidations are no longer queued.
            # Note: we are not yet fully ready.
            self.ready = True

            try:
                info = await call('get_info')
            except Exception as exc:
                # This is weird. We were connected and verified our cache, but
                # Now we errored getting info.

                # XXX Need a test for this. The lone before is what we
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
                # Order is important: as soon as we set the result
                # of ``connected``, a waiter for connectedness
                # can proceed and expect ``operational == True``
                # We are at this time fully operational even if
                # the ``connected`` result is not yet set
                self.operational = True
                self.connected.set_result(None)  # signal full readyness

    def get_peername(self):
        return self.protocol.get_peername()

    def call_async(self, method, args):
        return self.protocol.call_async(method, args)

    def call_async_iter(self, it):
        return self.protocol.call_async_iter(it)

    async def await_operational_fco(self, timeout, init_ok=False):
        """Wait *timeout* for operational.

        Fail immediately if ``ready is None`` unless ``init_ok``.
        """
        if self.ready is None  and  not init_ok:
            # We started without waiting for a connection. (prob tests :( )
            raise ClientDisconnected("never connected")
        if not self.operational:
            if timeout <= 0:
                raise ClientDisconnected("timed out waiting for connection")

            async def wait_operational():
                waiter = Future(self.loop)
                handle = None
                connected = self.connected

                def setup_timeout():
                    nonlocal handle
                    # we overwrite ``handle`` here to cancel
                    # the timeout (rather than the ``setup_timeout``).
                    # There is a race condition potential here,
                    # when ``setup_timeout`` has started but
                    # could not yet overwrite ``handle``.
                    # However, this simply means that the timeout
                    # is not cancelled which has no serious effects
                    handle = self.loop.call_later(timeout, stop)

                def stop(*unused):
                    if not waiter.done():
                        waiter.set_result(None)

                connected.add_done_callback(stop)
                handle = self.loop.call_soon_threadsafe(setup_timeout)
                await waiter
                try:
                    if connected.done():
                        return self.connected.result()
                    else:
                        connected.remove_done_callback(stop)
                        raise TimeoutError
                finally:
                    handle.cancel()

            try:
                await wait_operational()
            except TimeoutError:
                raise ClientDisconnected("timed out waiting for connection")
            except CancelledError:
                raise ClientDisconnected()
        # should be connected now

    async def call_sync_fco(self, method, args, timeout):
        """call method named *method* with *args* and *task* when ready.

        Wait at most *timeout* for readyness.
        """
        if not self.operational:
            await self.await_operational_fco(timeout)
        # race condition potential:
        # We have been operational but may meanwhile have lost the connection.
        # This will result in an exception propagated to the caller
        return await self.protocol.call_sync(method, args)

    async def close_co(self):
        # The following ``close`` deadlocked in isolated tests.
        # Prevent this with a timeout and log information
        # to understand the bug.
        # await self.close()
        closing = self.close()
        try:
            # the ``shield`` is necessary to keep the state unchanged
            await asyncio.wait_for(asyncio.shield(closing), 10)
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

    # Special methods because they update the cache.

    async def load_before_fco(self, oid, tid, timeout):
        if not self.operational:
            await self.await_operational_fco(timeout)
        # Race condition potential
        # -- see comment in ``call_sync_fco``
        return await self.protocol.load_before(oid, tid)

    async def prefetch_co(self, oids, tid):
        if not self.operational:
            raise ClientDisconnected()

        async def _prefetch(oid):
            """update the cache for *oid, tid* (if necessary)"""
            try:
                # ``load_before`` checks if the data is already in the cache
                await self.protocol.load_before(oid, tid)
            except ClientDisconnected:  # pragma no cover
                pass
            except Exception:  # pragma no cover
                logger.exception("Exception for prefetch `%r` `%r`", oid, tid)

        # We could directly ``gather`` ``protocol.load_before`` calls;
        # however, this would keep unneeded data in RAM
        # Therefore, we use the auxiliary function ``_prefetch`` which
        # discards the data
        await asyncio.gather(*(Task(_prefetch(oid), loop=self.loop)
                               for oid in oids))

    async def tpc_finish_fco(self, tid, updates, f):
        if not self.operational:
            raise ClientDisconnected()
        try:
            tid = await self.protocol.server_call('tpc_finish', tid)
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
        # see the cache related comment in ``tpc_finish_fco``
        # why we think that locking is not necessary at this place
        for oid in oids:
            self.cache.invalidate(oid, tid)
        self.client.invalidateTransaction(tid, oids)
        self.cache.setLastTid(tid)

    @property
    def protocol_version(self):
        return self.protocol.protocol_version

    def is_read_only(self):
        protocol = self.protocol
        return self.read_only if protocol is None else protocol.read_only


class ClientRunner:

    def set_options(self, addrs, wrapper, cache, storage_key, read_only,
                    timeout=30, disconnect_poll=1,
                    **kwargs):
        self.__args = (addrs, wrapper, cache, storage_key, read_only,
                       disconnect_poll)
        self.__kwargs = kwargs
        self.timeout = timeout

    def setup_delegation(self, loop):
        self.loop = loop
        self.client = client = ClientIO(loop, *self.__args, **self.__kwargs)
        self.call_sync_fco = client.call_sync_fco
        self.load_before_fco = client.load_before_fco
        run_threadsafe = run_coroutine_threadsafe
        run_fast = client.direct_socket_access and run_coroutine_fast \
                   or run_threadsafe

        def io_call(coro, wait=True, fast=True):
            """run coroutine *coro* in the IO thread.

            If *wait*, return the result otherwise the future.
            """
            run = fast and run_fast or run_threadsafe
            future = run(coro, loop)
            try:
                return future.result() if wait else future
            finally:
                del future  # break reference cycle in case of exception

        self.io_call = io_call  # creates reference cycle

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
        timeout = kw.pop("timeout", None)
        return self.io_call(
            self.call_sync_fco(
                method, args,
                timeout if timeout is not None else self.timeout),
            **kw)

    def async_(self, method, *args):
        """call method named *method* with *args* asynchronously."""
        client = self.client
        if not client.operational:
            raise ClientDisconnected
        # Potential race condition:
        # We may lose the connection before the call is sent to the server.
        # In this case, an exception is raised.
        if client.direct_socket_access:
            with client.loop.lock:
                client.call_async(method, args)
        else:
            self.loop.call_soon_threadsafe(client.call_async, method, args)

    def async_iter(self, it):
        client = self.client
        if not client.operational:
            raise ClientDisconnected
        # Potential race condition:
        # We may lose the connection before all messages are
        # sent to the server.
        # In this case, an exception is raised.
        if client.direct_socket_access:
            with client.loop.lock:
                client.call_async_iter(it)
        else:
            self.loop.call_soon_threadsafe(client.call_async_iter, it)

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
        # If the IO thread is not fast enough, the complete
        # structure may be released during a garbage collection.
        return self.io_call(
            self.client.prefetch_co(oids, tid), wait=False, fast=False)

    def load_before(self, oid, tid):
        return self.io_call(self.load_before_fco(oid, tid, self.timeout))

    def tpc_finish(self, tid, updates, f, **kw):
        # ``kw`` for test only; supported ``wait``
        return self.io_call(self.client.tpc_finish_fco(tid, updates, f), **kw)

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
        logger.debug("closing")
        call = self.io_call
        self.io_call = self._io_call_after_closure
        loop = self.loop
        if loop is None or loop.is_closed():  # pragma: no cover
            # this should not happen
            return
        if loop.is_running():
            call(self.client.close_co(), fast=False)
        else:  # pragma: no cover
            # Note: this can happen when loop.stop is called and so run_io_thread
            #       calls hereby close with already stopped event loop.
            # Note: run_coroutine_threadsafe - not Task - is used to protect
            #       from executing coro steps while loop is not yet running.
            loop.run_until_complete(
                    run_coroutine_threadsafe(self.client.close_co(), loop))
        self.__args = None  # break reference cycle

    @staticmethod
    def _io_call_after_closure(*args, **kw):
        """auxiliary method to be used as `io_call` after closure."""
        raise ClientDisconnected('closed')

    @staticmethod
    async def apply_co(func, *args):
        return func(*args)

    def new_addrs(self, addrs):
        # This usually doesn't have an immediate effect, since the
        # addrs aren't used until the client disconnects.xs
        self.io_call(self.apply_co(self.client.new_addrs, addrs), fast=False)

    def wait(self, timeout=None):
        """wait for readyness"""
        return self.io_call(
            self.client.await_operational_fco(
                self.timeout if timeout is None else timeout, True))


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
            name="%s zeo client networking thread %s"
            % (client.__name__, make_thread_id()))
        self.thread.daemon = True
        self.started = threading.Event()
        logger.debug("starting %s", self.thread.name)
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
            loop.call_soon(self.started.set)
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
            logger.debug("close requested")
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
    if sys.version_info < (3,9):
        task.add_done_callback(
            lambda future: future.cancelled() or future.exception())


# Debugging aid
thread_counter = count()

def make_thread_id():
    """return new unique thread id -- for log analysis."""
    return next(thread_counter)

