from pickle import loads, dumps
from ZEO.Exceptions import ClientDisconnected
from ZODB.ConflictResolution import ResolvedSerial
from struct import unpack
import asyncio
import concurrent.futures
import logging
import random
import threading
import traceback

import ZODB.event
import ZODB.POSException

import ZEO.Exceptions
import ZEO.interfaces

logger = logging.getLogger(__name__)

Fallback = object()

local_random = random.Random() # use separate generator to facilitate tests

class Closed(Exception):
    """A connection has been closed
    """

class Protocol(asyncio.Protocol):
    """asyncio low-level ZEO client interface
    """

    # All of the code in this class runs in a single dedicated
    # thread. Thus, we can mostly avoid worrying about interleaved
    # operations.

    # One place where special care was required was in cache setup on
    # connect. See finish connect below.

    transport = protocol_version = None

    protocols = b"Z309", b"Z310", b"Z3101"

    def __init__(self, loop,
                 addr, client, storage_key, read_only, connect_poll=1):
        """Create a client interface

        addr is either a host,port tuple or a string file name.

        client is a ClientStorage. It must be thread safe.

        cache is a ZEO.interfaces.IClientCache.
        """
        self.loop = loop
        self.addr = addr
        self.storage_key = storage_key
        self.read_only = read_only
        self.name = "%s(%r, %r, %r)" % (
            self.__class__.__name__, addr, storage_key, read_only)
        self.client = client
        self.connect_poll = connect_poll
        self.futures = {} # { message_id -> future }
        self.input  = [] # Buffer when assembling messages
        self.output = [] # Buffer when paused
        self.paused = [] # Paused indicator, mutable to avoid attr lookup

        # Handle the first message, the protocol handshake, differently
        self.message_received = self.first_message_received

        self.connect()

    def __repr__(self):
        return self.name

    closed = False
    def close(self):
        if not self.closed:
            self.closed = True
            self._connecting.cancel()
            if self.transport is not None:
                self.transport.close()
            for future in self.pop_futures():
                future.set_exception(ClientDisconnected("Closed"))

    def pop_futures(self):
        # Remove and return futures from self.futures.  The caller
        # will finalize them in some way and callbacks may modify
        # self.futures.
        futures = list(self.futures.values())
        self.futures.clear()
        return futures

    def protocol_factory(self):
        return self

    def connect(self):
        if isinstance(self.addr, tuple):
            host, port = self.addr
            cr = self.loop.create_connection(self.protocol_factory, host, port)
        else:
            cr = self.loop.create_unix_connection(
                self.protocol_factory, self.addr)

        self._connecting = cr = asyncio.async(cr, loop=self.loop)

        @cr.add_done_callback
        def done_connecting(future):
            if future.exception() is not None:
                logger.info("Connection to %rfailed, retrying, %s",
                            self.addr, future.exception())
                # keep trying
                if not self.closed:
                    self.loop.call_later(
                        self.connect_poll + local_random.random(),
                        self.connect,
                        )

    def connection_made(self, transport):
        logger.info("Connected %s", self)
        self.transport = transport
        paused = self.paused
        output = self.output
        append = output.append
        writelines = transport.writelines
        from struct import pack

        def write(message):
            if paused:
                append(message)
            else:
                writelines((pack(">I", len(message)), message))

        self._write = write

        def writeit(data):
            # Note, don't worry about combining messages.  Iters
            # will be used with blobs, in which case, the individual
            # messages will be big to begin with.
            data = iter(data)
            for message in data:
                writelines((pack(">I", len(message)), message))
                if paused:
                    append(data)
                    break

        self._writeit = writeit

    def connection_lost(self, exc):
        if self.closed:
            for f in self.pop_futures():
                f.cancel()
        else:
            self.client.disconnected(self)
            # We have to be careful processing the futures, because
            # exception callbacks might modufy them.
            for f in self.pop_futures():
                f.set_exception(ClientDisconnected(exc or 'connection lost'))

    def finish_connect(self, protocol_version):

        # We use a promise model rather than coroutines here because
        # for the most part, this class is reactive and coroutines
        # aren't a good model of it's activities.  During
        # initialization, however, we use promises to provide an
        # imperative flow.

        # The promise(/future) implementation we use differs from
        # asyncio.Future in that callbacks are called immediately,
        # rather than using the loops call_soon.  We want to avoid a
        # race between invalidations and cache initialization. In
        # particular, after getting a response from lastTransaction or
        # getInvalidations, we want to make sure we set the cache's
        # lastTid before processing (and possibly missing) subsequent
        # invalidations.

        self.protocol_version = min(protocol_version, self.protocols[-1])
        if self.protocol_version not in self.protocols:
            self.client.register_failed(
                self, ZEO.Exceptions.ProtocolError(protocol_version))
            return
        self._write(self.protocol_version)

        register = self.promise(
            'register', self.storage_key,
            self.read_only if self.read_only is not Fallback else False,
            )
        if self.read_only is not Fallback:
            # Get lastTransaction in flight right away to make
            # successful connection quicker, but only if we're not
            # doing read-only fallback.  If we might need to retry, we
            # can't send lastTransaction because if the registration
            # fails, it will be seen as an invalid message and the
            # connection will close. :( It would be a lot better of
            # registere returned the last transaction (and info while
            # it's at it).
            lastTransaction = self.promise('lastTransaction')
        else:
            lastTransaction = None # to make python happy

        @register
        def registered(_):
            if self.read_only is Fallback:
                self.read_only = False
                r_lastTransaction = self.promise('lastTransaction')
            else:
                r_lastTransaction = lastTransaction
            self.client.registered(self, r_lastTransaction)

        @register.catch
        def register_failed(exc):
            if (isinstance(exc, ZODB.POSException.ReadOnlyError) and
                self.read_only is Fallback):
                # We tried a write connection, degrade to a read-only one
                self.read_only = True
                logger.info("%s write connection failed. Trying read-only",
                            self)
                register = self.promise('register', self.storage_key, True)
                # get lastTransaction in flight.
                lastTransaction = self.promise('lastTransaction')

                @register
                def registered(_):
                    self.client.registered(self, lastTransaction)

                @register.catch
                def register_failed(exc):
                    self.client.register_failed(self, exc)

            else:
                self.client.register_failed(self, exc)

    got = 0
    want = 4
    getting_size = True
    def data_received(self, data):

        # Low-level input handler collects data into sized messages.

        self.got += len(data)
        self.input.append(data)
        while self.got >= self.want:
            extra = self.got - self.want
            if extra == 0:
                collected = b''.join(self.input)
                self.input = []
            else:
                input = self.input
                self.input = [data[-extra:]]
                input[-1] = input[-1][:-extra]
                collected = b''.join(input)

            self.got = extra

            if self.getting_size:
                # we were recieving the message size
                assert self.want == 4
                self.want = unpack(">I", collected)[0]
                self.getting_size = False
            else:
                self.want = 4
                self.getting_size = True
                self.message_received(collected)

    def first_message_received(self, data):
        # Handler for first/handshake message, set up in __init__
        del self.message_received # use default handler from here on
        self.finish_connect(data)

    exception_type_type = type(Exception)
    def message_received(self, data):
        msgid, async, name, args = loads(data)
        if name == '.reply':
            future = self.futures.pop(msgid)
            if (isinstance(args, tuple) and len(args) > 1 and
                type(args[0]) == self.exception_type_type and
                issubclass(args[0], Exception)
                ):
                if not issubclass(
                    args[0], (
                        ZODB.POSException.POSKeyError,
                        ZODB.POSException.ConflictError,)
                    ):
                    logger.error("%s from server: %s.%s:%s",
                                 self.name,
                                 args[0].__module__,
                                 args[0].__name__,
                                 args[1])
                future.set_exception(args[1])
            else:
                future.set_result(args)
        else:
            assert async # clients only get async calls
            if name in self.client_methods:
                getattr(self.client, name)(*args)
            else:
                raise AttributeError(name)

    def call_async(self, method, args):
        self._write(dumps((0, True, method, args), 3))

    def call_async_iter(self, it):
        self._writeit(dumps((0, True, method, args), 3) for method, args in it)

    message_id = 0
    def call(self, future, method, args):
        self.message_id += 1
        self.futures[self.message_id] = future
        self._write(dumps((self.message_id, False, method, args), 3))
        return future

    def promise(self, method, *args):
        return self.call(Promise(), method, args)

    def pause_writing(self):
        self.paused.append(1)

    def resume_writing(self):
        paused = self.paused
        del paused[:]
        output = self.output
        writelines = self.transport.writelines
        from struct import pack
        while output and not paused:
            message = output.pop(0)
            if isinstance(message, bytes):
                writelines((pack(">I", len(message)), message))
            else:
                data = message
                for message in data:
                    writelines((pack(">I", len(message)), message))
                    if paused: # paused again. Put iter back.
                        output.insert(0, data)
                        break

    def get_peername(self):
        return self.transport.get_extra_info('peername')

    # Methods called by the server.
    # WARNING WARNING we can't call methods that call back to us
    # syncronously, as that would lead to DEADLOCK!

    client_methods = (
        'invalidateTransaction', 'serialnos', 'info',
        'receiveBlobStart', 'receiveBlobChunk', 'receiveBlobStop',
        # plus: notify_connected, notify_disconnected
        )
    client_delegated = client_methods[2:]

class Client:
    """asyncio low-level ZEO client interface
    """

    # All of the code in this class runs in a single dedicated
    # thread. Thus, we can mostly avoid worrying about interleaved
    # operations.

    # One place where special care was required was in cache setup on
    # connect.

    protocol = None
    ready = None # Tri-value: None=Never connected, True=connected,
                 # False=Disconnected

    def __init__(self, loop,
                 addrs, client, cache, storage_key, read_only, connect_poll,
                 register_failed_poll=9):
        """Create a client interface

        addr is either a host,port tuple or a string file name.

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
        for name in Protocol.client_delegated:
            setattr(self, name, getattr(client, name))
        self.cache = cache
        self.protocols = ()
        self.disconnected(None)

    closed = False
    def close(self):
        if not self.closed:
            self.closed = True
            self.ready = False
            if self.protocol is not None:
                self.protocol.close()
            self.cache.close()
            self._clear_protocols()

    def _clear_protocols(self, protocol=None):
        for p in self.protocols:
            if p is not protocol:
                p.close()
        self.protocols = ()

    def disconnected(self, protocol=None):
        if protocol is None or protocol is self.protocol:
            if protocol is self.protocol and protocol is not None:
                self.client.notify_disconnected()
            if self.ready:
                self.ready = False
            self.connected = concurrent.futures.Future()
            self.protocol = None
            self._clear_protocols()
            self.try_connecting()

    def upgrade(self, protocol):
        self.ready = False
        self.connected = concurrent.futures.Future()
        self.protocol.close()
        self.protocol = protocol
        self._clear_protocols(protocol)

    def try_connecting(self):
        if not self.closed:
            self.protocols = [
                Protocol(self.loop, addr, self,
                         self.storage_key, self.read_only, self.connect_poll,
                         )
                for addr in self.addrs
                ]

    def registered(self, protocol, last_transaction_promise):
        if self.protocol is None:
            self.protocol = protocol
            if not (self.read_only is Fallback and protocol.read_only):
                # We're happy with this protocol. Tell the others to
                # stop trying.
                self._clear_protocols(protocol)
            self.verify(last_transaction_promise)
        elif (self.read_only is Fallback and not protocol.read_only and
              self.protocol.read_only):
            self.upgrade(protocol)
            self.verify(last_transaction_promise)
        else:
            protocol.close() # too late, we went home with another

    def register_failed(self, protocol, exc):
        # A protcol failed registration. That's weird.  If they've all
        # failed, we should try again in a bit.
        protocol.close()
        logger.exception("Registration or cache validation failed, %s", exc)
        if (self.protocol is None and not
            any(not p.closed for p in self.protocols)
            ):
            self.loop.call_later(
                self.register_failed_poll + local_random.random(),
                self.try_connecting)

    verify_result = None # for tests
    def verify(self, last_transaction_promise):
        protocol = self.protocol

        @last_transaction_promise
        def finish_verify(server_tid):
            cache = self.cache
            if cache:
                cache_tid = cache.getLastTid()
                if not cache_tid:
                    self.verify_result = "Non-empty cache w/o tid"
                    logger.error("Non-empty cache w/o tid -- clearing")
                    cache.clear()
                    self.client.invalidateCache()
                    self.finished_verify(server_tid)
                elif cache_tid > server_tid:
                    self.verify_result = "Cache newer than server"
                    logger.critical(
                        'Client has seen newer transactions than server!')
                    raise AssertionError("Server behind client, %r < %r, %s",
                                         server_tid, cache_tid, protocol)
                elif cache_tid == server_tid:
                    self.verify_result = "Cache up to date"
                    self.finished_verify(server_tid)
                else:
                    @protocol.promise('getInvalidations', cache_tid)
                    def verify_invalidations(vdata):
                        if vdata:
                            self.verify_result = "quick verification"
                            tid, oids = vdata
                            for oid in oids:
                                cache.invalidate(oid, None)
                            self.client.invalidateTransaction(tid, oids)
                            return tid
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
                            return server_tid

                    verify_invalidations(
                        self.finished_verify,
                        self.connected.set_exception,
                        )
            else:
                self.verify_result = "empty cache"
                self.finished_verify(server_tid)

        @finish_verify.catch
        def verify_failed(exc):
            del self.protocol
            self.register_failed(protocol, exc)

    def finished_verify(self, server_tid):
        # The cache is validated and the last tid we got from the server.
        # Set ready so we apply any invalidations that follow.
        # We've been ignoring them up to this point.
        self.cache.setLastTid(server_tid)
        self.ready = True

        @self.protocol.promise('get_info')
        def got_info(info):
            self.client.notify_connected(self, info)
            self.connected.set_result(None)

        @got_info.catch
        def failed_info(exc):
            self.register_failed(self, exc)

    def get_peername(self):
        return self.protocol.get_peername()

    def call_async_threadsafe(self, future, method, args):
        if self.ready:
            self.protocol.call_async(method, args)
            future.set_result(None)
        else:
            future.set_exception(ClientDisconnected())

    def call_async_from_same_thread(self, method, *args):
        return self.protocol.call_async(method, args)

    def call_async_iter_threadsafe(self, future, it):
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
                e = future.exception()
                if e is not None:
                    future.set_exception(e)
                else:
                    if self.ready:
                        func(result_future, *args)
                    else:
                        self._when_ready(func, result_future, *args)

    def call_threadsafe(self, future, method, args):
        if self.ready:
            self.protocol.call(future, method, args)
        else:
            self._when_ready(self.call_threadsafe, future, method, args)

    # Special methods because they update the cache.

    def load_threadsafe(self, future, oid):
        data = self.cache.load(oid)
        if data is not None:
            future.set_result(data)
        elif self.ready:
            @self.protocol.promise('loadEx', oid)
            def load(data):
                future.set_result(data)
                data, tid = data
                self.cache.store(oid, tid, None, data)

            load.catch(future.set_exception)
        else:
            self._when_ready(self.load_threadsafe, future, oid)

    def load_before_threadsafe(self, future, oid, tid):
        data = self.cache.loadBefore(oid, tid)
        if data is not None:
            future.set_result(data)
        elif self.ready:
            @self.protocol.promise('loadBefore', oid, tid)
            def load_before(data):
                future.set_result(data)
                if data:
                    data, start, end = data
                    self.cache.store(oid, start, end, data)

            load_before.catch(future.set_exception)
        else:
            self._when_ready(self.load_before_threadsafe, future, oid, tid)

    def tpc_finish_threadsafe(self, future, tid, updates, f):
        if self.ready:
            @self.protocol.promise('tpc_finish', tid)
            def committed(tid):
                try:
                    cache = self.cache
                    for oid, data, resolved in updates:
                        cache.invalidate(oid, tid)
                        if data and not resolved:
                            cache.store(oid, tid, None, data)
                    cache.setLastTid(tid)
                except Exception as exc:
                    future.set_exception(exc)

                    # At this point, our cache is in an inconsistent
                    # state.  We need to reconnect in hopes of
                    # recovering to a consistent state.
                    self.protocol.close()
                    self.disconnected(self.protocol)
                else:
                    f(tid)
                    future.set_result(tid)

            committed.catch(future.set_exception)
        else:
            future.set_exception(ClientDisconnected())

    def close_threadsafe(self, future):
        self.close()
        future.set_result(None)

    def invalidateTransaction(self, tid, oids):
        if self.ready:
            for oid in oids:
                self.cache.invalidate(oid, tid)
            self.cache.setLastTid(tid)
            self.client.invalidateTransaction(tid, oids)

    def serialnos(self, serials):
        # Before delegating, check for errors (likely ConflictErrors)
        # and invalidate the oids they're associated with.  In the
        # past, this was done by the client, but now we control the
        # cache and this is our last chance, as the client won't call
        # back into us when there's an error.
        for oid, serial in serials:
            if isinstance(serial, Exception):
                self.cache.invalidate(oid, None)

        self.client.serialnos(serials)

    @property
    def protocol_version(self):
        return self.protocol.protocol_version

    def is_read_only(self):
        try:
            protocol = self.protocol
        except AttributeError:
            return self.read_only
        else:
            if protocol is None:
                return self.read_only
            else:
                return protocol.read_only

class ClientRunner:

    def set_options(self, addrs, wrapper, cache, storage_key, read_only,
                    timeout=30, disconnect_poll=1):
        self.__args = (addrs, wrapper, cache, storage_key, read_only,
                       disconnect_poll)
        self.timeout = timeout
        self.connected = concurrent.futures.Future()

    def setup_delegation(self, loop):
        self.loop = loop
        self.client = Client(loop, *self.__args)
        self.call_threadsafe = self.client.call_threadsafe
        self.call_async_threadsafe = self.client.call_async_threadsafe

        from concurrent.futures import Future
        call_soon_threadsafe = loop.call_soon_threadsafe

        def call(meth, *args, timeout=False):
            result = Future()
            call_soon_threadsafe(meth, result, *args)
            return self.wait_for_result(result, timeout)

        self.__call = call

        @self.client.connected.add_done_callback
        def thread_done_connecting(future):
            e = future.exception()
            if e is not None:
                self.connected.set_exception(e)
            else:
                self.connected.set_result(None)

    def wait_for_result(self, future, timeout):
        try:
            return future.result(self.timeout if timeout is False else timeout)
        except concurrent.futures.TimeoutError:
            if not self.client.ready:
                raise ClientDisconnected("timed out waiting for connection")
            else:
                raise

    def call(self, method, *args, timeout=None):
        return self.__call(self.call_threadsafe, method, args)

    def call_future(self, method, *args):
        # for tests
        result = concurrent.futures.Future()
        self.loop.call_soon_threadsafe(
            self.call_threadsafe, result, method, args)
        return result

    def async(self, method, *args):
        return self.__call(self.call_async_threadsafe, method, args)

    def async_iter(self, it):
        return self.__call(self.client.call_async_iter_threadsafe, it)

    def load(self, oid):
        return self.__call(self.client.load_threadsafe, oid)

    def load_before(self, oid, tid):
        return self.__call(self.client.load_before_threadsafe, oid, tid)

    def tpc_finish(self, tid, updates, f):
        return self.__call(self.client.tpc_finish_threadsafe, tid, updates, f)

    def is_connected(self):
        return self.client.ready

    def is_read_only(self):
        try:
            protocol = self.client.protocol
        except AttributeError:
            return True
        else:
            if protocol is None:
                return True
            else:
                return protocol.read_only

    def close(self):
        self.__call(self.client.close_threadsafe)

        # Short circuit from now on. We're closed.
        def call_closed(*a, **k):
            raise ClientDisconnected('closed')

        self.__call = call_closed

    def new_addr(self, addrs):
        # This usually doesn't have an immediate effect, since the
        # addrs aren't used until the client disconnects.xs
        self.client.addrs = addrs

class ClientThread(ClientRunner):
    """Thread wrapper for client interface

    A ClientProtocol is run in a dedicated thread.

    Calls to it are made in a thread-safe fashion.
    """

    def __init__(self, addrs, client, cache,
                 storage_key='1', read_only=False, timeout=30,
                 disconnect_poll=1):
        self.set_options(addrs, client, cache, storage_key, read_only,
                         timeout, disconnect_poll)
        self.thread = threading.Thread(
            target=self.run,
            name="%s zeo client networking thread" % client.__name__,
            daemon=True,
            )
        self.started = threading.Event()
        self.thread.start()
        self.started.wait()
        if self.exception:
            raise self.exception

    exception = None
    def run(self):
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self.setup_delegation(loop)
            self.started.set()
            loop.run_forever()
        except Exception as exc:
            logger.exception("Client thread")
            self.exception = exc
        finally:
            if not self.closed:
                if self.client.ready:
                    self.closed = True
                    self.client.ready = False
                    self.client.client.notify_disconnected()
                logger.critical("Client loop stopped unexpectedly")
            loop.close()
            logger.debug('Stopping client thread')

    def start(self, wait=True):
        if wait:
            self.wait_for_result(self.connected, self.timeout)

    closed = False
    def close(self):
        if not self.closed:
            self.closed = True
            super().close()
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.thread.join(9)
            if self.exception:
                raise self.exception

class Promise:
    """Lightweight future with a partial promise API.

    These are lighweight because they call callbacks synchronously
    rather than through an event loop, and because they ony support
    single callbacks.
    """

    # Note that we can know that they are completed after callbacks
    # are set up because they're used to make network requests.
    # Requests are made by writing to a transport.  Because we're used
    # in a single-threaded protocol, we can't get a response and be
    # completed if the callbacks are set in the same code that
    # created the promise, which they are.

    next = success_callback = error_callback = cancelled = None

    def __call__(self, success_callback = None, error_callback = None):
        """Set the promises success and error handlers and beget a new promise

        The promise returned provides for promise chaining, providing
        a sane imperative flow.  Let's call this the "next" promise.
        Any results or exceptions generated by the promise or it's
        callbacks are passed on to the next promise.

        When the promise completes successfully, if a success callback
        isn't set, then the next promise is completed with the
        successfull result.  If a success callback is provided, it's
        called. If the call succeeds, and the result is a promise,
        them the result is called with the next promise's set_result
        and set_exception methods, chaining the result and next
        promise. If the result isn't a promise, then the next promise
        is completed with it by calling set_result. If the success
        callback fails, then it's exception is passed to
        next.set_exception.

        If the promise completes with an error and the error callback
        isn't set, then the exception is passed to the next promises
        set_exception.  If an error handler is provided, it's called
        and if it doesn't error, then the original exception is passed
        to the next promise's set_exception. If there error handler
        errors, then that exception is passed to the next promise's
        set_exception.
        """
        self.next = self.__class__()
        self.success_callback = success_callback
        self.error_callback = error_callback
        return self.next

    def cancel(self):
        self.set_exception(concurrent.futures.CancelledError)

    def catch(self, error_callback):
        self.error_callback = error_callback

    def set_exception(self, exc):
        self._notify(None, exc)

    def set_result(self, result):
        self._notify(result, None)

    def _notify(self, result, exc):
        next = self.next
        if exc is not None:
            if self.error_callback is not None:
                try:
                    result = self.error_callback(exc)
                except Exception:
                    logger.exception("Exception handling error %s", exc)
                    if next is not None:
                        next.set_exception(exc)
                else:
                    if next is not None:
                        next.set_result(result)
            elif next is not None:
                next.set_exception(exc)
        else:
            if self.success_callback is not None:
                try:
                    result = self.success_callback(result)
                except Exception as exc:
                    logger.exception("Exception in success callback")
                    if next is not None:
                        next.set_exception(exc)
                else:
                    if next is not None:
                        if isinstance(result, Promise):
                            result(next.set_result, next.set_exception)
                        else:
                            next.set_result(result)
            elif next is not None:
                next.set_result(result)
