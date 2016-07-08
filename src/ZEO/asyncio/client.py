from .._compat import PY3

if PY3:
    import asyncio
else:
    import trollius as asyncio

from ZEO.Exceptions import ClientDisconnected
from ZODB.ConflictResolution import ResolvedSerial
import concurrent.futures
import logging
import random
import threading

import ZODB.event
import ZODB.POSException

import ZEO.Exceptions
import ZEO.interfaces

from . import base
from .marshal import decode

logger = logging.getLogger(__name__)

Fallback = object()

local_random = random.Random() # use separate generator to facilitate tests

class Protocol(base.Protocol):
    """asyncio low-level ZEO client interface
    """

    # All of the code in this class runs in a single dedicated
    # thread. Thus, we can mostly avoid worrying about interleaved
    # operations.

    # One place where special care was required was in cache setup on
    # connect. See finish connect below.

    protocols = b'Z309', b'Z310', b'Z3101', b'Z4', b'Z5'

    def __init__(self, loop,
                 addr, client, storage_key, read_only, connect_poll=1,
                 heartbeat_interval=60, ssl=None, ssl_server_hostname=None):
        """Create a client interface

        addr is either a host,port tuple or a string file name.

        client is a ClientStorage. It must be thread safe.

        cache is a ZEO.interfaces.IClientCache.
        """
        super(Protocol, self).__init__(loop, addr)
        self.storage_key = storage_key
        self.read_only = read_only
        self.name = "%s(%r, %r, %r)" % (
            self.__class__.__name__, addr, storage_key, read_only)
        self.client = client
        self.connect_poll = connect_poll
        self.heartbeat_interval = heartbeat_interval
        self.futures = {} # { message_id -> future }
        self.ssl = ssl
        self.ssl_server_hostname = ssl_server_hostname

        self.connect()

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
            cr = self.loop.create_connection(
                self.protocol_factory, host or 'localhost', port,
                ssl=self.ssl, server_hostname=self.ssl_server_hostname)
        else:
            cr = self.loop.create_unix_connection(
                self.protocol_factory, self.addr, ssl=self.ssl)

        self._connecting = cr = asyncio.async(cr, loop=self.loop)

        @cr.add_done_callback
        def done_connecting(future):
            if future.exception() is not None:
                logger.info("Connection to %r failed, retrying, %s",
                            self.addr, future.exception())
                # keep trying
                if not self.closed:
                    self.loop.call_later(
                        self.connect_poll + local_random.random(),
                        self.connect,
                        )

    def connection_made(self, transport):
        super(Protocol, self).connection_made(transport)
        self.heartbeat(write=False)

    def connection_lost(self, exc):
        logger.debug('connection_lost %r', exc)
        self.heartbeat_handle.cancel()
        if self.closed:
            for f in self.pop_futures():
                f.cancel()
        else:
            # We have to be careful processing the futures, because
            # exception callbacks might modufy them.
            for f in self.pop_futures():
                f.set_exception(ClientDisconnected(exc or 'connection lost'))
            self.closed = True
            self.client.disconnected(self)

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

    exception_type_type = type(Exception)
    def message_received(self, data):
        msgid, async, name, args = decode(data)
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

    message_id = 0
    def call(self, future, method, args):
        self.message_id += 1
        self.futures[self.message_id] = future
        self._write(self.encode(self.message_id, False, method, args))
        return future

    def promise(self, method, *args):
        return self.call(Promise(), method, args)

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
            self._write(b'(J\xff\xff\xff\xffK\x00U\x06.replyNt.')
        self.heartbeat_handle = self.loop.call_later(
            self.heartbeat_interval, self.heartbeat)

class Client(object):
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
                 register_failed_poll=9,
                 ssl=None, ssl_server_hostname=None):
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
        self.ssl = ssl
        self.ssl_server_hostname = ssl_server_hostname
        for name in Protocol.client_delegated:
            setattr(self, name, getattr(client, name))
        self.cache = cache
        self.protocols = ()
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
        return (not self.ready or
                self.is_read_only() and self.read_only is Fallback)

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
        logger.debug('disconnected %r %r', self, protocol)
        if protocol is None or protocol is self.protocol:
            if protocol is self.protocol and protocol is not None:
                self.client.notify_disconnected()
            if self.ready:
                self.ready = False
            self.connected = concurrent.futures.Future()
            self.protocol = None
            self._clear_protocols()

        if all(p.closed for p in self.protocols):
            self.try_connecting()

    def upgrade(self, protocol):
        self.ready = False
        self.connected = concurrent.futures.Future()
        self.protocol.close()
        self.protocol = protocol
        self._clear_protocols(protocol)

    def try_connecting(self):
        logger.debug('try_connecting')
        if not self.closed:
            self.protocols = [
                Protocol(self.loop, addr, self,
                         self.storage_key, self.read_only, self.connect_poll,
                         ssl=self.ssl,
                         ssl_server_hostname=self.ssl_server_hostname,
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
        # A protocol failed registration. That's weird.  If they've all
        # failed, we should try again in a bit.
        if protocol is not self:
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
            self.client.invalidateTransaction(tid, oids)
            self.cache.setLastTid(tid)

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
        self.client = Client(loop, *self.__args, **self.__kwargs)
        self.call_threadsafe = self.client.call_threadsafe
        self.call_async_threadsafe = self.client.call_async_threadsafe

        from concurrent.futures import Future
        call_soon_threadsafe = loop.call_soon_threadsafe

        def call(meth, *args, **kw):
            timeout = kw.pop('timeout', None)
            assert not kw
            result = Future()
            call_soon_threadsafe(meth, result, *args)
            return self.wait_for_result(result, timeout)

        self.__call = call

    def wait_for_result(self, future, timeout):
        try:
            return future.result(self.timeout if timeout is None else timeout)
        except concurrent.futures.TimeoutError:
            if not self.client.ready:
                raise ClientDisconnected("timed out waiting for connection")
            else:
                raise

    def call(self, method, *args, **kw):
        return self.__call(self.call_threadsafe, method, args, **kw)

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

    def apply_threadsafe(self, future, func, *args):
        try:
            future.set_result(func(*args))
        except Exception as exc:
            future.set_exception(exc)

    def new_addrs(self, addrs):
        # This usually doesn't have an immediate effect, since the
        # addrs aren't used until the client disconnects.xs
        self.__call(self.apply_threadsafe, self.client.new_addrs, addrs)

    def wait(self, timeout=None):
        if timeout is None:
            timeout = self.timeout
        self.wait_for_result(self.client.connected, timeout)

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
                         ssl=ssl, ssl_server_hostname=ssl_server_hostname)
        self.thread = threading.Thread(
            target=self.run,
            name="%s zeo client networking thread" % client.__name__,
            )
        self.thread.setDaemon(True)
        self.started = threading.Event()
        self.thread.start()
        self.started.wait()
        if self.exception:
            raise self.exception

    exception = None
    def run(self):
        loop = None
        try:
            loop = asyncio.new_event_loop()
            self.setup_delegation(loop)
            self.started.set()
            loop.run_forever()
        except Exception as exc:
            raise
            logger.exception("Client thread")
            self.exception = exc
        finally:
            if not self.closed:
                self.closed = True
                try:
                    if self.client.ready:
                        self.client.ready = False
                        self.client.client.notify_disconnected()
                except AttributeError:
                    pass
                logger.critical("Client loop stopped unexpectedly")
            if loop is not None:
                loop.close()
            logger.debug('Stopping client thread')

    closed = False
    def close(self):
        if not self.closed:
            self.closed = True
            super(ClientThread, self).close()
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.thread.join(9)
            if self.exception:
                raise self.exception

class Promise(object):
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
