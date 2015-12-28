from pickle import loads, dumps
from ZODB.ConflictResolution import ResolvedSerial
import asyncio
import concurrent.futures
import logging
import random
import threading
import ZEO.Exceptions

from . import adapters

logger = logging.getLogger(__name__)

local_random = random.Random() # use separate generator to facilitate tests

class Client(asyncio.Protocol):
    """asyncio low-level ZEO client interface
    """

    # All of the code in this class runs in a single dedicated
    # thread. Thus, we can mostly avoid worrying about interleaved
    # operations.

    # One place where special care was required was in cache setup on
    # connect. See finish connect below.

    def __init__(self, addr, client, cache, storage_key, read_only, loop):
        self.loop = loop
        self.addr = addr
        self.storage_key = storage_key
        self.read_only = read_only
        self.client = client
        for name in self.client_delegated:
            setattr(self, name, getattr(client, name))
        self.info = client.info
        self.cache = cache
        self.disconnected()

    closed = False
    def close(self):
        self.closed = True
        self.transport.close()
        self.cache.close()

    def protocol_factory(self):
        return adapters.SizedProtocolAdapter(self)

    def disconnected(self):
        self.ready = False
        self.connected = concurrent.futures.Future()
        self.protocol_version = None
        self.futures = {}
        self.connect()

    def connect(self):
        if isinstance(self.addr, tuple):
            host, port = self.addr
            cr = self.loop.create_connection(self.protocol_factory, host, port)
        else:
            cr = self.loop.create_unix_connection(
                self.protocol_factory, self.addr)

        cr = asyncio.async(cr, loop=self.loop)

        @cr.add_done_callback
        def done_connecting(future):
            if future.exception() is not None:
                # keep trying
                self.loop.call_later(1 + local_random.random(), self.connect)

    def connection_made(self, transport):
        logger.info("Connected")
        self.transport = adapters.SizedTransportAdapter(transport)

    def connection_lost(self, exc):
        exc = exc or ClientDisconnected()
        logger.info("Disconnected, %r", exc)
        for f in self.futures.values():
            f.set_exception(exc)
        self.disconnected()

    def finish_connect(self, protocol_version):
        # We use a promise model rather than coroutines here because
        # for the most part, this class is reactive a coroutines
        # aren't a good model of it's activities.  During
        # initialization, however, we use promises to provide an
        # impertive flow.

        # The promise(/future) implementation we use differs from
        # asyncio.Future in that callbacks are called immediately,
        # rather than using the loops call_soon.  We want to avoid a
        # race between invalidations and cache initialization. In
        # particular, after calling lastTransaction or
        # getInvalidations, we want to make sure we set the cache's
        # lastTid before processing subsequent invalidations.

        self.protocol_version = protocol_version
        self.transport.write(protocol_version)
        register = self.promise('register', self.storage_key, self.read_only)
        lastTransaction = self.promise('lastTransaction')
        cache = self.cache

        @register(lambda _ : lastTransaction)
        def verify(server_tid):
            if not cache:
                return server_tid

            cache_tid = cache.getLastTid()
            if not cache_tid:
                logger.error("Non-empty cache w/o tid -- clearing")
                cache.clear()
                self.client.invalidateCache()
                return server_tid
            elif cache_tid == server_tid:
                logger.info("Cache up to date %r", server_tid)
                return server_tid
            elif cache_tid >= server_tid:
                raise AssertionError("Server behind client, %r < %r",
                                     server_tid, cache_tid)

            @self.promise('getInvalidations', cache_tid)
            def verify_invalidations(vdata):
                if vdata:
                    tid, oids = vdata
                    for oid in oids:
                        cache.invalidate(oid, None)
                    return tid
                else:
                    # cache is too old
                    self.cache.clear()
                    self.client.invalidateCache()
                    return server_tid

            return verify_invalidations

        @verify
        def finish_verification(server_tid):
            cache.setLastTid(server_tid)
            self.ready = True

        finish_verification(
            lambda _ : self.connected.set_result(None),
            lambda e: self.connected.set_exception(e),
            )

    exception_type_type = type(Exception)
    def data_received(self, data):
        if self.protocol_version is None:
            self.finish_connect(data)
        else:
            msgid, async, name, args = loads(data)
            if name == '.reply':
                future = self.futures.pop(msgid)
                if (isinstance(args, tuple) and len(args) > 1 and
                    type(args[0]) == self.exception_type_type and
                    issubclass(r_args[0], Exception)
                    ):
                    future.set_exception(args[0])
                else:
                    future.set_result(args)
            else:
                assert async # clients only get async calls
                if name in self.client_methods:
                    getattr(self, name)(*args)
                else:
                    raise AttributeError(name)

    def call_async(self, method, *args):
        if self.ready:
            self.transport.write(dumps((0, True, method, args), 3))
        else:
            raise ZEO.Exceptions.ClientDisconnected()

    def call_async_threadsafe(self, future, method, args):
        try:
            self.call_async(method, *args)
        except Exception as e:
            future.set_exception(e)
        else:
            future.set_result(None)

    message_id = 0
    def _call(self, future, method, args):
        self.message_id += 1
        self.futures[self.message_id] = future
        self.transport.write(dumps((self.message_id, False, method, args), 3))
        return future

    def promise(self, method, *args):
        return self._call(Promise(), method, args)

    def call_threadsafe(self, result_future, method, args):
        if self.ready:
            return self._call(result_future, method, args)

        @self.connected.add_done_callback
        def done(future):
            e = future.exception()
            if e is not None:
                result_future.set_exception(e)
            else:
                self.call_threadsafe(result_future, method, args)

        return result_future

    # Special methods because they update the cache.

    def load_threadsafe(self, future, oid):
        data = self.cache.load(oid)
        if data is not None:
            future.set_result(data)
        else:
            @self.promise('loadEx', oid)
            def load(data):
                future.set_result(data)
                data, tid = data
                self.cache.store(oid, tid, None, data)

            load.catch(future.set_exception)

    def load_before_threadsafe(self, future, oid, tid):
        data = self.cache.loadBefore(oid, tid)
        if data is not None:
            future.set_result(data)
        else:
            @self.promise('loadBefore', oid, tid)
            def load_before(data):
                future.set_result(data)
                if data:
                    data, start, end = data
                self.cache.store(oid, start, end, data)

            load_before.catch(future.set_exception)

    def tpc_finish_threadsafe(self, future, tid, updates):
        @self.promise('tpc_finish', tid)
        def committed(_):
            cache = self.cache
            for oid, s, data in updates:
                cache.invalidate(oid, tid)
                if data and s != ResolvedSerial:
                    cache.store(oid, tid, None, data)
            cache.setLastTid(tid)
            future.set_result(None)

        committed.catch(future.set_exception)

    # Methods called by the server:

    client_methods = (
        'invalidateTransaction', 'serialnos', 'info',
        'receiveBlobStart', 'receiveBlobChunk', 'receiveBlobStop',
        )
    client_delegated = client_methods[1:]

    def invalidateTransaction(self, tid, oids):
        for oid in oids:
            self.cache.invalidate(oid, tid)
        self.cache.setLastTid(tid)
        self.client.invalidateTransaction(tid, oids)


class ClientRunner:

    def set_options(self, addr, wrapper, cache, storage_key, read_only,
                    timeout=30):
        self.__args = addr, wrapper, cache, storage_key, read_only
        self.timeout = timeout
        self.connected = concurrent.futures.Future()

    def setup_delegation(self, loop):
        self.loop = loop
        self.client = Client(*self.__args, loop=loop)

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
        return future.result(self.timeout if timeout is False else timeout)

    def call(self, method, *args, timeout=None):
        return self.__call(self.client.call_threadsafe, method, args)

    def callAsync(self, method, *args):
        return self.__call(self.client.call_async_threadsafe, method, args)

    def load(self, oid):
        return self.__call(self.client.load_threadsafe, oid)

    def load_before(self, oid, tid):
        return self.__call(self.client.load_before_threadsafe, oid, tid)

    def tpc_finish(self, tid, updates):
        return self.__call(self.client.tpc_finish_threadsafe, tid, updates)

class ClientThread(ClientRunner):
    """Thread wrapper for client interface

    A ClientProtocol is run in a dedicated thread.

    Calls to it are made in a thread-safe fashion.
    """

    def __init__(self, addr, client, cache,
                 storage_key='1', read_only=False, timeout=30):
        self.set_options(addr, client, cache, storage_key, read_only, timeout)
        threading.Thread(
            target=self.run,
            args=(addr, client, cache, storage_key, read_only),
            name='zeo_client_'+storage_key,
            daemon=True,
            ).start()
        self.connected.result(timeout)

    def run(self, *args):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.setup_delegation(loop, *args)
        loop.run_forever()

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

    next = success_callback = error_callback = None

    def __call__(self, success_callback = None, error_callback = None):
        self.next = self.__class__()
        self.success_callback = success_callback
        self.error_callback = error_callback
        return self.next

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
