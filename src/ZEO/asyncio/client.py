from pickle import loads, dumps
from ZODB.ConflictResolution import ResolvedSerial
from struct import unpack
import asyncio
import concurrent.futures
import logging
import random
import threading
import ZEO.Exceptions
import ZODB.POSException

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

    def __init__(self, addr, client, storage_key, read_only, loop,
                 connect_timeout=1):
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
        self.connect_timeout = connect_timeout
        self.futures = {} # { message_id -> future }
        self.input = []
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
            for future in self.futures.values():
                future.set_exception(Closed())
            self.futures.clear()

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
                # keep trying
                if not self.closed:
                    self.loop.call_later(1 + local_random.random(),
                                         self.connect)

    def connection_made(self, transport):
        logger.info("Connected %s", self)
        self.transport = transport

        writelines = transport.writelines
        from struct import pack

        def write(message):
            writelines((pack(">I", len(message)), message))

        self._write = write

    def connection_lost(self, exc):
        if exc is None:
            # we were closed
            for f in self.futures.values():
                f.cancel()
        else:
            logger.info("Disconnected, %s, %r", self, exc)
            for f in self.futures.values():
                f.set_exception(exc)
            self.client.disconnected(self)

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
        self._write(protocol_version)

        register = self.promise(
            'register', self.storage_key,
            self.read_only if self.read_only is not Fallback else False,
            )
        # Get lastTransaction in flight right away to make successful
        # connection quicker
        lastTransaction = self.promise('lastTransaction')

        @register
        def registered(_):
            if self.read_only is Fallback:
                self.read_only = False
            self.client.registered(self, lastTransaction)

        @register.catch
        def register_failed(exc):
            if (isinstance(exc, ZODB.POSException.ReadOnlyError) and
                self.read_only is Fallback):
                # We tried a write connection, degrade to a read-only one
                self.read_only = True
                register = self.promise(
                    'register', self.storage_key, self.read_only)
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

    exception_type_type = type(Exception)
    def message_received(self, data):
        if self.protocol_version is None:
            self.finish_connect(data)
        else:
            msgid, async, name, args = loads(data)
            if name == '.reply':
                future = self.futures.pop(msgid)
                if (isinstance(args, tuple) and len(args) > 1 and
                    type(args[0]) == self.exception_type_type and
                    issubclass(args[0], Exception)
                    ):
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

    message_id = 0
    def call(self, future, method, args):
        self.message_id += 1
        self.futures[self.message_id] = future
        self._write(dumps((self.message_id, False, method, args), 3))
        return future

    def promise(self, method, *args):
        return self.call(Promise(), method, args)

    # Methods called by the server:

    client_methods = (
        'invalidateTransaction', 'serialnos', 'info',
        'receiveBlobStart', 'receiveBlobChunk', 'receiveBlobStop',
        )
    client_delegated = client_methods[1:]

class Client:
    """asyncio low-level ZEO client interface
    """

    # All of the code in this class runs in a single dedicated
    # thread. Thus, we can mostly avoid worrying about interleaved
    # operations.

    # One place where special care was required was in cache setup on
    # connect.

    protocol = None
    ready = False

    def __init__(self, addrs, client, cache, storage_key, read_only, loop):
        """Create a client interface

        addr is either a host,port tuple or a string file name.

        client is a ClientStorage. It must be thread safe.

        cache is a ZEO.interfaces.IClientCache.
        """
        self.loop = loop
        self.addrs = addrs
        self.storage_key = storage_key
        self.read_only = read_only
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
                Protocol(addr, self, self.storage_key, self.read_only,
                         self.loop)
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
        logger.error("Registration or cache validation failed, %s", exc)
        if (self.protocol is None and not
            any(not p.closed for p in self.protocols)
            ):
            self.loop.call_later(9 + local_random.random(), self.try_connecting)

    def verify(self, last_transaction_promise):
        protocol = self.protocol

        @last_transaction_promise
        def finish_verify(server_tid):
            cache = self.cache
            if cache:
                cache_tid = cache.getLastTid()
                if not cache_tid:
                    logger.error("Non-empty cache w/o tid -- clearing")
                    cache.clear()
                    self.client.invalidateCache()
                    self.finished_verify(server_tid)
                elif cache_tid > server_tid:
                    raise AssertionError("Server behind client, %r < %r, %s",
                                         server_tid, cache_tid, protocol)
                elif cache_tid == server_tid:
                    self.finished_verify(server_tid)
                else:
                    @protocol.promise('getInvalidations', cache_tid)
                    def verify_invalidations(vdata):
                        if vdata:
                            tid, oids = vdata
                            for oid in oids:
                                cache.invalidate(oid, None)
                            return tid
                        else:
                            # cache is too old
                            logger.info("cache too old %s", protocol)
                            self.cache.clear()
                            self.client.invalidateCache()
                            return server_tid

                    verify_invalidations(self.finished_verify,
                                         self.connected.set_exception)
            else:
                self.finished_verify(server_tid)

        @finish_verify.catch
        def verify_failed(exc):
            del self.protocol
            self.register_failed(protocol, exc)

    def finished_verify(self, server_tid):
        self.cache.setLastTid(server_tid)
        self.ready = True
        self.connected.set_result(None)

    def call_async_threadsafe(self, future, method, args):
        if self.ready:
            self.protocol.call_async(method, args)
            future.set_result(None)
        else:
            future.set_exception(ZEO.Exceptions.ClientDisconnected())

    def _when_ready(self, func, result_future, *args):

        @self.connected.add_done_callback
        def done(future):
            e = future.exception()
            if e is not None:
                result_future.set_exception(e)
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

    def tpc_finish_threadsafe(self, future, tid, updates):
        if self.ready:
            @self.protocol.promise('tpc_finish', tid)
            def committed(_):
                cache = self.cache
                for oid, s, data in updates:
                    cache.invalidate(oid, tid)
                    if data and s != ResolvedSerial:
                        cache.store(oid, tid, None, data)
                cache.setLastTid(tid)
                future.set_result(None)

            committed.catch(future.set_exception)
        else:
            future.set_exception(ClientDisconnected())

    # Methods called by the server:

    client_methods = (
        'invalidateTransaction', 'serialnos', 'info',
        'receiveBlobStart', 'receiveBlobChunk', 'receiveBlobStop',
        )
    client_delegated = client_methods[1:]

    def invalidateTransaction(self, tid, oids):
        if self.ready:
            for oid in oids:
                self.cache.invalidate(oid, tid)
            self.cache.setLastTid(tid)
            self.client.invalidateTransaction(tid, oids)


class ClientRunner:

    def set_options(self, addrs, wrapper, cache, storage_key, read_only,
                    timeout=30):
        self.__args = addrs, wrapper, cache, storage_key, read_only
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

    def __init__(self, addrs, client, cache,
                 storage_key='1', read_only=False, timeout=30):
        self.set_options(addrs, client, cache, storage_key, read_only, timeout)
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

    next = success_callback = error_callback = cancelled = None

    def __call__(self, success_callback = None, error_callback = None):
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
