import asyncio
from _thread import allocate_lock
from time import sleep


try:
    ConnectionRefusedError
except NameError:
    class ConnectionRefusedError(OSError):
        pass


class Loop:
    """Simple loop for testing purposes.

    It calls callbacks directly (instead of in the next round);
    it remembers ``call_later`` calls rather than schedule them;
    it does not check calls to non threadsafe methods.
    """

    def __init__(self, addrs=(), debug=True):
        self.protocol = self.transport = None
        self.addrs = addrs
        self.get_debug = lambda: debug
        self.connecting = {}
        self.later = []
        self.exceptions = []

    def call_soon(self, func, *args, **kw):
        # Python 3.7+ calls us with a `context` keyword-only argument:
        kw.pop('context', None)
        assert not kw
        func(*args)

    def _connect(self, future, protocol_factory):
        self.protocol = protocol = protocol_factory()
        self.transport = transport = Transport(protocol)
        protocol.connection_made(transport)
        future.set_result((transport, protocol))

    def connect_connecting(self, addr):
        future, protocol_factory = self.connecting.pop(addr)
        self._connect(future, protocol_factory)

    def fail_connecting(self, addr):
        future, protocol_factory = self.connecting.pop(addr)
        if not future.cancelled():
            future.set_exception(ConnectionRefusedError())

    def create_connection(self, protocol_factory, host=None, port=None,
                          sock=None, ssl=None, server_hostname=None):
        future = asyncio.Future(loop=self)
        if sock is None:
            addr = host, port
            if addr in self.addrs:
                self._connect(future, protocol_factory)
            else:
                self.connecting[addr] = future, protocol_factory
        else:
            self._connect(future, protocol_factory)

        return future

    def create_unix_connection(self, protocol_factory, path):
        future = asyncio.Future(loop=self)
        if path in self.addrs:
            self._connect(future, protocol_factory)
        else:
            self.connecting[path] = future, protocol_factory

        return future

    def call_soon_threadsafe(self, func, *args):
        func(*args)
        return Handle()

    def call_later(self, delay, func, *args):
        handle = Handle()
        self.later.append((delay, func, args, handle))
        return handle

    def call_exception_handler(self, context):
        self.exceptions.append(context)

    closed = False

    def close(self):
        # break reference cycles
        Loop.__init__(self)
        self.closed = True

    def create_future(self):
        return asyncio.Future(loop=self)

    stopped = False

    def stop(self):
        self.stopped = True


AsyncioLoop = asyncio.get_event_loop_policy()._loop_factory


class FaithfulLoop(Loop, AsyncioLoop):
    """Testing loop variant with true ``asyncio`` ``call_*`` methods."""
    def __init__(self, addrs=(), debug=True):
        Loop.__init__(self, addrs, debug)
        AsyncioLoop.__init__(self)

    call_soon = AsyncioLoop.call_soon

    call_soon_threadsafe = AsyncioLoop.call_soon_threadsafe

    def call_later(self, delay, func, *args):
        th = AsyncioLoop.call_later(self, delay, func, *args)
        self.later.append((delay, func, args, th))
        return th

    def call_exception_handler(self, context):
        Loop.call_exception_handler(self, context)
        AsyncioLoop.call_exception_handler(self, context)  # log exception

    def _connect(self, future, protocol_factory):
        """a real ``asyncio`` loop checks that non threadsafe
        functions are only called from the ``asyncio`` context.
        Our protocols assume they are only called from within this
        context. The tests however call their method differently.
        Therefore, we wrap the protocol to properly delegate to the
        ``asyncio`` context.
        """
        protocol = self.protocol = protocol_factory()
        self.transport = transport = Transport(protocol)
        protocol.connection_made(transport)
        protocol = self.protocol = _ProtocolWrapper(self)
        future.set_result((transport, protocol))

    def stop(self):
        AsyncioLoop.stop(self)
        Loop.stop(self)

    def close(self):
        if not self.is_closed():
            AsyncioLoop.close(self)
            Loop.close(self)

    _inactivity_checker_scheduled = False
    _inactivity_lock = allocate_lock()

    def run_until_inactive(self):
        """return when the loop becomes inactive."""
        with self._inactivity_lock:
            if not self._inactivity_checker_scheduled:
                self._inactivity_checker_scheduled = True
                self.call_soon_threadsafe(self._check_inactive)
        while True:
            sleep(0.005)
            with self._inactivity_lock:
                if not self._inactivity_checker_scheduled:
                    return

    def _check_inactive(self):
        """check whether the loop has nothing to do.

        In this case, reset ``_inactivity_checker_scheduled``,
        otherwise, reschedule itself.
        """
        with self._inactivity_lock:
            # We use here implementation details
            if len(self._ready) == 0:  # inactive
                self._inactivity_checker_scheduled = False
            else:
                self.call_soon(self._check_inactive)


class _ProtocolWrapper:
    """Wrapped protocol.

    Protocol instances can only be used by their ``asyncio`` thread.
    The wrapper allows tests (run in a different thread) to
    use the wrapped protocol normally by properly delegating
    method calls to the ``asyncio`` thread.
    """

    def __init__(self, loop):
        self._loop = loop
        self._protocol = loop.protocol

    def data_received(self, data):
        # perform in IO thread
        self._loop.call_soon_threadsafe(self._protocol.data_received, data)
        self._loop.run_until_inactive()

    def connection_lost(self, exc):
        self._loop.call_soon_threadsafe(self._protocol.connection_lost, exc)
        self._loop.run_until_inactive()

    _protocol = None

    def __getattr__(self, attr):
        return getattr(self._protocol, attr)


class Handle:

    _cancelled = False

    def cancel(self):
        self._cancelled = True


class Transport:

    capacity = 1 << 64
    paused = False
    extra = dict(peername='1.2.3.4', sockname=('127.0.0.1', 4200), socket=None)

    def __init__(self, protocol):
        self.data = []
        self.protocol = protocol

    def write(self, data):
        self.data.append(data)
        self.check_pause()

    def writelines(self, lines):
        self.data.extend(lines)
        self.check_pause()

    def check_pause(self):
        if len(self.data) > self.capacity and not self.paused:
            self.paused = True
            self.protocol.pause_writing()

    def pop(self, count=None):
        if count:
            r = self.data[:count]
            del self.data[:count]
        else:
            r = self.data[:]
            del self.data[:]
        self.check_resume()
        return r

    def check_resume(self):
        if len(self.data) < self.capacity and self.paused:
            self.paused = False
            self.protocol.resume_writing()

    closed = False

    def close(self):
        if self.closed:
            return
        self.closed = True
        # honor the ``asyncio.Protocol`` obligation:
        # if ``connection_made`` has been called, there will
        # be exactly one ``connection_lost`` call.
        if not self.protocol.connection_lost_called:
            self.protocol.connection_lost(None)
        self.protocol = None  # break reference cycle

    def get_extra_info(self, name):
        return self.extra[name]


class AsyncRPC:
    """Adapt an asyncio API to an RPC to help hysterical tests
    """
    def __init__(self, api):
        self.api = api

    def __getattr__(self, name):
        return lambda *a, **kw: self.api.call(name, *a, **kw)


class ClientRunner:

    def __init__(self, addr, client, cache, storage, read_only, timeout,
                 **kw):
        self.addr = addr
        self.client = client
        self.cache = cache
        self.storage = storage
        self.read_only = read_only
        self.timeout = timeout,
        for name in kw:
            self.__dict__[name] = kw[name]

    def start(self, wait=True):
        pass

    def call(self, method, *args, **kw):
        return getattr(self, method)(*args)

    async_ = async_iter = call

    def wait(self, timeout=None):
        pass

    def close(self):
        self.cache.close()  # client responsibility
