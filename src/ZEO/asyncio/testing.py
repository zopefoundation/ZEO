import asyncio

try:
    ConnectionRefusedError
except NameError:
    class ConnectionRefusedError(OSError):
        pass

import pprint

class Loop:
    """Simple loop for testing purposes.

    It calls callbacks directly (instead of in the next round);
    it remembers ``call_later`` calls rather than schedule them;
    it does not check calls to non threadsafe methods.
    """

    protocol = transport = None

    def __init__(self, addrs=(), debug=True):
        self.addrs = addrs
        self.get_debug = lambda : debug
        self.connecting = {}
        self.later = []
        self.exceptions = []
        self.tasks = []

    def call_soon(self, func, *args, **kw):
        # Python 3.7+ calls us with a `context` keyword-only argument:
        kw.pop('context', None)
        assert not kw
        func(*args)

    def _connect(self, future, protocol_factory):
        self.protocol  = protocol  = protocol_factory()
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

    def create_connection(
        self, protocol_factory, host=None, port=None, sock=None,
        ssl=None, server_hostname=None
        ):
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
        Loop.__init__(self)  # break reference cycles
        self.closed = True

    stopped = False
    def stop(self):
        self.stopped = True


AsyncioLoop = asyncio.get_event_loop_policy()._loop_factory

class FaithfulLoop(Loop, AsyncioLoop):
    """Testing loop variant with true ``asyncio`` ``call_*`` methods."""
    def __init__(self, addrs=(), debug=True, wait=None):
        Loop.__init__(self, addrs, debug)
        AsyncioLoop.__init__(self)
        self.wait = wait

    call_soon = AsyncioLoop.call_soon

    def call_soon_threadsafe(self, func, *args, **kw):
        handle = AsyncioLoop.call_soon_threadsafe(self, func, *args, **kw)
        if self.wait is not None:
            self.wait()
        return handle

    def call_later(self, delay, func, *args):
        th = AsyncioLoop.call_later(self, delay, func, *args)
        self.later.append((delay, func, args, th))
        return th

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
            self.wait = None  # break reference cycle


class _ProtocolWrapper:
    """Wrapped protocol.

    It properly delegates to the ``asyncio`` thread.
    """

    def __init__(self, loop):
        self._loop = loop
        self._protocol = loop.protocol

    def data_received(self, data):
        # perform in IO thread
        self._loop.call_soon_threadsafe(self._protocol.data_received, data)

    def connection_lost(self, exc):
        self._loop.call_soon_threadsafe(self._protocol.connection_lost, exc)

    _protocol = None
    def __getattr__(self, attr):
        return getattr(self._protocol, attr)


class Handle(object):

    cancelled = False

    def cancel(self):
        self.cancelled = True


class Transport(object):

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
        self.closed = True

    def get_extra_info(self, name):
        return self.extra[name]

class AsyncRPC(object):
    """Adapt an asyncio API to an RPC to help hysterical tests
    """
    def __init__(self, api):
        self.api = api

    def __getattr__(self, name):
        return lambda *a, **kw: self.api.call(name, *a, **kw)

class ClientRunner(object):

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
        pass
