from .._compat import PY3

if PY3:
    import asyncio
else:
    import trollius as asyncio

try:
    ConnectionRefusedError
except NameError:
    class ConnectionRefusedError(OSError):
        pass

import pprint

class Loop(object):

    protocol = transport = None

    def __init__(self, addrs=(), debug=True):
        self.addrs = addrs
        self.get_debug = lambda : debug
        self.connecting = {}
        self.later = []
        self.exceptions = []

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
        self.closed = True

    stopped = False
    def stop(self):
        self.stopped = True

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
