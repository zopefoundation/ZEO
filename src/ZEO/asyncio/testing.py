import asyncio
import pprint

class Loop:

    protocol = transport = None

    def __init__(self, addrs=(), debug=True):
        self.addrs = addrs
        self.get_debug = lambda : debug
        self.connecting = {}
        self.later = []
        self.exceptions = []

    def call_soon(self, func, *args):
        func(*args)

    def _connect(self, future, protocol_factory):
        self.protocol  = protocol  = protocol_factory()
        self.transport = transport = Transport()
        protocol.connection_made(transport)
        future.set_result((transport, protocol))

    def connect_connecting(self, addr):
        future, protocol_factory = self.connecting.pop(addr)
        self._connect(future, protocol_factory)

    def fail_connecting(self, addr):
        future, protocol_factory = self.connecting.pop(addr)
        if not future.cancelled():
            future.set_exception(ConnectionRefusedError())

    def create_connection(self, protocol_factory, host, port):
        future = asyncio.Future(loop=self)
        addr = host, port
        if addr in self.addrs:
            self._connect(future, protocol_factory)
        else:
            self.connecting[addr] = future, protocol_factory

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

    def call_later(self, delay, func, *args):
        self.later.append((delay, func, args))

    def call_exception_handler(self, context):
        self.exceptions.append(context)

class Transport:

    def __init__(self):
        self.data = []

    def write(self, data):
        self.data.append(data)

    def writelines(self, lines):
        self.data.extend(lines)

    def pop(self, count=None):
        if count:
            r = self.data[:count]
            del self.data[:count]
        else:
            r = self.data[:]
            del self.data[:]
        return r

    closed = False
    def close(self):
        self.closed = True
