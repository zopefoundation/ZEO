import asyncio

class Loop:

    def __init__(self, debug=True):
        self.get_debug = lambda : debug

    def call_soon(self, func, *args):
        func(*args)

    def create_connection(self, protocol_factory, host, port):
        self.protocol  = protocol  = protocol_factory()
        self.transport = transport = Transport()
        future = asyncio.Future(loop=self)
        future.set_result((transport, protocol))
        protocol.connection_made(transport)
        return future

    def call_soon_threadsafe(self, func, *args):
        func(*args)

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
