"""Low-level protocol adapters

Derived from ngi connection adapters and filling a similar role to the
old zrpc smac layer for sized messages.
"""
import struct

class BaseTransportAdapter:
    def __init__(self, transport):
        self.transport = transport
    def close(self):
        self.transport.close()
    def is_closing(self):
        return self.transport.is_closing()
    def get_extra_info(self, name, default=None):
        return self.transport.get_extra_info(name, default)
    def pause_reading(self):
        self.transport.pause_reading()
    def resume_reading(self):
        self.transport.resume_reading()
    def abort(self):
        self.transport.abort()
    def can_write_eof(self):
        return self.transport.can_write_eof()
    def get_write_buffer_size(self):
        return self.transport.get_write_buffer_size()
    def get_write_buffer_limits(self):
        return self.transport.get_write_buffer_limits()
    def set_write_buffer_limits(self, high=None, low=None):
        self.transport.set_write_buffer_limits(high, low)
    def write(self, data):
        self.transport.write(data)
    def writelines(self, list_of_data):
        self.transport.writelines(list_of_data)
    def write_eof(self):
        self.transport.write_eof()

class BaseProtocolAdapter:
    def __init__(self, protocol):
        self.protocol = protocol
    def connection_made(self, transport):
        self.protocol.connection_made(transport)
    def connection_lost(self, exc):
        self.protocol.connection_lost(exc)
    def data_received(self, data):
        self.protocol.data_received(data)
    def eof_received(self):
        return self.protocol.eof_received()

class SizedTransportAdapter(BaseTransportAdapter):
    """Sized-message transport adapter
    """

    def write(self, message):
        self.transport.writelines((struct.pack(">I", len(message)), message))

    def writelines(self, list_of_data):
        self.transport.writelines(sized_iter(list_of_data))

def sized_iter(data):
    for message in data:
        yield struct.pack(">I", len(message))
        yield message

class SizedProtocolAdapter(BaseProtocolAdapter):

    def __init__(self, protocol):
        self.protocol = protocol
        self.want = 4
        self.got = 0
        self.getting_size = True
        self.input = []

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
                self.want = struct.unpack(">I", collected)[0]
                self.getting_size = False
            else:
                self.want = 4
                self.getting_size = True
                self.protocol.data_received(collected)
