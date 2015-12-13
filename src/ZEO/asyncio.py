from pickle import loads, dumps
import asyncio
import concurrent.futures
import logging
import struct
import threading

logger = logging.getLogger(__name__)

class Disconnected(Exception):
    pass

class BaseTransportAdapter:
    def __init__(self, transport):
        self.transport = transport
    def close(self):
        self.transport.close
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
        self.transport.write(struct.pack(">I", len(message)))
        self.transport.write(message)

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

class ClientProtocol(asyncio.Protocol):
    """asyncio low-level ZEO client interface
    """

    def __init__(self, addr,
                 client=None, storage_key='1', read_only=False, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop
        self.addr = addr
        self.storage_key = storage_key
        self.read_only = read_only
        self.client = client
        self.connected = asyncio.Future()

    def protocol_factory(self):
        return SizedProtocolAdapter(self)

    def connect(self):
        self.protocol_version = None
        self.futures = {} # outstanding requests {request_id -> future}
        if isinstance(self.addr, tuple):
            host, port = self.addr
            cr = self.loop.create_connection(self.protocol_factory, host, port)
        else:
            cr = self.loop.create_unix_connection(
                self.protocol_factory, self.addr)

        future = asyncio.async(cr, loop=self.loop)
        @future.add_done_callback
        def done_connecting(future):
            e = future.exception()
            if e is not None:
                self.connected.set_exception(e)

        return self.connected

    def connection_made(self, transport):
        logger.info("Connected")
        self.transport = SizedTransportAdapter(transport)

    def connection_lost(self, exc):
        logger.info("Disconnected, %r", exc)
        for f in self.futures.values():
            d.set_exception(exc or Disconnected())
        self.futures = {}
        self.connect() # Reconnect

    exception_type_type = type(Exception)
    def data_received(self, data):
        if self.protocol_version is None:
            self.protocol_version = data
            self.transport.write(data) # pleased to meet you version :)
            self.call_async('register', self.storage_key, self.read_only)
            self.connected.set_result(data)
        else:
            msgid, async, name, args = loads(data)
            if name == '.reply':
                future = self.futures.pop(msgid)
                if (isinstance(args, tuple) and len(args) > 1 and
                    type(args[0]) == self.exception_type_type and
                    issubclass(r_args[0], Exception)
                    ):
                    future.set_exception(args[0]) # XXX security checks
                else:
                    future.set_result(args)
            else:
                assert async # clients only get async calls
                if self.client:
                    getattr(self.client, name)(*args) # XXX security
                else:
                    logger.info('called %r %r', (name, args))

    def call_async(self, method, *args):
        # XXX connection status...
        self.transport.write(dumps((0, True, method, args), 3))

    message_id = 0
    def call(self, method, *args):
        future = asyncio.Future()
        self.message_id += 1
        self.futures[self.message_id] = future
        self.transport.write(dumps((self.message_id, False, method, args), 3))
        return future

    def call_concurrent(self, result_future, method, *args):
        future = self.call(method, *args)
        @future.add_done_callback
        def concurrent_result(future):
            if future.exception() is None:
                result_future.set_result(future.result())
            else:
                result_future.set_exception(future.exception())

class ClientThread:
    """Thread wrapper for client interface

    A ClientProtocol is run in a dedicated thread.

    Calls to it are made in a thread-safe fashion.
    """

    def __init__(self, addr,
                 client=None, storage_key='1', read_only=False, timeout=None):
        self.addr = addr
        self.client = client
        self.storage_key = storage_key
        self.read_only = read_only
        self.connected = concurrent.futures.Future()
        threading.Thread(target=self.run,
                         name='zeo_client_'+storage_key,
                         daemon=True,
                         ).start()
        self.connected.result(timeout)

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.loop = loop
        self.proto = ClientProtocol(
            self.addr, None, self.storage_key, self.read_only)
        f = self.proto.connect()
        @f.add_done_callback
        def thread_done_connecting(future):
            e = future.exception()
            if e is not None:
                self.connected.set_exception(e)
            else:
                self.connected.set_result(None) # XXX prob return some info

        loop.run_forever()

    def call_async(self, method, *args):
        self.loop.call_soon_threadsafe(self.proto.call_async, method, *args)

    def call(self, method, *args, timeout=None):
        result = concurrent.futures.Future()
        self.loop.call_soon_threadsafe(
            self.proto.call_concurrent, result, method, *args)
        return result.result()
