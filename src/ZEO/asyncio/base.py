from .._compat import PY3

if PY3:
    import asyncio
else:
    import trollius as asyncio

import logging
import socket
from struct import unpack
import sys

logger = logging.getLogger(__name__)

INET_FAMILIES = socket.AF_INET, socket.AF_INET6

class Protocol(asyncio.Protocol):
    """asyncio low-level ZEO base interface
    """

    # All of the code in this class runs in a single dedicated
    # thread. Thus, we can mostly avoid worrying about interleaved
    # operations.

    # One place where special care was required was in cache setup on
    # connect. See finish connect below.

    transport = protocol_version = None

    def __init__(self, loop, addr):
        self.loop = loop
        self.addr = addr
        self.input  = [] # Input buffer when assembling messages
        self.output = [] # Output buffer when paused
        self.paused = [] # Paused indicator, mutable to avoid attr lookup

        # Handle the first message, the protocol handshake, differently
        self.message_received = self.first_message_received

    def __repr__(self):
        return self.name

    closed = False
    def close(self):
        if not self.closed:
            self.closed = True
            if self.transport is not None:
                self.transport.close()

    def connection_made(self, transport):
        logger.info("Connected %s", self)


        if sys.version_info < (3, 6):
            sock = transport.get_extra_info('socket')
            if sock is not None and sock.family in INET_FAMILIES:
                # See https://bugs.python.org/issue27456 :(
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        self.transport = transport

        paused = self.paused
        output = self.output
        append = output.append
        writelines = transport.writelines
        from struct import pack

        def write(message):
            if paused:
                append(message)
            else:
                writelines((pack(">I", len(message)), message))

        self._write = write

        def writeit(data):
            # Note, don't worry about combining messages.  Iters
            # will be used with blobs, in which case, the individual
            # messages will be big to begin with.
            data = iter(data)
            for message in data:
                writelines((pack(">I", len(message)), message))
                if paused:
                    append(data)
                    break

        self._writeit = writeit

    got = 0
    want = 4
    getting_size = True
    def data_received(self, data):

        # Low-level input handler collects data into sized messages.

        # Note that the logic below assume that when new data pushes
        # us over what we want, we process it in one call until we
        # need more, because we assume that excess data is all in the
        # last item of self.input. This is why the exception handling
        # in the while loop is critical.  Without it, an exception
        # might cause us to exit before processing all of the data we
        # should, when then causes the logic to be broken in
        # subsequent calls.

        self.got += len(data)
        self.input.append(data)
        while self.got >= self.want:
            try:
                extra = self.got - self.want
                if extra == 0:
                    collected = b''.join(self.input)
                    self.input = []
                else:
                    input = self.input
                    self.input = [input[-1][-extra:]]
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
            except Exception:
                logger.exception("data_received %s %s %s",
                                 self.want, self.got, self.getting_size)

    def first_message_received(self, protocol_version):
        # Handler for first/handshake message, set up in __init__
        del self.message_received # use default handler from here on
        self.finish_connect(protocol_version)

    def call_async(self, method, args):
        self._write(self.encode(0, True, method, args))

    def call_async_iter(self, it):
        self._writeit(self.encode(0, True, method, args)
                      for method, args in it)

    def pause_writing(self):
        self.paused.append(1)

    def resume_writing(self):
        paused = self.paused
        del paused[:]
        output = self.output
        writelines = self.transport.writelines
        from struct import pack
        while output and not paused:
            message = output.pop(0)
            if isinstance(message, bytes):
                writelines((pack(">I", len(message)), message))
            else:
                data = message
                for message in data:
                    writelines((pack(">I", len(message)), message))
                    if paused: # paused again. Put iter back.
                        output.insert(0, data)
                        break

    def get_peername(self):
        return self.transport.get_extra_info('peername')
