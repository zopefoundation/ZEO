"""ZEO Protocol.

A ZEO protocol instance can be used as a connection.
It exchanges ``bytes`` messages.
Messages are sent via the methods
``write_message`` (send a single message) and
``write_message_iter`` (send the messages generated by an iterator).
Received messages are reported via callbacks.
Messages are received in the same order as they have been written;
especially, the messages wrote with ``write_message_iter``
are received as contiguous messages.

The first message transmits the protocol version.
Its callback is ``finish_connection``.
The first byte of the protocol version message identifies
an encoding type; the remaining bytes specify the version.
``finish_connection`` is expected to set up
methods ``encode`` and ``decode`` corresponding to the
encoding type.

Followup messages carry encoded tuples
*msgid*, *async_flag*, *name*, *args*
representing either calls (synchronous or asynchronous) or replies.
Their callback is ``message_received``.

ZEO protocol instances can be used concurrently from coroutines (executed
in the same thread).
They are not thread safe.

The ZEO protocol sits on top of a sized message protocol.

The ZEO protocol has client and server variants.
"""
import logging
import socket
import sys

from .compat import asyncio
from .smp import SizedMessageProtocol


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

    protocol_version = None

    def __init__(self, loop, addr):
        self.loop = loop
        self.addr = addr

    def __repr__(self):
        return self.name

    closed = False
    sm_protocol = None

    def close(self):
        if not self.closed:
            self.closed = True
            # can get closed before ``sm_protocol`` set up
            if self.sm_protocol is not None:
                self.sm_protocol.close()

    #  ``Protocol`` responsibilities -- defined in ``connection_made``
    # data_received
    # pause_writing
    # resume_writing
    def connection_made(self, transport):
        logger.info("Connected %s", self)

        if sys.version_info < (3, 6):
            sock = transport.get_extra_info('socket')
            if sock is not None and sock.family in INET_FAMILIES:
                # See https://bugs.python.org/issue27456 :(
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)

        # set up lower level sized message protocol
        smp = self.sm_protocol = SizedMessageProtocol(self.first_message_received)
        smp.connection_made(transport)  # takes over ``transport``
        self.data_received = smp.data_received
        self.pause_writing = smp.pause_writing
        self.resume_writing = smp.resume_writing
        self.write_message = smp.write_message
        self.write_message_iter = smp.write_message_iter

    def first_message_received(self, protocol_version):
        self.sm_protocol.set_receive(self.message_received)
        self.finish_connection(protocol_version)

    def call_async(self, method, args):
        """call method named *method* asynchronously with *args*."""
        self.write_message(self.encode(0, True, method, args))

    def call_async_iter(self, it):
        self.write_message_iter(self.encode(0, True, method, args)
                                for method, args in it)

    def get_peername(self):
        return self.sm_protocol.transport.get_extra_info('peername')
