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


class ZEOBaseProtocol(asyncio.Protocol):
    """ZEO protocol base class for the common features."""

    # All of the code in this class runs in a single dedicated
    # thread. Thus, we can mostly avoid worrying about interleaved
    # operations.

    # One place where special care was required was in cache setup on
    # connect. See finish connect below.

    protocol_version = None

    def __init__(self, loop, name):
        self.loop = loop
        self.name = name

    # API -- defined in ``connection_made``
    # write_message(message)
    # write_message_iter(message_iter)
    def call_async(self, method, args):
        """call method named *method* asynchronously with *args*."""
        self.write_message(self.encode(0, True, method, args))

    def call_async_iter(self, it):
        self.write_message_iter(self.encode(0, True, method, args)
                                for method, args in it)

    def get_peername(self):
        return self.sm_protocol.transport.get_extra_info('peername')

    def protocol_factory(self):
        return self

    closing = None  # ``None`` or closed future
    sm_protocol = None

    def close(self):
        """schedule closing, return closed future."""
        # with ``asyncio``, ``close`` only schedules the closing;
        # close completion is signalled via a call to ``connection_lost``.
        closing = self.closing
        if closing is None:
            closing = self.closing = create_future(self.loop)
            # can get closed before ``sm_protocol`` set up
            if self.sm_protocol is not None:
                # will eventually cause ``connection_lost``
                self.sm_protocol.close()
            else:
                closing.set_result(True)
        elif self.sm_protocol is not None:
            self.sm_protocol.close()  # no problem if repeated
        return closing

    def __repr__(self):
        cls = self.__class__
        return "%s.%s(%s)" % (
            cls.__module__, cls.__name__, self.name)

    # to be defined by deriving classes
    # def finish_connection(protocol_version_message)
    # def message_received(message)

    #  ``Protocol`` responsibilities -- defined in ``connection_made``
    # data_received
    # eof_received
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
        # creates reference cycle
        smp = self.sm_protocol = SizedMessageProtocol(self._first_message)
        smp.connection_made(transport)  # takes over ``transport``
        self.data_received = smp.data_received
        self.eof_received = smp.eof_received
        self.pause_writing = smp.pause_writing
        self.resume_writing = smp.resume_writing
        self.write_message = smp.write_message
        self.write_message_iter = smp.write_message_iter

    # In real life ``connection_lost`` is only called by
    # the transport and ``asyncio.Protocol`` guarantees that
    # it is called exactly once (if ``connection_made`` has
    # been called) or not at all.
    # Some tests, however, call ``connection_lost`` themselves.
    # The following attribute helps to ensure that ``connection_lost``
    # is called exactly once.
    connection_lost_called = False

    def connection_lost(self, exc):
        """The call signals close completion."""
        self.connection_lost_called = True
        self.sm_protocol.connection_lost(exc)
        closing = self.closing
        if closing is None:
            closing = self.closing = create_future(self.loop)
        if not closing.done():
            closing.set_result(True)

    # internal
    def _first_message(self, protocol_version):
        self.sm_protocol.set_receive(self.message_received)
        self.finish_connection(protocol_version)

    # ``uvloop`` workaround
    # We define ``data_received`` in ``connection_made``.
    # ``uvloop``, however, caches ``protocol.data_received`` before
    # it calls ``connection_made`` - at a consequence, data is not
    # received
    # The method below is overridden in ``connection_made``.
    def data_received(self, data):
        self.data_received(data)  # not an infinite loop, because overridden


def create_future(loop):
    mkf = getattr(loop, 'create_future', None)  # py3.5+
    if mkf is not None:
        return mkf()
    else:
        return asyncio.Future(loop=loop)        # py2
