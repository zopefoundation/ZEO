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
from asyncio import Protocol
import logging
import struct


logger = logging.getLogger(__name__)


class ZEOBaseProtocol(Protocol):
    """ZEO protocol base class for the common features."""

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
            closing = self.closing = self.loop.create_future()
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
        # set up lower level sized message protocol
        smp = self.sm_protocol = SizedMessageProtocol(
            self.loop, self._first_message)  # creates reference cycle
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
            closing = self.closing = self.loop.create_future()
        if not closing.done():
            closing.set_result(True)

    # internal
    def _first_message(self, protocol_version):
        self.sm_protocol.set_receive(self.message_received)
        self.finish_connection(protocol_version)

    # ``uvloop`` workaround
    # We define ``data_reveived`` in ``connection_made``.
    # ``uvloop``, however, caches ``protocol.data_received`` before
    # it calls ``connection_made`` - at a consequence, data is not
    # received
    # The method below is overridden in ``connection_made``.
    def data_received(self, data):
        self.data_received(data)  # not an infinite loop, because overridden


class SizedMessageProtocol(Protocol):
    """asyncio protocol for the exchange of sized messages.

    A protocol object can be used as a connection.
    Write methods are
    ``write_message`` (write a single message)
    and ``write_message_iter`` (write the messages generated by an iterator).
    A received message is reported via the ``receive`` callback.

    Messages are received in the same order as they have been
    written.

    ``SizedMessageProtocol`` instances can be used concurrently
    from coroutines (executed in the same thread).
    They are not thread safe.
    """
    def __init__(self, loop, receive):
        self.receive = receive

    def connection_made(self, transport):
        self.transport = transport

        # output handling
        pack = struct.pack
        paused = False  # if ``paused`` we must buffer output
        output = []  # output buffer - contains messages or iterators
        append = output.append
        writelines = transport.writelines

        def _write_message(message):
            """hand the message over to the transport.

            May set ``paused``.
            """
            writelines((pack(">I", len(message)), message))

        # Note: Outside ``resume_writing``
        # ``not paused`` implies ``not output``. This
        # allows to use ``if paused`` instead of
        # ``if paused or output`` in ``write_message`` and
        # ``write_message_iter``.

        def write_message(message):
            if paused:  # equivalent to ``paused or output``
                append(message)
            else:
                _write_message(message)

        self.write_message = write_message

        def write_message_iter(message_iter):
            data = iter(message_iter)
            if paused:  # equivalent to ``paused or output``
                append(data)
                return
            for message in data:
                _write_message(message)
                if paused:
                    append(data)
                    return

        self.write_message_iter = write_message_iter

        def resume_writing():
            # precondition: ``paused`` and "at least 1 message writable"
            nonlocal paused
            paused = False
            while output and not paused:
                message = output.pop(0)
                if isinstance(message, bytes):
                    # a message
                    _write_message(message)
                else:
                    # an iterator
                    it = message
                    for message in it:
                        _write_message(message)
                        if paused:  # paused again. Put iter back.
                            output.insert(0, it)
                            break
            # post condition: ``paused or not output``

        self.resume_writing = resume_writing

        def pause_writing():
            nonlocal paused
            paused = True

        self.pause_writing = pause_writing

        # input handling
        # the following implements a state machine with
        # states ``process_size``, ``process_message`` and ``closed``
        process_size = 1
        process_message = 2
        closed = None
        read_state = process_size  # current state
        read_wanted = 4  # bytes required for this state
        received_count = 0  # number of received (not yet processed) bytes
        received_buffer = []  # received data chunks
        chunk_index = 0  # first unprocessed byte in first chunk
        unpack = struct.unpack

        def data_received(data):
            nonlocal received_count, read_state, read_wanted, chunk_index
            received_buffer.append(data)
            received_count += len(data)
            # ``not read_state`` means "closed"
            while read_state and read_wanted <= received_count:
                # transfer ``read_wanted`` bytes into ``data``
                data = None
                wanted = read_wanted
                while wanted:
                    chunk = received_buffer[0]
                    ch_unprocessed = len(chunk) - chunk_index
                    if ch_unprocessed > wanted:
                        n_index = chunk_index + wanted
                        fragment = chunk[chunk_index:n_index]
                        chunk_index = n_index
                        wanted = 0
                    else:
                        del received_buffer[0]
                        fragment = \
                            chunk[chunk_index:] if chunk_index else chunk
                        chunk_index = 0
                        wanted -= ch_unprocessed
                    if data is None:  # typical case
                        data = fragment
                    else:
                        data += fragment
                received_count -= read_wanted
                # process ``data``
                if read_state is process_size:
                    read_state = process_message
                    read_wanted = unpack(">I", data)[0]
                else:  # ``read_state is process_message``
                    try:
                        self.receive(data)  # may close: ``not read_state``
                    except Exception:
                        logger.exception("Processing message `%r` failed"
                                         % data)
                    if read_state:  # not yet closed
                        read_state = process_size
                        read_wanted = 4

        # the following introduces a reference cycle broken in ``close``
        self.data_received = data_received

        def eof_received():
            nonlocal read_state
            read_state = closed

        self.eof_received = eof_received

        self.connected = True

    def set_receive(self, receive):
        self.receive = receive

    __closed = False

    def close(self):
        if self.__closed:
            return
        self.__closed = True
        self.eof_received()
        self.transport.close()
        # break reference cycles
        self.transport = self.receive = self.data_received = None

    # We define ``connection_lost`` to close the transport
    # in order to avoid a ``ResourceWarning``
    # about an unclosed SSL transport -- it should not be necessary
    # as the transport informed us about the lost connection.
    # It also helps for some tests which call ``connection_lost``
    # without transport intervention.
    connection_lost_called = False  # for tests

    def connection_lost(self, exc):
        self.connection_lost_called = True
        if self.__closed:
            return
        self.transport.close()
