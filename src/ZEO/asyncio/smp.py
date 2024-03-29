"""SizedMessageProtocol.

The protocal exchanges ``bytes`` messages.
On the wire, a message is represented by a 4 bytes length followed
by the message's bytes.
"""

import struct
from asyncio import Protocol
from logging import getLogger


logger = getLogger(__name__)


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

    transport = None

    def __init__(self, receive):
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
            it = iter(message_iter)
            if paused:  # equivalent to ``paused or output``
                append(it)
                return
            for message in it:
                _write_message(message)
                if paused:
                    append(it)
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


# Use C implementation if available
try:
    from ._smp import SizedMessageProtocol  # noqa: F811, F401
except ImportError:
    pass
