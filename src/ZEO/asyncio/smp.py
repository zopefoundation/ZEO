"""SizedMessageProtocol.

The protocal exchanges ``bytes`` messages.
On the wire, a message is represented by a 4 bytes length followed
by the message's bytes.
"""

from .compat import asyncio
from logging import getLogger
import struct

logger = getLogger(__name__)


class SizedMessageProtocol(asyncio.Protocol):
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
        paused = []  # if ``paused`` we must buffer output
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
                    break

        self.write_message_iter = write_message_iter

        def resume_writing():
            # precondition: ``paused`` and "at least 1 message writable"
            del paused[:]
            while output and not paused:
                message = output.pop(0)
                if isinstance(message, bytes):
                    # a message
                    _write_message(message)
                else:
                    # an iterator
                    data = message
                    for message in data:
                        _write_message(message)
                        if paused:  # paused again. Put iter back.
                            output.insert(0, data)
                            break
            # post condition: ``paused or not output``

        self.resume_writing = resume_writing

        def pause_writing():
            paused.append(1)

        self.pause_writing = pause_writing

        # input handling
        self.got = 0
        self.want = 4
        self.getting_size = True
        self.input = []  # Input buffer when assembling messages
        unpack = struct.unpack

        def data_received(data):
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
                    try:
                        self.receive(collected)
                    except Exception:
                        logger.exception("Processing message `%r` failed"
                                         % collected)

        self.data_received = data_received

    def set_receive(self, receive):
        self.receive = receive

    __closed = False

    def close(self):
        if self.__closed:
            return
        self.__closed = True
        self.transport.close()
