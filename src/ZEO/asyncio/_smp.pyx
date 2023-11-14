# cython: language_level=3, boundscheck=False, wraparound=False

"""``cython`` implementation for ``SizedMessageProtocol``."""
from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.string cimport memcpy

import logging
import struct


logger = logging.getLogger(__name__)


cdef enum ReadState:
    process_size = 1
    process_message = 2
    closed = 0

cdef object pack = struct.pack
cdef object unpack = struct.unpack


cdef class SizedMessageProtocol:
    cdef object __closed
    cdef public object receive  # callback for received messages
    cdef public object connection_lost_called

    def __init__(self, receive):
        self.receive = receive
        self.__closed = self.connection_lost_called = False

    def set_receive(self, receive):
        self.receive = receive

    cdef public object transport

    def close(self):
        if self.__closed:
            return
        self.__closed = True
        self.eof_received()
        self.transport.close()
        # break reference cycles
        self.transport = self.receive = self.writelines = None

    # output
    cdef bint paused
    cdef list output  # buffer
    cdef object writelines

    cdef _write_message(self, bytes message):
        self.writelines((pack(">I", len(message)), message))

    def write_message(self, message):
        if self.paused:
            self.output.append(message)
        else:
            self._write_message(message)

    def write_message_iter(self, message_iter):
        it = iter(message_iter)
        if self.paused:
            self.output.append(it)
            return
        for message in it:
            self._write_message(message)
            if self.paused:
                self.output.append(it)
                return

    # protocol responsibilities
    def pause_writing(self):
        self.paused = 1

    def resume_writing(self):
        self.paused = 0
        cdef list output = self.output
        while output and not self.paused:
            message = output.pop(0)
            if type(message) is bytes:
                self._write_message(message)
            else:
                it = message
                for message in it:
                    self._write_message(message)
                    if self.paused:
                        self.output.insert(0, it)
                        return

    # input
    cdef ReadState read_state  # current read state
    cdef unsigned read_wanted  # wanted data size
    cdef unsigned received_count  # received unprocessed bytes
    cdef list chunk_buffer  # received data chunks
    cdef unsigned chunk_index  # unprocessed index in 1. chunk

    # protocol responsibilities
    def data_received(self, bytes data):
        self.chunk_buffer.append(data)
        self.received_count += len(data)
        cdef unsigned wanted = self.read_wanted
        cdef bytes target
        cdef unsigned char *tv
        cdef unsigned tvi
        cdef bytes chunk
        cdef const unsigned char *cv
        cdef unsigned ci
        cdef unsigned unprocessed, use, i
        while self.read_state and self.read_wanted <= self.received_count:
            wanted = self.read_wanted
            tv = target = PyBytes_FromStringAndSize(<char *> NULL, wanted)
            tvi = 0
            while wanted:
                cv = chunk = self.chunk_buffer[0]
                ci = self.chunk_index
                unprocessed = len(chunk) - ci
                if unprocessed > wanted:
                    use = wanted
                    self.chunk_index += wanted
                else:
                    use = unprocessed
                    self.chunk_buffer.pop(0)
                    self.chunk_index = 0
                if use <= 4:
                    for i in range(use):
                        tv[tvi + i] = cv[ci + i]
                else:
                    memcpy(&tv[tvi], &cv[ci], use)
                tvi += use
                wanted -= use
            self.received_count -= self.read_wanted
            if self.read_state == process_size:
                self.read_state = process_message
                self.read_wanted = unpack(">I", target)[0]
            else:  # read_state == process_message
                try:
                    self.receive(target)
                except Exception:
                    logger.exception("Processing message `%r` failed"
                                     % target)
                if self.read_state:  # not yet closed
                    self.read_state = process_size
                    self.read_wanted = 4

    def connection_made(self, transport):
        self.transport = transport
        self.writelines = transport.writelines
        self.paused = 0
        self.output = []
        self.read_state = process_size
        self.read_wanted = 4
        self.received_count = 0
        self.chunk_buffer = []
        self.chunk_index = 0


    def connection_lost(self, exc):
        self.connection_lost_called = True
        if self.__closed:
            return
        self.transport.close()

    def eof_received(self):
        self.read_state = closed
