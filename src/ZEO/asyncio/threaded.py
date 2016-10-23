# Thread-based event loop.

import concurrent.futures
import functools
import socket
import threading
import time

def run_in_thread(func):
    thread = threading.Thread(target=func)
    thread.setDaemon(True)
    thread.start()
    return thread

class EventLoop:

    def __init__(self):
        self.lock = threading.Lock()
        self.running = threading.Event()

    def run_forever(self):
        self.running.wait()

    def run_until_complete(self, future):
        @future.add_done_callback
        def _():
            self.running.set()
        self.running.wait()

    def stop(self):
        self.running.set()

    _closed = False
    def is_closed(self):
        return self._closed

    def close(self):
        self._closed = True

    def call_soon(self, callback, *args):
        callback(*args)

    def call_soon_threadsafe(self, callback, *args):
        with self.lock:
            callback(*args)

    def call_later(self, delay, callback, *args):
        handle = Handle()

        @run_in_thread
        @functools.wraps(callback)
        def _():
            time.sleep(delay)
            if not handle.cancelled:
                with self.lock:
                    callback(*args)

        return handle

    def ensure_future(self, future):
        return future
    create_task = ensure_future

    def create_connection(
        self,
        protocol_factory,
        host=None, port=None,
        ssl=None, server_hostname=None
        ):
        future = concurrent.futures.Future()

        @run_in_thread
        def connect():
            protocol = protocol_factory()
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((host, port))
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
                transport = Transport(sock)
                with self.lock:
                    protocol.connection_made(transport)
            except Exception as exc:
                future.set_exception(exc)
                return
            else:
                future.set_result(None)

            while True:
                try:
                    data = sock.recv(4096)
                except Exception as exc:
                    with self.lock:
                        protocol.connection_lost(exc)
                    break

                with self.lock:
                    if data:
                        protocol.data_received(data)
                    else:
                        protocol.connection_lost(None)
                        break

        return future

    def call_exception_handler(self, context):
        pass # ?

    debug = False
    def get_debug(self):
        return self.debug
    def set_debug(self, v):
        self.debug = v


class Transport:

    def __init__(self, sock):
        self.socket = sock
        self._extra = dict(
            socket = sock,
            peername = sock.getpeername(),
            sockname = sock.getsockname(),
            )

    def get_extra_info(self, name, default=None):
        """Get optional transport information."""
        return self._extra.get(name, default)

    def close(self):
        self.socket.close()

    def write(self, data):
        self.socket.sendall(data)

    def writelines(self, list_of_data):
        self.write(b''.join(list_of_data))

class Handle:
    cancelled = False

    def cancel(self):
        self.cancelled = True
