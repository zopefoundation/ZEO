"""Testing helpers
"""
import ZEO.StorageServer

from ..asyncio.server import best_protocol_version


class ServerProtocol:

    method = ('register', )

    def __init__(self, zs,
                 protocol_version=best_protocol_version,
                 addr='test-address'):
        self.calls = []
        self.addr = addr
        self.zs = zs
        self.protocol_version = protocol_version
        zs.notify_connected(self)

    closed = False

    def close(self):
        if not self.closed:
            self.closed = True
            self.zs.notify_disconnected()

    def call_soon_threadsafe(self, func, *args):
        func(*args)

    def async_(self, *args):
        self.calls.append(args)

    async_threadsafe = async_


class StorageServer:
    """Create a client interface to a StorageServer.

    This is for testing StorageServer. It interacts with the storgr
    server through its network interface, but without creating a
    network connection.
    """

    def __init__(self, test, storage,
                 protocol_version=b'Z' + best_protocol_version,
                 **kw):
        self.test = test
        self.storage_server = ZEO.StorageServer.StorageServer(
            None, {'1': storage}, **kw)
        self.zs = self.storage_server.create_client_handler()
        self.protocol = ServerProtocol(self.zs,
                                       protocol_version=protocol_version)
        self.zs.register('1', kw.get('read_only', False))

    def assert_calls(self, test, *argss):
        if argss:
            for args in argss:
                test.assertEqual(self.protocol.calls.pop(0), args)
        else:
            test.assertEqual(self.protocol.calls, ())

    def unpack_result(self, result):
        """For methods that return Result objects, unwrap the results
        """
        result, callback = result.args
        callback()
        return result
