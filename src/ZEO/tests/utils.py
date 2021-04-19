"""Testing helpers
"""
import sys

import ZEO.StorageServer
from ..asyncio.server import best_protocol_version

class ServerProtocol(object):

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

class StorageServer(object):
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


class HighContextSwitchFrequencyLayer(object):
    """layer ensuring a high context switch frequency
    
    Race condition probability depends on context switch frequency.
    For race condition tests it is therefore important to ensure
    a high switch frequency.

    By default, Python 2 switches every 100 interpreter instructions
    (fast enough) while Python 3 switches every 5 ms (usually too slow).
    """
    @classmethod
    def setUp(cls):
        if hasattr(sys, "getswitchinterval"):
            # PY3
            cls.restore = sys.setswitchinterval, sys.getswitchinterval()
            # estimate interpreter instruction time
            from time import time
            c = compile("i = 1\n" * 50, "-str-", "exec")  # 100 instrs
            s = time()
            exec(c, {})
            t = time() - s  # approx. time for 100 interpreter instructions
            # change frequency
            sys.setswitchinterval(t)
        else:
            # PY2
            cls.restore = sys.setcheckinterval, sys.getcheckinterval()
            sys.setcheckinterval(100)

    @classmethod
    def tearDown(cls):
        cls.restore[0](cls.restore[1])
