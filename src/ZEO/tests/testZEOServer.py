#
# Fix AttributeError: 'ZEOServer' object has no attribute 'server' in
# ZEOServer.main
#
import unittest

from ZEO.runzeo import ZEOServer


class TestStorageServer:

    def __init__(self, fail_create_server):
        self.called = []
        if fail_create_server: raise RuntimeError()

    def close(self):
        self.called.append("close")


class TestZEOServer(ZEOServer):

    def __init__(self, fail_create_server=False, fail_loop_forever=False):
        ZEOServer.__init__(self, None)
        self.called = []
        self.fail_create_server = fail_create_server
        self.fail_loop_forever = fail_loop_forever

    def setup_default_logging(self):
        self.called.append("setup_default_logging")

    def check_socket(self):
        self.called.append("check_socket")

    def clear_socket(self):
        self.called.append("clear_socket")

    def make_pidfile(self):
        self.called.append("make_pidfile")

    def open_storages(self):
        self.called.append("open_storages")

    def setup_signals(self):
        self.called.append("setup_signals")

    def create_server(self):
        self.called.append("create_server")
        self.server = TestStorageServer(self.fail_create_server)

    def loop_forever(self):
        self.called.append("loop_forever")
        if self.fail_loop_forever: raise RuntimeError()

    def close_server(self):
        self.called.append("close_server")
        ZEOServer.close_server(self)

    def clear_socket(self):
        self.called.append("clear_socket")

    def remove_pidfile(self):
        self.called.append("remove_pidfile")


class AttributeErrorTests(unittest.TestCase):

    def testFailCreateServer(self):
        # Demonstrate the AttributeError
        zeo = TestZEOServer(fail_create_server=True)
        self.assertRaises(RuntimeError, zeo.main)


class CloseServerTests(unittest.TestCase):

    def testCallSequence(self):
        # The close_server hook is called after loop_forever
        # has returned
        zeo = TestZEOServer()
        zeo.main()
        self.assertEqual(zeo.called, [
            "setup_default_logging",
            "check_socket",
            "clear_socket",
            "make_pidfile",
            "open_storages",
            "setup_signals",
            "create_server",
            "loop_forever",
            "close_server", # New
            "clear_socket",
            "remove_pidfile",
            ])
        # The default implementation closes the storage server
        self.assertEqual(hasattr(zeo, "server"), True)
        self.assertEqual(zeo.server.called, ["close"])

    def testFailLoopForever(self):
        # The close_server hook is called if loop_forever exits
        # with an exception
        zeo = TestZEOServer(fail_loop_forever=True)
        self.assertRaises(RuntimeError, zeo.main)
        self.assertEqual(zeo.called, [
            "setup_default_logging",
            "check_socket",
            "clear_socket",
            "make_pidfile",
            "open_storages",
            "setup_signals",
            "create_server",
            "loop_forever",
            "close_server",
            "clear_socket",
            "remove_pidfile",
            ])
        # The storage server has been closed
        self.assertEqual(hasattr(zeo, "server"), True)
        self.assertEqual(zeo.server.called, ["close"])

    def testFailCreateServer(self):
        # The close_server hook is called if create_server exits
        # with an exception
        zeo = TestZEOServer(fail_create_server=True)
        self.assertRaises(RuntimeError, zeo.main)
        self.assertEqual(zeo.called, [
            "setup_default_logging",
            "check_socket",
            "clear_socket",
            "make_pidfile",
            "open_storages",
            "setup_signals",
            "create_server",
            "close_server",
            "clear_socket",
            "remove_pidfile",
            ])
        # The server attribute is present but None
        self.assertEqual(hasattr(zeo, "server"), True)
        self.assertEqual(zeo.server, None)


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(AttributeErrorTests))
    suite.addTest(unittest.makeSuite(CloseServerTests))
    return suite
