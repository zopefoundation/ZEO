import unittest

from zope.testing import setupstack

from .. import server, client

from ZEO import _forker as forker

if forker.ZEO4_SERVER:
    server_ping_method = 'lastTransaction'
    server_zss = 'connections'
else:
    server_ping_method = 'ping'
    server_zss = 'zeo_storages_by_storage_id'

class SyncTests(setupstack.TestCase):

    def instrument(self):
        self.__ping_calls = 0

        server = getattr(forker, self.__name + '_server')

        [zs] = getattr(server.server, server_zss)['1']
        orig_ping = getattr(zs, server_ping_method)
        def ping():
            self.__ping_calls += 1
            return orig_ping()

        setattr(zs, server_ping_method, ping)

    def test_server_sync(self):
        self.__name = 's%s' % id(self)
        addr, stop = server(name=self.__name)

        # By default the client sync method is a noop:
        c = client(addr)
        self.instrument()
        c.sync()
        self.assertEqual(self.__ping_calls, 0)
        c.close()

        # But if we pass server_sync:
        c = client(addr, server_sync=True)
        self.instrument()
        c.sync()
        self.assertEqual(self.__ping_calls, 1)
        c.close()

        stop()
