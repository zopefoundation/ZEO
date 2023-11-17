"""Clients can pass credentials to a server.

This is an experimental feature to enable server authentication and
authorization.
"""
import unittest

from zope.testing import setupstack

import ZEO.StorageServer

from .threaded import threaded_server_tests


class ClientAuthTests(setupstack.TestCase):

    def setUp(self):
        self.setUpDirectory()
        self.__register = ZEO.StorageServer.ZEOStorage.register

    def tearDown(self):
        ZEO.StorageServer.ZEOStorage.register = self.__register

    def test_passing_credentials(self):

        # First, we'll temporarily swap the storage server register
        # method with one that let's is see credentials that were passed:

        creds_log = []

        def register(zs, storage_id, read_only, credentials=self):
            creds_log.append(credentials)
            return self.__register(zs, storage_id, read_only)

        ZEO.StorageServer.ZEOStorage.register = register

        # Now start an in process server
        addr, stop = ZEO.server()

        # If we connect, without providing credentials, then no
        # credentials will be passed to register:

        client = ZEO.client(addr)

        self.assertEqual(creds_log, [self])
        client.close()
        creds_log.pop()

        # Even if we pass credentials, they'll be ignored
        creds = dict(user='me', password='123')
        client = ZEO.client(addr, credentials=creds)
        self.assertEqual(creds_log, [self])
        client.close()

        stop()


def test_suite():
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ClientAuthTests)
    suite.layer = threaded_server_tests
    return suite
