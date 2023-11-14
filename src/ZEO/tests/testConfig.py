##############################################################################
#
# Copyright (c) 2003 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

import unittest

from ZODB.config import storageFromString
from zope.testing import setupstack

from .forker import start_zeo_server
from .threaded import threaded_server_tests


class ZEOConfigTestBase(setupstack.TestCase):

    setUp = setupstack.setUpDirectory

    def start_server(self, settings='', **kw):

        for name, value in kw.items():
            settings += '\n{} {}\n'.format(name.replace('_', '-'), value)

        zeo_conf = """
        <zeo>
          address 127.0.0.1:0
          %s
        </zeo>
        """ % settings
        return start_zeo_server("<mappingstorage>\n</mappingstorage>\n",
                                zeo_conf, threaded=True)

    def start_client(self, addr, settings='', **kw):
        settings += '\nserver %s:%s\n' % addr
        for name, value in kw.items():
            settings += '\n{} {}\n'.format(name.replace('_', '-'), value)
        return storageFromString(
            """
            %import ZEO

            <clientstorage>
            {}
            </clientstorage>
            """.format(settings))

    def _client_assertions(self, client, addr,
                           connected=True,
                           cache_size=20 * (1 << 20),
                           cache_path=None,
                           blob_dir=None,
                           shared_blob_dir=False,
                           blob_cache_size=None,
                           blob_cache_size_check=10,
                           read_only=False,
                           read_only_fallback=False,
                           server_sync=False,
                           wait_timeout=30,
                           client_label=None,
                           storage='1',
                           name=None):
        self.assertEqual(client.is_connected(), connected)
        self.assertEqual(client._addr, [addr])
        self.assertEqual(client._cache.maxsize, cache_size)

        self.assertEqual(client._cache.path, cache_path)
        self.assertEqual(client.blob_dir, blob_dir)
        self.assertEqual(client.shared_blob_dir, shared_blob_dir)
        self.assertEqual(client._blob_cache_size, blob_cache_size)
        if blob_cache_size:
            self.assertEqual(client._blob_cache_size_check,
                             blob_cache_size * blob_cache_size_check // 100)
        self.assertEqual(client._is_read_only, read_only)
        self.assertEqual(client._read_only_fallback, read_only_fallback)
        self.assertEqual(client._server.timeout, wait_timeout)
        self.assertEqual(client._client_label, client_label)
        self.assertEqual(client._storage, storage)
        self.assertEqual(client.__name__,
                         name if name is not None else str(client._addr))


class ZEOConfigTest(ZEOConfigTestBase):

    def test_default_zeo_config(self, **client_settings):
        addr, stop = self.start_server()

        client = self.start_client(addr, **client_settings)
        self._client_assertions(client, addr, **client_settings)

        client.close()
        stop()

    def test_client_variations(self):

        for name, value in dict(cache_size=4200,
                                cache_path='test',
                                blob_dir='blobs',
                                blob_cache_size=424242,
                                read_only=True,
                                read_only_fallback=True,
                                server_sync=True,
                                wait_timeout=33,
                                client_label='test_client',
                                name='Test',
                                ).items():
            params = {name: value}
            self.test_default_zeo_config(**params)

    def test_blob_cache_size_check(self):
        self.test_default_zeo_config(blob_cache_size=424242,
                                     blob_cache_size_check=50)


def test_suite():
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ZEOConfigTest)
    suite.layer = threaded_server_tests
    return suite
