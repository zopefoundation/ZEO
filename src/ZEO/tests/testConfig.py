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

import mock
import os
import ssl
import unittest


from zope.testing import setupstack
from ZODB.config import storageFromString

from ..Exceptions import ClientDisconnected
from .. import runzeo

from .forker import start_zeo_server

here = os.path.dirname(__file__)
server_cert = os.path.join(here, 'server.pem')
server_key  = os.path.join(here, 'server_key.pem')
serverpw_cert = os.path.join(here, 'serverpw.pem')
serverpw_key  = os.path.join(here, 'serverpw_key.pem')
client_cert = os.path.join(here, 'client.pem')
client_key  = os.path.join(here, 'client_key.pem')

class ZEOConfigTest(setupstack.TestCase):

    setUp = setupstack.setUpDirectory

    def start_server(self, settings='', **kw):

        for name, value in kw.items():
            settings += '\n%s %s\n' % (name.replace('_', '-'), value)

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
            settings += '\n%s %s\n' % (name.replace('_', '-'), value)
        return storageFromString(
            """
            %import ZEO

            <clientstorage>
            {}
            </clientstorage>
            """.format(settings))

    def _client_assertions(
        self, client, addr,
        connected=True,
        cache_size=20 * (1<<20),
        cache_path=None,
        blob_dir=None,
        shared_blob_dir=False,
        blob_cache_size=None,
        blob_cache_size_check=10,
        read_only=False,
        read_only_fallback=False,
        wait_timeout=30,
        client_label=None,
        storage='1',
        name=None,
        ):
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

    def test_default_zeo_config(self, **client_settings):
        addr, stop = self.start_server()

        client = self.start_client(addr, **client_settings)
        self._client_assertions(client, addr, **client_settings)

        client.close()
        stop()

    def test_client_variations(self):

        for name, value in dict(
            cache_size=4200,
            cache_path='test',
            blob_dir='blobs',
            blob_cache_size=424242,
            read_only=True,
            read_only_fallback=True,
            wait_timeout=33,
            client_label='test_client',
            name='Test'
            ).items():
            params = {name: value}
            self.test_default_zeo_config(**params)

    def test_blob_cache_size_check(self):
        self.test_default_zeo_config(blob_cache_size=424242,
                                     blob_cache_size_check=50)

    def test_ssl_basic(self):
        # This shows that configuring ssl has an actual effect on connections.
        # Other SSL configuration tests will be Mockiavellian.

        # Also test that an SSL connection mismatch doesn't kill
        # the server loop.

        # An SSL client can't talk to a non-SSL server:
        addr, stop = self.start_server()
        with self.assertRaises(ClientDisconnected):
            self.start_client(
                addr,
                """<ssl>
                certificate {}
                key {}
                </ssl>""".format(client_cert, client_key), wait_timeout=1)

        # But a non-ssl one can:
        client = self.start_client(addr)
        self._client_assertions(client, addr)
        client.close()
        stop()

        # A non-SSL client can't talk to an SSL server:
        addr, stop = self.start_server(
            """<ssl>
            certificate {}
            key {}
            authenticate {}
            </ssl>""".format(server_cert, server_key, client_cert)
            )
        with self.assertRaises(ClientDisconnected):
            self.start_client(addr, wait_timeout=1)

        # But an SSL one can:
        client = self.start_client(
            addr,
            """<ssl>
                certificate {}
                key {}
                authenticate {}
                </ssl>""".format(client_cert, client_key, server_cert))
        self._client_assertions(client, addr)
        client.close()
        stop()

    def test_ssl_hostname_check(self):
        addr, stop = self.start_server(
            """<ssl>
            certificate {}
            key {}
            authenticate {}
            </ssl>""".format(server_cert, server_key, client_cert)
            )

        # Connext with bad hostname fails:

        with self.assertRaises(ClientDisconnected):
            client = self.start_client(
                addr,
                """<ssl>
                    certificate {}
                    key {}
                    authenticate {}
                    server-hostname example.org
                    </ssl>""".format(client_cert, client_key, server_cert),
                wait_timeout=1)

        # Connext with good hostname succeeds:
        client = self.start_client(
            addr,
            """<ssl>
                certificate {}
                key {}
                authenticate {}
                server-hostname zodb.org
                </ssl>""".format(client_cert, client_key, server_cert))
        self._client_assertions(client, addr)
        client.close()
        stop()

    def test_ssl_pw(self):
        addr, stop = self.start_server(
            """<ssl>
            certificate {}
            key {}
            authenticate {}
            password-function ZEO.tests.testConfig.pwfunc
            </ssl>""".format(serverpw_cert, serverpw_key, client_cert)
            )
        stop()

    @mock.patch('ssl.create_default_context')
    def test_ssl_mockiavellian_server_no_ssl(self, factory):
        server = create_server()
        self.assertFalse(factory.called)
        self.assertEqual(server.acceptor._Acceptor__ssl, None)
        server.close()

    def assert_context(
        self, factory, context,
        cert=(server_cert, server_key, None),
        verify_mode=ssl.CERT_REQUIRED,
        check_hostname=False,
        cafile=None, capath=None,
        ):
        factory.assert_called_with(
            ssl.Purpose.CLIENT_AUTH, cafile=cafile, capath=capath)
        context.load_cert_chain.assert_called_with(*cert)
        self.assertEqual(context, factory.return_value)
        self.assertEqual(context.verify_mode, verify_mode)
        self.assertEqual(context.check_hostname, check_hostname)

    @mock.patch('ssl.create_default_context')
    def test_ssl_mockiavellian_server_ssl_no_auth(self, factory):
        with self.assertRaises(SystemExit):
            # auth is required
            create_server(certificate=server_cert, key=server_key)

    @mock.patch('ssl.create_default_context')
    def test_ssl_mockiavellian_server_ssl_auth_file(self, factory):
        server = create_server(
            certificate=server_cert, key=server_key, authenticate=__file__)
        context = server.acceptor._Acceptor__ssl
        self.assert_context(factory, context, cafile=__file__)
        server.close()

    @mock.patch('ssl.create_default_context')
    def test_ssl_mockiavellian_server_ssl_auth_dir(self, factory):
        server = create_server(
            certificate=server_cert, key=server_key, authenticate=here)
        context = server.acceptor._Acceptor__ssl
        self.assert_context(factory, context, capath=here)
        server.close()

    @mock.patch('ssl.create_default_context')
    def test_ssl_mockiavellian_server_ssl_pw(self, factory):
        server = create_server(
            certificate=server_cert,
            key=server_key,
            password_function='ZEO.tests.testConfig.pwfunc',
            authenticate=here,
            )
        context = server.acceptor._Acceptor__ssl
        self.assert_context(
            factory, context, (server_cert, server_key, pwfunc), capath=here)
        server.close()

    @mock.patch('ssl.create_default_context')
    @mock.patch('ZEO.ClientStorage.ClientStorage')
    def test_ssl_mockiavellian_client_no_ssl(self, ClientStorage, factory):
        client = ssl_client()
        self.assertFalse('ssl' in ClientStorage.call_args[1])
        self.assertFalse('ssl_server_hostname' in ClientStorage.call_args[1])

    @mock.patch('ssl.create_default_context')
    @mock.patch('ZEO.ClientStorage.ClientStorage')
    def test_ssl_mockiavellian_client_server_signed(
        self, ClientStorage, factory
        ):
        client = ssl_client(certificate=client_cert, key=client_key)
        context = ClientStorage.call_args[1]['ssl']
        self.assertEqual(ClientStorage.call_args[1]['ssl_server_hostname'],
                         None)
        self.assert_context(
            factory, context, (client_cert, client_key, None),
            check_hostname=True)

    @mock.patch('ssl.create_default_context')
    @mock.patch('ZEO.ClientStorage.ClientStorage')
    def test_ssl_mockiavellian_client_auth_dir(
        self, ClientStorage, factory
        ):
        client = ssl_client(
            certificate=client_cert, key=client_key, authenticate=here)
        context = ClientStorage.call_args[1]['ssl']
        self.assertEqual(ClientStorage.call_args[1]['ssl_server_hostname'],
                         None)
        self.assert_context(
            factory, context, (client_cert, client_key, None),
            capath=here,
            )

    @mock.patch('ssl.create_default_context')
    @mock.patch('ZEO.ClientStorage.ClientStorage')
    def test_ssl_mockiavellian_client_auth_file(
        self, ClientStorage, factory
        ):
        client = ssl_client(
            certificate=client_cert, key=client_key, authenticate=server_cert)
        context = ClientStorage.call_args[1]['ssl']
        self.assertEqual(ClientStorage.call_args[1]['ssl_server_hostname'],
                         None)
        self.assert_context(
            factory, context, (client_cert, client_key, None),
            cafile=server_cert,
            )

    @mock.patch('ssl.create_default_context')
    @mock.patch('ZEO.ClientStorage.ClientStorage')
    def test_ssl_mockiavellian_client_pw(
        self, ClientStorage, factory
        ):
        client = ssl_client(
            certificate=client_cert, key=client_key,
            password_function='ZEO.tests.testConfig.pwfunc',
            authenticate=server_cert)
        context = ClientStorage.call_args[1]['ssl']
        self.assertEqual(ClientStorage.call_args[1]['ssl_server_hostname'],
                         None)
        self.assert_context(
            factory, context, (client_cert, client_key, pwfunc),
            check_hostname=False,
            cafile=server_cert,
            )

    @mock.patch('ssl.create_default_context')
    @mock.patch('ZEO.ClientStorage.ClientStorage')
    def test_ssl_mockiavellian_client_server_hostname(
        self, ClientStorage, factory
        ):
        client = ssl_client(
            certificate=client_cert, key=client_key, authenticate=server_cert,
            server_hostname='example.com')
        context = ClientStorage.call_args[1]['ssl']
        self.assertEqual(ClientStorage.call_args[1]['ssl_server_hostname'],
                         'example.com')
        self.assert_context(
            factory, context, (client_cert, client_key, None),
            cafile=server_cert,
            check_hostname=True,
            )

    @mock.patch('ssl.create_default_context')
    @mock.patch('ZEO.ClientStorage.ClientStorage')
    def test_ssl_mockiavellian_client_check_hostname(
        self, ClientStorage, factory
        ):
        client = ssl_client(
            certificate=client_cert, key=client_key, authenticate=server_cert,
            check_hostname=True)
        context = ClientStorage.call_args[1]['ssl']
        self.assertEqual(ClientStorage.call_args[1]['ssl_server_hostname'],
                         None)
        self.assert_context(
            factory, context, (client_cert, client_key, None),
            cafile=server_cert,
            check_hostname=True,
            )

def args(*a, **kw):
    return a, kw

def ssl_conf(**ssl_settings):
    if ssl_settings:
        ssl_conf = '<ssl>\n' + '\n'.join(
            '{} {}'.format(name.replace('_', '-'), value)
            for name, value in ssl_settings.items()
            ) + '\n</ssl>\n'
    else:
        ssl_conf = ''

    return ssl_conf

def ssl_client(**ssl_settings):
    return storageFromString(
        """%import ZEO

        <clientstorage>
          server localhost:0
          {}
        </clientstorage>
        """.format(ssl_conf(**ssl_settings))
        )

def create_server(**ssl_settings):
    with open('conf', 'w') as f:
        f.write(
            """
            <zeo>
              address localhost:0
              {}
            </zeo>
            <mappingstorage>
            </mappingstorage>
            """.format(ssl_conf(**ssl_settings)))

    options = runzeo.ZEOOptions()
    options.realize(['-C', 'conf'])
    s = runzeo.ZEOServer(options)
    s.open_storages()
    s.create_server()
    return s.server

pwfunc = lambda : '1234'

def test_suite():
    return unittest.makeSuite(ZEOConfigTest)
