from .._compat import PY3

import mock
import os
import ssl
import unittest
from ZODB.config import storageFromString

from ..Exceptions import ClientDisconnected
from .. import runzeo

from .testConfig import ZEOConfigTestBase
from . import forker
from .threaded import threaded_server_tests

here = os.path.dirname(__file__)
server_cert = os.path.join(here, 'server.pem')
server_key  = os.path.join(here, 'server_key.pem')
serverpw_cert = os.path.join(here, 'serverpw.pem')
serverpw_key  = os.path.join(here, 'serverpw_key.pem')
client_cert = os.path.join(here, 'client.pem')
client_key  = os.path.join(here, 'client_key.pem')

@unittest.skipIf(forker.ZEO4_SERVER, "ZEO4 servers don't support SSL")
class SSLConfigTest(ZEOConfigTestBase):

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
                server-hostname zodb.org
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
            password-function ZEO.tests.testssl.pwfunc
            </ssl>""".format(serverpw_cert, serverpw_key, client_cert)
            )
        stop()

@unittest.skipIf(forker.ZEO4_SERVER, "ZEO4 servers don't support SSL")
@mock.patch(('asyncio' if PY3 else 'trollius') + '.ensure_future')
@mock.patch(('asyncio' if PY3 else 'trollius') + '.set_event_loop')
@mock.patch(('asyncio' if PY3 else 'trollius') + '.new_event_loop')
@mock.patch('ZEO.asyncio.client.new_event_loop')
@mock.patch('ZEO.asyncio.server.new_event_loop')
class SSLConfigTestMockiavellian(ZEOConfigTestBase):

    @mock.patch('ssl.create_default_context')
    def test_ssl_mockiavellian_server_no_ssl(self, factory, *_):
        server = create_server()
        self.assertFalse(factory.called)
        self.assertEqual(server.acceptor.ssl_context, None)
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
    def test_ssl_mockiavellian_server_ssl_no_auth(self, factory, *_):
        with self.assertRaises(SystemExit):
            # auth is required
            create_server(certificate=server_cert, key=server_key)

    @mock.patch('ssl.create_default_context')
    def test_ssl_mockiavellian_server_ssl_auth_file(self, factory, *_):
        server = create_server(
            certificate=server_cert, key=server_key, authenticate=__file__)
        context = server.acceptor.ssl_context
        self.assert_context(factory, context, cafile=__file__)
        server.close()

    @mock.patch('ssl.create_default_context')
    def test_ssl_mockiavellian_server_ssl_auth_dir(self, factory, *_):
        server = create_server(
            certificate=server_cert, key=server_key, authenticate=here)
        context = server.acceptor.ssl_context
        self.assert_context(factory, context, capath=here)
        server.close()

    @mock.patch('ssl.create_default_context')
    def test_ssl_mockiavellian_server_ssl_pw(self, factory, *_):
        server = create_server(
            certificate=server_cert,
            key=server_key,
            password_function='ZEO.tests.testssl.pwfunc',
            authenticate=here,
            )
        context = server.acceptor.ssl_context
        self.assert_context(
            factory, context, (server_cert, server_key, pwfunc), capath=here)
        server.close()

    @mock.patch('ssl.create_default_context')
    @mock.patch('ZEO.ClientStorage.ClientStorage')
    def test_ssl_mockiavellian_client_no_ssl(self, ClientStorage, factory, *_):
        client = ssl_client()
        self.assertFalse('ssl' in ClientStorage.call_args[1])
        self.assertFalse('ssl_server_hostname' in ClientStorage.call_args[1])

    @mock.patch('ssl.create_default_context')
    @mock.patch('ZEO.ClientStorage.ClientStorage')
    def test_ssl_mockiavellian_client_server_signed(
        self, ClientStorage, factory, *_
        ):
        client = ssl_client(certificate=client_cert, key=client_key)
        context = ClientStorage.call_args[1]['ssl']
        self.assertEqual(ClientStorage.call_args[1]['ssl_server_hostname'],
                         None)
        self.assert_context(
            factory, context, (client_cert, client_key, None),
            check_hostname=True)

        context.load_default_certs.assert_called_with()

    @mock.patch('ssl.create_default_context')
    @mock.patch('ZEO.ClientStorage.ClientStorage')
    def test_ssl_mockiavellian_client_auth_dir(
        self, ClientStorage, factory, *_
        ):
        client = ssl_client(
            certificate=client_cert, key=client_key, authenticate=here)
        context = ClientStorage.call_args[1]['ssl']
        self.assertEqual(ClientStorage.call_args[1]['ssl_server_hostname'],
                         None)
        self.assert_context(
            factory, context, (client_cert, client_key, None),
            capath=here,
            check_hostname=True,
            )
        context.load_default_certs.assert_not_called()

    @mock.patch('ssl.create_default_context')
    @mock.patch('ZEO.ClientStorage.ClientStorage')
    def test_ssl_mockiavellian_client_auth_file(
        self, ClientStorage, factory, *_
        ):
        client = ssl_client(
            certificate=client_cert, key=client_key, authenticate=server_cert)
        context = ClientStorage.call_args[1]['ssl']
        self.assertEqual(ClientStorage.call_args[1]['ssl_server_hostname'],
                         None)
        self.assert_context(
            factory, context, (client_cert, client_key, None),
            cafile=server_cert,
            check_hostname=True,
            )
        context.load_default_certs.assert_not_called()

    @mock.patch('ssl.create_default_context')
    @mock.patch('ZEO.ClientStorage.ClientStorage')
    def test_ssl_mockiavellian_client_pw(
        self, ClientStorage, factory, *_
        ):
        client = ssl_client(
            certificate=client_cert, key=client_key,
            password_function='ZEO.tests.testssl.pwfunc',
            authenticate=server_cert)
        context = ClientStorage.call_args[1]['ssl']
        self.assertEqual(ClientStorage.call_args[1]['ssl_server_hostname'],
                         None)
        self.assert_context(
            factory, context, (client_cert, client_key, pwfunc),
            cafile=server_cert,
            check_hostname=True,
            )

    @mock.patch('ssl.create_default_context')
    @mock.patch('ZEO.ClientStorage.ClientStorage')
    def test_ssl_mockiavellian_client_server_hostname(
        self, ClientStorage, factory, *_
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
        self, ClientStorage, factory, *_
        ):
        client = ssl_client(
            certificate=client_cert, key=client_key, authenticate=server_cert,
            check_hostname=False)
        context = ClientStorage.call_args[1]['ssl']
        self.assertEqual(ClientStorage.call_args[1]['ssl_server_hostname'],
                         None)
        self.assert_context(
            factory, context, (client_cert, client_key, None),
            cafile=server_cert,
            check_hostname=False,
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
          server 127.0.0.1:0
          {}
        </clientstorage>
        """.format(ssl_conf(**ssl_settings))
        )

def create_server(**ssl_settings):
    with open('conf', 'w') as f:
        f.write(
            """
            <zeo>
              address 127.0.0.1:0
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
    suite = unittest.TestSuite((
        unittest.makeSuite(SSLConfigTest),
        unittest.makeSuite(SSLConfigTestMockiavellian),
        ))
    suite.layer = threaded_server_tests
    return suite

# Helpers for other tests:

server_config = """
    <zeo>
      address 127.0.0.1:0
      <ssl>
        certificate {}
        key {}
        authenticate {}
      </ssl>
    </zeo>
    """.format(server_cert, server_key, client_cert)

def client_ssl(cafile=server_key,
               client_cert=client_cert,
               client_key=client_key,
               ):
    context = ssl.create_default_context(
        ssl.Purpose.CLIENT_AUTH, cafile=server_cert)

    context.load_cert_chain(client_cert, client_key)
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = False
    return context

# See
# https://discuss.pivotal.io/hc/en-us/articles/202653388-How-to-renew-an-expired-Apache-Web-Server-self-signed-certificate-using-the-OpenSSL-tool
# for instructions on updating the server.pem (the certificate) if
# needed. server.pem.csr is the request.
# This should do it:
# openssl x509 -req -days 999999 -in src/ZEO/tests/server.pem.csr -signkey src/ZEO/tests/server_key.pem -out src/ZEO/tests/server.pem
# If you need to create a new key first:
# openssl genrsa -out server_key.pem 2048
# These two files should then be copied to client_key.pem and client.pem.
