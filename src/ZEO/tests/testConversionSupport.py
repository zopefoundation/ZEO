##############################################################################
#
# Copyright (c) 2006 Zope Foundation and Contributors.
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
import doctest
import unittest


class FakeStorageBase:

    def __getattr__(self, name):
        if name in ('getTid', 'history', 'load', 'loadSerial',
                    'lastTransaction', 'getSize', 'getName', 'supportsUndo',
                    'tpc_transaction'):
            return lambda *a, **k: None
        raise AttributeError(name)

    def isReadOnly(self):
        return False

    def __len__(self):
        return 4


class FakeStorage(FakeStorageBase):

    def record_iternext(self, next=None):
        if next is None:
            next = '0'
        next = str(int(next) + 1)
        oid = next
        if next == '4':
            next = None

        return oid, oid*8, 'data ' + oid, next


class FakeServer:
    storages = {
        '1': FakeStorage(),
        '2': FakeStorageBase(),
        }
    lock_managers = storages

    def register_connection(*args):
        return None, None

    client_conflict_resolution = False


class FakeConnection:
    protocol_version = b'Z5'
    addr = 'test'

    def call_soon_threadsafe(f, *a):
        return f(*a)

    async_ = async_threadsafe = None


def test_server_record_iternext():
    """

On the server, record_iternext calls are simply delegated to the
underlying storage.

    >>> import ZEO.StorageServer

    >>> zeo = ZEO.StorageServer.ZEOStorage(FakeServer(), False)
    >>> zeo.notify_connected(FakeConnection())
    >>> zeo.register('1', False)

    >>> next = None
    >>> while 1:
    ...     oid, serial, data, next = zeo.record_iternext(next)
    ...     print(oid)
    ...     if next is None:
    ...         break
    1
    2
    3
    4

The storage info also reflects the fact that record_iternext is supported.

    >>> zeo.get_info()['supports_record_iternext']
    True

    >>> zeo = ZEO.StorageServer.ZEOStorage(FakeServer(), False)
    >>> zeo.notify_connected(FakeConnection())
    >>> zeo.register('2', False)

    >>> zeo.get_info()['supports_record_iternext']
    False

"""


def test_client_record_iternext():
    """Test client storage delegation to the network client

The client simply delegates record_iternext calls to it's server stub.

There's really no decent way to test ZEO without running too much crazy
stuff.  I'd rather do a lame test than a really lame test, so here goes.

First, fake out the connection manager so we can make a connection:

    >>> import ZEO

    >>> class Client(ZEO.asyncio.testing.ClientRunner):
    ...
    ...    def record_iternext(self, next=None):
    ...        if next == None:
    ...           next = '0'
    ...        next = str(int(next) + 1)
    ...        oid = next
    ...        if next == '4':
    ...            next = None
    ...
    ...        return oid, oid*8, 'data ' + oid, next
    ...

    >>> client = ZEO.client(
    ...     '', wait=False, _client_factory=Client)

Now we'll have our way with it's private _server attr:

    >>> next = None
    >>> while 1:
    ...     oid, serial, data, next = client.record_iternext(next)
    ...     print(oid)
    ...     if next is None:
    ...         break
    1
    2
    3
    4
    >>> client.close()

"""


def test_suite():
    return doctest.DocTestSuite()
