##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Test suite for ZEO based on ZODB.tests."""
from __future__ import print_function
import multiprocessing
import re

from ZEO.ClientStorage import ClientStorage
from ZEO.tests import forker, Cache, CommitLockTests, ThreadTests
from ZEO.tests import IterationTests
from ZEO._compat import PY3

from ZODB.Connection import TransactionMetaData
from ZODB.tests import StorageTestBase, BasicStorage,  \
     TransactionalUndoStorage,  \
     PackableStorage, Synchronization, ConflictResolution, RevisionStorage, \
     MTStorage, ReadOnlyStorage, IteratorStorage, RecoveryStorage
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle
from ZODB.utils import maxtid, p64, u64, z64
from zope.testing import renormalizing

import doctest
import logging
import os
import persistent
import pprint
import re
import shutil
import signal
import stat
import ssl
import sys
import tempfile
import threading
import time
import transaction
import unittest
import ZEO.StorageServer
import ZEO.tests.ConnectionTests
import ZODB
import ZODB.blob
import ZODB.tests.hexstorage
import ZODB.tests.testblob
import ZODB.tests.util
import ZODB.utils
import zope.testing.setupstack

from . import testssl

logger = logging.getLogger('ZEO.tests.testZEO')

class DummyDB:
    def invalidate(self, *args):
        pass
    def invalidateCache(*unused):
        pass
    transform_record_data = untransform_record_data = lambda self, v: v


class CreativeGetState(persistent.Persistent):
    def __getstate__(self):
        self.name = 'me'
        return super(CreativeGetState, self).__getstate__()


class MiscZEOTests:
    """ZEO tests that don't fit in elsewhere."""

    def checkCreativeGetState(self):
        # This test covers persistent objects that provide their own
        # __getstate__ which modifies the state of the object.
        # For details see bug #98275

        db = ZODB.DB(self._storage)
        cn = db.open()
        rt = cn.root()
        m = CreativeGetState()
        m.attr = 'hi'
        rt['a'] = m

        # This commit used to fail because of the `Mine` object being put back
        # into `changed` state although it was already stored causing the ZEO
        # cache to bail out.
        transaction.commit()
        cn.close()

    def checkLargeUpdate(self):
        obj = MinPO("X" * (10 * 128 * 1024))
        self._dostore(data=obj)

    def checkZEOInvalidation(self):
        addr = self._storage._addr
        storage2 = self._wrap_client(
            ClientStorage(addr, wait=1, **self._client_options()))
        try:
            oid = self._storage.new_oid()
            ob = MinPO('first')
            revid1 = self._dostore(oid, data=ob)
            data, serial = storage2.load(oid, '')
            self.assertEqual(zodb_unpickle(data), MinPO('first'))
            self.assertEqual(serial, revid1)
            revid2 = self._dostore(oid, data=MinPO('second'), revid=revid1)

            # Now, storage 2 should eventually get the new data. It
            # will take some time, although hopefully not much.
            # We'll poll till we get it and whine if we time out:
            for n in range(30):
                time.sleep(.1)
                data, serial = storage2.load(oid, '')
                if (serial == revid2 and
                    zodb_unpickle(data) == MinPO('second')
                    ):
                    break
            else:
                raise AssertionError('Invalidation message was not sent!')
        finally:
            storage2.close()

    def checkVolatileCacheWithImmediateLastTransaction(self):
        # Earlier, a ClientStorage would not have the last transaction id
        # available right after successful connection, this is required now.
        addr = self._storage._addr
        storage2 = ClientStorage(addr, **self._client_options())
        self.assert_(storage2.is_connected())
        self.assertEquals(ZODB.utils.z64, storage2.lastTransaction())
        storage2.close()

        self._dostore()
        storage3 = ClientStorage(addr, **self._client_options())
        self.assert_(storage3.is_connected())
        self.assertEquals(8, len(storage3.lastTransaction()))
        self.assertNotEquals(ZODB.utils.z64, storage3.lastTransaction())
        storage3.close()

class GenericTestBase(
    # Base class for all ZODB tests
    StorageTestBase.StorageTestBase):

    shared_blob_dir = False
    blob_cache_dir = None
    server_debug = False

    def setUp(self):
        StorageTestBase.StorageTestBase.setUp(self)
        logger.info("setUp() %s", self.id())
        zport, stop = forker.start_zeo_server(
            self.getConfig(), self.getZEOConfig(), debug=self.server_debug)
        self._servers = [stop]
        if not self.blob_cache_dir:
            # This is the blob cache for ClientStorage
            self.blob_cache_dir = tempfile.mkdtemp(
                'blob_cache',
                dir=os.path.abspath(os.getcwd()))
        self._storage = self._wrap_client(
            ClientStorage(
                zport, '1', cache_size=20000000,
                min_disconnect_poll=0.5, wait=1,
                wait_timeout=60, blob_dir=self.blob_cache_dir,
                shared_blob_dir=self.shared_blob_dir,
                **self._client_options()),
            )
        self._storage.registerDB(DummyDB())

    def getZEOConfig(self):
        return forker.ZEOConfig(('127.0.0.1', 0))

    def _wrap_client(self, client):
        return client

    def _client_options(self):
        return {}

    def tearDown(self):
        self._storage.close()
        for stop in self._servers:
            stop()
        StorageTestBase.StorageTestBase.tearDown(self)

class GenericTests(
    GenericTestBase,

    # ZODB test mixin classes (in the same order as imported)
    BasicStorage.BasicStorage,
    PackableStorage.PackableStorage,
    Synchronization.SynchronizedStorage,
    MTStorage.MTStorage,
    ReadOnlyStorage.ReadOnlyStorage,
    # ZEO test mixin classes (in the same order as imported)
    CommitLockTests.CommitLockVoteTests,
    ThreadTests.ThreadTests,
    # Locally defined (see above)
    MiscZEOTests,
    ):
    """Combine tests from various origins in one class.
    """

    def open(self, read_only=0):
        # Needed to support ReadOnlyStorage tests.  Ought to be a
        # cleaner way.
        addr = self._storage._addr
        self._storage.close()
        self._storage = ClientStorage(
            addr, read_only=read_only, wait=1, **self._client_options())

    def checkWriteMethods(self):
        # ReadOnlyStorage defines checkWriteMethods.  The decision
        # about where to raise the read-only error was changed after
        # Zope 2.5 was released.  So this test needs to detect Zope
        # of the 2.5 vintage and skip the test.

        # The __version__ attribute was not present in Zope 2.5.
        if hasattr(ZODB, "__version__"):
            ReadOnlyStorage.ReadOnlyStorage.checkWriteMethods(self)

    def checkSortKey(self):
        key = '%s:%s' % (self._storage._storage, self._storage._server_addr)
        self.assertEqual(self._storage.sortKey(), key)

    def _do_store_in_separate_thread(self, oid, revid, voted):

        def do_store():
            store = ZEO.ClientStorage.ClientStorage(
                self._storage._addr, **self._client_options())
            try:
                t = transaction.get()
                store.tpc_begin(t)
                store.store(oid, revid, b'x', '', t)
                store.tpc_vote(t)
                store.tpc_finish(t)
            except Exception as v:
                import traceback
                print('E'*70)
                print(v)
                traceback.print_exception(*sys.exc_info())
            finally:
                store.close()

        thread = threading.Thread(name='T2', target=do_store)
        thread.setDaemon(True)
        thread.start()
        thread.join(voted and .1 or 9)
        return thread

class FullGenericTests(
    GenericTests,
    Cache.TransUndoStorageWithCache,
    ConflictResolution.ConflictResolvingStorage,
    ConflictResolution.ConflictResolvingTransUndoStorage,
    PackableStorage.PackableUndoStorage,
    RevisionStorage.RevisionStorage,
    TransactionalUndoStorage.TransactionalUndoStorage,
    IteratorStorage.IteratorStorage,
    IterationTests.IterationTests,
    ):
    """Extend GenericTests with tests that MappingStorage can't pass."""

class FileStorageRecoveryTests(StorageTestBase.StorageTestBase,
                               RecoveryStorage.RecoveryStorage):

    def getConfig(self):
        return """\
        <filestorage 1>
        path %s
        </filestorage>
        """ % tempfile.mktemp(dir='.')

    def _new_storage(self):
        zconf = forker.ZEOConfig(('127.0.0.1', 0))
        zport, stop = forker.start_zeo_server(self.getConfig(),
                                              zconf)
        self._servers.append(stop)

        blob_cache_dir = tempfile.mkdtemp(dir='.')

        storage = ClientStorage(
            zport, '1', cache_size=20000000,
            min_disconnect_poll=0.5, wait=1,
            wait_timeout=60, blob_dir=blob_cache_dir)
        storage.registerDB(DummyDB())
        return storage

    def setUp(self):
        StorageTestBase.StorageTestBase.setUp(self)
        self._servers = []

        self._storage = self._new_storage()
        self._dst = self._new_storage()

    def tearDown(self):
        self._storage.close()
        self._dst.close()

        for stop in self._servers:
            stop()
        StorageTestBase.StorageTestBase.tearDown(self)

    def new_dest(self):
        return self._new_storage()


class FileStorageTests(FullGenericTests):
    """Test ZEO backed by a FileStorage."""

    def getConfig(self):
        return """\
        <filestorage 1>
        path Data.fs
        </filestorage>
        """

    _expected_interfaces = (
        ('ZODB.interfaces', 'IStorageRestoreable'),
        ('ZODB.interfaces', 'IStorageIteration'),
        ('ZODB.interfaces', 'IStorageUndoable'),
        ('ZODB.interfaces', 'IStorageCurrentRecordIteration'),
        ('ZODB.interfaces', 'IExternalGC'),
        ('ZODB.interfaces', 'IStorage'),
        ('zope.interface', 'Interface'),
        )

    def checkInterfaceFromRemoteStorage(self):
        # ClientStorage itself doesn't implement IStorageIteration, but the
        # FileStorage on the other end does, and thus the ClientStorage
        # instance that is connected to it reflects this.
        self.failIf(ZODB.interfaces.IStorageIteration.implementedBy(
            ZEO.ClientStorage.ClientStorage))
        self.failUnless(ZODB.interfaces.IStorageIteration.providedBy(
            self._storage))
        # This is communicated using ClientStorage's _info object:
        self.assertEquals(self._expected_interfaces,
            self._storage._info['interfaces']
            )

class FileStorageSSLTests(FileStorageTests):

    def getZEOConfig(self):
        return testssl.server_config

    def _client_options(self):
        return {'ssl': testssl.client_ssl()}


class FileStorageHexTests(FileStorageTests):
    _expected_interfaces = (
        ('ZODB.interfaces', 'IStorageRestoreable'),
        ('ZODB.interfaces', 'IStorageIteration'),
        ('ZODB.interfaces', 'IStorageUndoable'),
        ('ZODB.interfaces', 'IStorageCurrentRecordIteration'),
        ('ZODB.interfaces', 'IExternalGC'),
        ('ZODB.interfaces', 'IStorage'),
        ('ZODB.interfaces', 'IStorageWrapper'),
        ('zope.interface', 'Interface'),
        )

    def getConfig(self):
        return """\
        %import ZODB.tests
        <hexstorage>
        <filestorage 1>
        path Data.fs
        </filestorage>
        </hexstorage>
        """

class FileStorageClientHexTests(FileStorageHexTests):

    def getConfig(self):
        return """\
        %import ZODB.tests
        <serverhexstorage>
        <filestorage 1>
        path Data.fs
        </filestorage>
        </serverhexstorage>
        """

    def _wrap_client(self, client):
        return ZODB.tests.hexstorage.HexStorage(client)

class ClientConflictResolutionTests(
    GenericTestBase,
    ConflictResolution.ConflictResolvingStorage,
    ):

    def getConfig(self):
        return '<mappingstorage>\n</mappingstorage>\n'

    def getZEOConfig(self):
        return forker.ZEOConfig(('', 0), client_conflict_resolution=True)

class MappingStorageTests(GenericTests):
    """ZEO backed by a Mapping storage."""

    def getConfig(self):
        return """<mappingstorage 1/>"""

    def checkSimpleIteration(self):
        # The test base class IteratorStorage assumes that we keep undo data
        # to construct our iterator, which we don't, so we disable this test.
        pass

    def checkUndoZombie(self):
        # The test base class IteratorStorage assumes that we keep undo data
        # to construct our iterator, which we don't, so we disable this test.
        pass

class DemoStorageTests(
    GenericTests,
    ):

    def getConfig(self):
        return """
        <demostorage 1>
          <filestorage 1>
             path Data.fs
          </filestorage>
        </demostorage>
        """

    def checkUndoZombie(self):
        # The test base class IteratorStorage assumes that we keep undo data
        # to construct our iterator, which we don't, so we disable this test.
        pass

    def checkPackWithMultiDatabaseReferences(self):
        pass # DemoStorage pack doesn't do gc
    checkPackAllRevisions = checkPackWithMultiDatabaseReferences

class ZRPCConnectionTests(ZEO.tests.ConnectionTests.CommonSetupTearDown):

    def getConfig(self, path, create, read_only):
        return """<mappingstorage 1/>"""

    def checkCatastrophicClientLoopFailure(self):
        # Test what happens when the client loop falls over
        self._storage = self.openClientStorage()

        import zope.testing.loggingsupport
        handler = zope.testing.loggingsupport.InstalledHandler(
            'ZEO.asyncio.client')


        # We no longer implement the event loop, we we no longer know
        # how to break it.  We'll just stop it instead for now.
        self._storage._server.loop.call_soon_threadsafe(
            self._storage._server.loop.stop)

        forker.wait_until(
            'disconnected',
            lambda : not self._storage.is_connected()
            )

        log = str(handler)
        handler.uninstall()
        self.assert_("Client loop stopped unexpectedly" in log)

    def checkExceptionLogsAtError(self):
        # Test the exceptions are logged at error
        self._storage = self.openClientStorage()
        self._dostore(z64, data=MinPO("X" * (10 * 128 * 1024)))

        from zope.testing.loggingsupport import InstalledHandler
        handler = InstalledHandler('ZEO.asyncio.client')
        import ZODB.POSException
        self.assertRaises(TypeError, self._storage.history, z64, None)
        self.assertTrue(re.search(" from server: .*TypeError", str(handler)))

        # POSKeyErrors and ConflictErrors aren't logged:
        handler.clear()
        self.assertRaises(ZODB.POSException.POSKeyError,
                          self._storage.history, None, None)
        handler.uninstall()
        self.assertEquals(str(handler), '')

    def checkConnectionInvalidationOnReconnect(self):

        storage = ClientStorage(self.addr, min_disconnect_poll=0.1)
        self._storage = storage
        assert storage.is_connected()

        class DummyDB:
            _invalidatedCache = 0
            def invalidateCache(self):
                self._invalidatedCache += 1
            def invalidate(*a, **k):
                pass
            transform_record_data = untransform_record_data = \
                                    lambda self, data: data

        db = DummyDB()
        storage.registerDB(db)

        base = db._invalidatedCache

        # Now we'll force a disconnection and reconnection
        storage._server.loop.call_soon_threadsafe(
            storage._server.client.protocol.connection_lost,
            ValueError('test'))

        # and we'll wait for the storage to be reconnected:
        for i in range(100):
            if storage.is_connected():
                if db._invalidatedCache > base:
                    break
            time.sleep(0.1)
        else:
            raise AssertionError("Couldn't connect to server")

        # Now, the root object in the connection should have been invalidated:
        self.assertEqual(db._invalidatedCache, base+1)


class CommonBlobTests:

    def getConfig(self):
        return """
        <blobstorage 1>
          blob-dir blobs
          <filestorage 2>
            path Data.fs
          </filestorage>
        </blobstorage>
        """

    blobdir = 'blobs'
    blob_cache_dir = 'blob_cache'

    def checkStoreBlob(self):
        import transaction
        from ZODB.blob import Blob
        from ZODB.tests.StorageTestBase import ZERO
        from ZODB.tests.StorageTestBase import zodb_pickle

        somedata = b'a' * 10

        blob = Blob()
        with blob.open('w') as bd_fh:
            bd_fh.write(somedata)
        tfname = bd_fh.name
        oid = self._storage.new_oid()
        data = zodb_pickle(blob)
        self.assert_(os.path.exists(tfname))

        t = TransactionMetaData()
        try:
            self._storage.tpc_begin(t)
            self._storage.storeBlob(oid, ZERO, data, tfname, '', t)
            self._storage.tpc_vote(t)
            revid = self._storage.tpc_finish(t)
        except:
            self._storage.tpc_abort(t)
            raise
        self.assert_(not os.path.exists(tfname))
        filename = self._storage.fshelper.getBlobFilename(oid, revid)
        self.assert_(os.path.exists(filename))
        with open(filename, 'rb') as f:
            self.assertEqual(somedata, f.read())

    def checkStoreBlob_wrong_partition(self):
        os_rename = os.rename
        try:
            def fail(*a):
                raise OSError
            os.rename = fail
            self.checkStoreBlob()
        finally:
            os.rename = os_rename

    def checkLoadBlob(self):
        from ZODB.blob import Blob
        from ZODB.tests.StorageTestBase import zodb_pickle, ZERO
        import transaction

        somedata = b'a' * 10

        blob = Blob()
        with blob.open('w') as bd_fh:
            bd_fh.write(somedata)
        tfname = bd_fh.name
        oid = self._storage.new_oid()
        data = zodb_pickle(blob)

        t = TransactionMetaData()
        try:
            self._storage.tpc_begin(t)
            self._storage.storeBlob(oid, ZERO, data, tfname, '', t)
            self._storage.tpc_vote(t)
            serial = self._storage.tpc_finish(t)
        except:
            self._storage.tpc_abort(t)
            raise

        filename = self._storage.loadBlob(oid, serial)
        with open(filename, 'rb') as f:
            self.assertEqual(somedata, f.read())
        self.assert_(not(os.stat(filename).st_mode & stat.S_IWRITE))
        self.assert_((os.stat(filename).st_mode & stat.S_IREAD))

    def checkTemporaryDirectory(self):
        self.assertEquals(os.path.join(self.blob_cache_dir, 'tmp'),
                          self._storage.temporaryDirectory())

    def checkTransactionBufferCleanup(self):
        oid = self._storage.new_oid()
        with open('blob_file', 'wb') as f:
            f.write(b'I am a happy blob.')
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.storeBlob(
          oid, ZODB.utils.z64, 'foo', 'blob_file', '', t)
        self._storage.close()


class BlobAdaptedFileStorageTests(FullGenericTests, CommonBlobTests):
    """ZEO backed by a BlobStorage-adapted FileStorage."""

    def checkStoreAndLoadBlob(self):
        import transaction
        from ZODB.blob import Blob
        from ZODB.tests.StorageTestBase import ZERO
        from ZODB.tests.StorageTestBase import zodb_pickle

        somedata_path = os.path.join(self.blob_cache_dir, 'somedata')
        with open(somedata_path, 'w+b') as somedata:
            for i in range(1000000):
                somedata.write(("%s\n" % i).encode('ascii'))

            def check_data(path):
                self.assert_(os.path.exists(path))
                f = open(path, 'rb')
                somedata.seek(0)
                d1 = d2 = 1
                while d1 or d2:
                    d1 = f.read(8096)
                    d2 = somedata.read(8096)
                    self.assertEqual(d1, d2)
            somedata.seek(0)

            blob = Blob()
            with blob.open('w') as bd_fh:
                ZODB.utils.cp(somedata, bd_fh)
                bd_fh.close()
                tfname = bd_fh.name
            oid = self._storage.new_oid()
            data = zodb_pickle(blob)
            self.assert_(os.path.exists(tfname))

            t = TransactionMetaData()
            try:
                self._storage.tpc_begin(t)
                self._storage.storeBlob(oid, ZERO, data, tfname, '', t)
                self._storage.tpc_vote(t)
                revid = self._storage.tpc_finish(t)
            except:
                self._storage.tpc_abort(t)
                raise

            # The uncommitted data file should have been removed
            self.assert_(not os.path.exists(tfname))

            # The file should be in the cache ...
            filename = self._storage.fshelper.getBlobFilename(oid, revid)
            check_data(filename)

            # ... and on the server
            server_filename = os.path.join(
                self.blobdir,
                ZODB.blob.BushyLayout().getBlobFilePath(oid, revid),
                )

            self.assert_(server_filename.startswith(self.blobdir))
            check_data(server_filename)

            # If we remove it from the cache and call loadBlob, it should
            # come back. We can do this in many threads.

            ZODB.blob.remove_committed(filename)
            returns = []
            threads = [
                threading.Thread(
                target=lambda :
                        returns.append(self._storage.loadBlob(oid, revid))
                )
                for i in range(10)
                ]
            [thread.start() for thread in threads]
            [thread.join() for thread in threads]
            [self.assertEqual(r, filename) for r in returns]
            check_data(filename)


class BlobWritableCacheTests(FullGenericTests, CommonBlobTests):

    blob_cache_dir = 'blobs'
    shared_blob_dir = True

class FauxConn:
    addr = 'x'
    protocol_version = ZEO.asyncio.server.best_protocol_version
    peer_protocol_version = protocol_version

    serials = []
    def async(self, method, *args):
        if method == 'serialnos':
            self.serials.extend(args[0])

    call_soon_threadsafe = async_threadsafe = async

class StorageServerWrapper:

    def __init__(self, server, storage_id):
        self.storage_id = storage_id
        self.server = ZEO.StorageServer.ZEOStorage(server, server.read_only)
        self.server.notify_connected(FauxConn())
        self.server.register(storage_id, False)

    def sortKey(self):
        return self.storage_id

    def __getattr__(self, name):
        return getattr(self.server, name)

    def registerDB(self, *args):
        pass

    def supportsUndo(self):
        return False

    def new_oid(self):
        return self.server.new_oids(1)[0]

    def tpc_begin(self, transaction):
        self.server.tpc_begin(id(transaction), '', '', {}, None, ' ')

    def tpc_vote(self, transaction):
        result = self.server.vote(id(transaction))
        assert result == self.server.connection.serials[:]
        del self.server.connection.serials[:]
        return result

    def store(self, oid, serial, data, version_ignored, transaction):
        self.server.storea(oid, serial, data, id(transaction))

    def send_reply(self, _, result):        # Masquerade as conn
        self._result = result

    def tpc_abort(self, transaction):
        self.server.tpc_abort(id(transaction))

    def tpc_finish(self, transaction, func = lambda: None):
        self.server.tpc_finish(id(transaction)).set_sender(0, self)
        return self._result

def multiple_storages_invalidation_queue_is_not_insane():
    """
    >>> from ZEO.StorageServer import StorageServer, ZEOStorage
    >>> from ZODB.FileStorage import FileStorage
    >>> from ZODB.DB import DB
    >>> from persistent.mapping import PersistentMapping
    >>> from transaction import commit
    >>> fs1 = FileStorage('t1.fs')
    >>> fs2 = FileStorage('t2.fs')
    >>> server = StorageServer(None, storages=dict(fs1=fs1, fs2=fs2))

    >>> s1 = StorageServerWrapper(server, 'fs1')
    >>> s2 = StorageServerWrapper(server, 'fs2')

    >>> db1 = DB(s1); conn1 = db1.open()
    >>> db2 = DB(s2); conn2 = db2.open()

    >>> commit()
    >>> o1 = conn1.root()
    >>> for i in range(10):
    ...     o1.x = PersistentMapping(); o1 = o1.x
    ...     commit()

    >>> last = fs1.lastTransaction()
    >>> for i in range(5):
    ...     o1.x = PersistentMapping(); o1 = o1.x
    ...     commit()

    >>> o2 = conn2.root()
    >>> for i in range(20):
    ...     o2.x = PersistentMapping(); o2 = o2.x
    ...     commit()

    >>> trans, oids = s1.getInvalidations(last)
    >>> from ZODB.utils import u64
    >>> sorted([int(u64(oid)) for oid in oids])
    [10, 11, 12, 13, 14]

    >>> fs1.close(); fs2.close()
    """

def getInvalidationsAfterServerRestart():
    """

Clients were often forced to verify their caches after a server
restart even if there weren't many transactions between the server
restart and the client connect.

Let's create a file storage and stuff some data into it:

    >>> from ZEO.StorageServer import StorageServer, ZEOStorage
    >>> from ZODB.FileStorage import FileStorage
    >>> from ZODB.DB import DB
    >>> from persistent.mapping import PersistentMapping
    >>> fs = FileStorage('t.fs')
    >>> db = DB(fs)
    >>> conn = db.open()
    >>> from transaction import commit
    >>> last = []
    >>> for i in range(100):
    ...     conn.root()[i] = PersistentMapping()
    ...     commit()
    ...     last.append(fs.lastTransaction())
    >>> db.close()

Now we'll open a storage server on the data, simulating a restart:

    >>> fs = FileStorage('t.fs')
    >>> sv = StorageServer(None, dict(fs=fs))
    >>> s = ZEOStorage(sv, sv.read_only)
    >>> s.notify_connected(FauxConn())
    >>> s.register('fs', False) == fs.lastTransaction()
    True

If we ask for the last transaction, we should get the last transaction
we saved:

    >>> s.lastTransaction() == last[-1]
    True

If a storage implements the method lastInvalidations, as FileStorage
does, then the storage server will populate its invalidation data
structure using lastTransactions.


    >>> tid, oids = s.getInvalidations(last[-10])
    >>> tid == last[-1]
    True


    >>> from ZODB.utils import u64
    >>> sorted([int(u64(oid)) for oid in oids])
    [0, 92, 93, 94, 95, 96, 97, 98, 99, 100]

(Note that the fact that we get oids for 92-100 is actually an
artifact of the fact that the FileStorage lastInvalidations method
returns all OIDs written by transactions, even if the OIDs were
created and not modified. FileStorages don't record whether objects
were created rather than modified. Objects that are just created don't
need to be invalidated.  This means we'll invalidate objects that
dont' need to be invalidated, however, that's better than verifying
caches.)

    >>> fs.close()

If a storage doesn't implement lastInvalidations, a client can still
avoid verifying its cache if it was up to date when the server
restarted.  To illustrate this, we'll create a subclass of FileStorage
without this method:

    >>> class FS(FileStorage):
    ...     lastInvalidations = property()

    >>> fs = FS('t.fs')
    >>> sv = StorageServer(None, dict(fs=fs))
    >>> st = StorageServerWrapper(sv, 'fs')
    >>> s = st.server

Now, if we ask for the invalidations since the last committed
transaction, we'll get a result:

    >>> tid, oids = s.getInvalidations(last[-1])
    >>> tid == last[-1]
    True
    >>> oids
    []

    >>> db = DB(st); conn = db.open()
    >>> ob = conn.root()
    >>> for i in range(5):
    ...     ob.x = PersistentMapping(); ob = ob.x
    ...     commit()
    ...     last.append(fs.lastTransaction())

    >>> ntid, oids = s.getInvalidations(tid)
    >>> ntid == last[-1]
    True

    >>> sorted([int(u64(oid)) for oid in oids])
    [0, 101, 102, 103, 104]

    >>> fs.close()
    """

def tpc_finish_error():
    r"""Server errors in tpc_finish weren't handled properly.

    If there are errors applying changes to the client cache, don't
    leave the cache in an inconsistent state.

    >>> addr, admin = start_server()

    >>> client = ZEO.client(addr)
    >>> db = ZODB.DB(client)
    >>> conn = db.open()
    >>> conn.root.x = 1
    >>> t = conn.transaction_manager.get()
    >>> conn.tpc_begin(t)
    >>> conn.commit(t)
    >>> transaction_meta_data = t.data(conn)
    >>> _ = client.tpc_vote(transaction_meta_data)

    Cause some breakage by messing with the clients transaction
    buffer, sadly, using implementation details:

    >>> tbuf = client._check_trans(transaction_meta_data, 'test')
    >>> tbuf.client_resolved = None

    tpc_finish will fail:

    >>> client.tpc_finish(transaction_meta_data) # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    AttributeError: ...

    >>> client.tpc_abort(transaction_meta_data)
    >>> t.abort()

    But we can still load the saved data:

    >>> conn2 = db.open()
    >>> conn2.root.x
    1

    And we can save new data:

    >>> conn2.root.x += 1
    >>> conn2.transaction_manager.commit()

    >>> db.close()

    >>> stop_server(admin)
    """

def test_prefetch(self):
    """The client storage prefetch method pre-fetches from the server

    >>> count = 999

    >>> import ZEO
    >>> addr, stop = start_server()
    >>> conn = ZEO.connection(addr)
    >>> root = conn.root()
    >>> cls = root.__class__
    >>> for i in range(count):
    ...     root[i] = cls()
    >>> conn.transaction_manager.commit()
    >>> oids = [root[i]._p_oid for i in range(count)]
    >>> conn.close()
    >>> conn = ZEO.connection(addr)
    >>> storage = conn.db().storage
    >>> len(storage._cache) <= 1
    True
    >>> storage.prefetch(oids, conn._storage._start)

    The prefetch returns before the cache is filled:

    >>> len(storage._cache) < count
    True

    But it is filled eventually:

    >>> from zope.testing.wait import wait
    >>> wait(lambda : len(storage._cache) > count)

    >>> loads = storage.server_status()['loads']

    Now if we reload the data, it will be satisfied from the cache:

    >>> for oid in oids:
    ...     _ = conn._storage.load(oid)

    >>> storage.server_status()['loads'] == loads
    True

    >>> conn.close()
    """

def client_has_newer_data_than_server():
    """It is bad if a client has newer data than the server.

    >>> db = ZODB.DB('Data.fs')
    >>> db.close()
    >>> r = shutil.copyfile('Data.fs', 'Data.save')
    >>> addr, admin = start_server(keep=1)
    >>> db = ZEO.DB(addr, name='client', max_disconnect_poll=.01)
    >>> wait_connected(db.storage)
    >>> conn = db.open()
    >>> conn.root().x = 1
    >>> transaction.commit()

    OK, we've added some data to the storage and the client cache has
    the new data. Now, we'll stop the server, put back the old data, and
    see what happens. :)

    >>> stop_server(admin)
    >>> r = shutil.copyfile('Data.save', 'Data.fs')

    >>> import zope.testing.loggingsupport
    >>> handler = zope.testing.loggingsupport.InstalledHandler(
    ...     'ZEO', level=logging.ERROR)
    >>> formatter = logging.Formatter('%(name)s %(levelname)s %(message)s')

    >>> _, admin = start_server(addr=addr)

    >>> wait_until('got enough errors', lambda:
    ...    len([x for x in handler.records
    ...         if x.levelname == 'CRITICAL' and
    ...            'Client has seen newer transactions than server!' in x.msg
    ...         ]) >= 2)

    Note that the errors repeat because the client keeps on trying to connect.

    >>> db.close()
    >>> handler.uninstall()
    >>> stop_server(admin)

    """

def history_over_zeo():
    """
    >>> addr, _ = start_server()
    >>> db = ZEO.DB(addr)
    >>> wait_connected(db.storage)
    >>> conn = db.open()
    >>> conn.root().x = 0
    >>> transaction.commit()
    >>> len(db.history(conn.root()._p_oid, 99))
    2

    >>> db.close()
    """

def dont_log_poskeyerrors_on_server():
    """
    >>> addr, admin = start_server(log='server.log')
    >>> cs = ClientStorage(addr)
    >>> cs.load(ZODB.utils.p64(1))
    Traceback (most recent call last):
    ...
    POSKeyError: 0x01

    >>> cs.close()
    >>> stop_server(admin)
    >>> with open('server.log') as f:
    ...     'POSKeyError' in f.read()
    False
    """

def open_convenience():
    """Often, we just want to open a single connection.

    >>> addr, _ = start_server(path='data.fs')
    >>> conn = ZEO.connection(addr)
    >>> conn.root()
    {}

    >>> conn.root()['x'] = 1
    >>> transaction.commit()
    >>> conn.close()

    Let's make sure the database was cloased when we closed the
    connection, and that the data is there.

    >>> db = ZEO.DB(addr)
    >>> conn = db.open()
    >>> conn.root()
    {'x': 1}
    >>> db.close()
    """

def client_asyncore_thread_has_name():
    """
    >>> addr, _ = start_server()
    >>> db = ZEO.DB(addr)
    >>> any(t for t in threading.enumerate()
    ...     if ' zeo client networking thread' in t.getName())
    True
    >>> db.close()
    """

def runzeo_without_configfile():
    """
    >>> with open('runzeo', 'w') as r:
    ...     _ = r.write('''
    ... import sys
    ... sys.path[:] = %r
    ... import ZEO.runzeo
    ... ZEO.runzeo.main(sys.argv[1:])
    ... ''' % sys.path)

    >>> import subprocess, re
    >>> print(re.sub(b'\d\d+|[:]', b'', subprocess.Popen(
    ...     [sys.executable, 'runzeo', '-a:0', '-ft', '--test'],
    ...     stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    ...     ).stdout.read()).decode('ascii'))
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    ------
    --T INFO ZEO.runzeo () opening storage '1' using FileStorage
    ------
    --T INFO ZEO.StorageServer StorageServer created RW with storages 1RWt
    ------
    --T INFO ZEO.asyncio... listening on ...
    ------
    --T INFO ZEO.StorageServer closing storage '1'
    testing exit immediately
    """

def close_client_storage_w_invalidations():
    r"""
Invalidations could cause errors when closing client storages,

    >>> addr, _ = start_server()
    >>> writing = threading.Event()
    >>> def mad_write_thread():
    ...     global writing
    ...     conn = ZEO.connection(addr)
    ...     writing.set()
    ...     while writing.isSet():
    ...         conn.root.x = 1
    ...         transaction.commit()
    ...     conn.close()

    >>> thread = threading.Thread(target=mad_write_thread)
    >>> thread.setDaemon(True)
    >>> thread.start()
    >>> _ = writing.wait()
    >>> time.sleep(.01)
    >>> for i in range(10):
    ...     conn = ZEO.connection(addr)
    ...     _ = conn._storage.load(b'\0'*8)
    ...     conn.close()

    >>> writing.clear()
    >>> thread.join(1)
    """

def convenient_to_pass_port_to_client_and_ZEO_dot_client():
    """Jim hates typing

    >>> addr, _ = start_server()
    >>> client = ZEO.client(addr[1])
    >>> client.__name__ == "('127.0.0.1', %s)" % addr[1]
    True

    >>> client.close()
    """

@forker.skip_if_testing_client_against_zeo4
def test_server_status():
    """
    You can get server status using the server_status method.

    >>> addr, _ = start_server(zeo_conf=dict(transaction_timeout=1))
    >>> db = ZEO.DB(addr)
    >>> pprint.pprint(db.storage.server_status(), width=40)
    {'aborts': 0,
     'active_txns': 0,
     'commits': 1,
     'conflicts': 0,
     'conflicts_resolved': 0,
     'connections': 1,
     'last-transaction': '03ac11b771fa1c00',
     'loads': 1,
     'lock_time': None,
     'start': 'Tue May  4 10:55:20 2010',
     'stores': 1,
     'timeout-thread-is-alive': True,
     'waiting': 0}

    >>> db.close()
    """

@forker.skip_if_testing_client_against_zeo4
def test_ruok():
    """
    You can also get server status using the ruok protocol.

    >>> addr, _ = start_server(zeo_conf=dict(transaction_timeout=1))
    >>> db = ZEO.DB(addr) # force a transaction :)
    >>> import json, socket, struct
    >>> s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    >>> s.connect(addr)
    >>> writer = s.makefile(mode='wb')
    >>> _ = writer.write(struct.pack(">I", 4)+b"ruok")
    >>> writer.close()
    >>> proto = s.recv(struct.unpack(">I", s.recv(4))[0])
    >>> data = json.loads(
    ...     s.recv(struct.unpack(">I", s.recv(4))[0]).decode("ascii"))
    >>> pprint.pprint(data['1'])
    {u'aborts': 0,
     u'active_txns': 0,
     u'commits': 1,
     u'conflicts': 0,
     u'conflicts_resolved': 0,
     u'connections': 1,
     u'last-transaction': u'03ac11cd11372499',
     u'loads': 1,
     u'lock_time': None,
     u'start': u'Sun Jan  4 09:37:03 2015',
     u'stores': 1,
     u'timeout-thread-is-alive': True,
     u'waiting': 0}
    >>> db.close(); s.close()
    """

def client_labels():
    """
When looking at server logs, for servers with lots of clients coming
from the same machine, it can be very difficult to correlate server
log entries with actual clients.  It's possible, sort of, but tedious.

You can make this easier by passing a label to the ClientStorage
constructor.

    >>> addr, _ = start_server(log='server.log')
    >>> db = ZEO.DB(addr, client_label='test-label-1')
    >>> db.close()
    >>> @wait_until
    ... def check_for_test_label_1():
    ...    with open('server.log') as f:
    ...        for line in f:
    ...            if 'test-label-1' in line:
    ...                print(line.split()[1:4])
    ...                return True
    ['INFO', 'ZEO.StorageServer', '(test-label-1']

You can specify the client label via a configuration file as well:

    >>> import ZODB.config
    >>> db = ZODB.config.databaseFromString('''
    ... <zodb>
    ...    <zeoclient>
    ...       server :%s
    ...       client-label test-label-2
    ...    </zeoclient>
    ... </zodb>
    ... ''' % addr[1])
    >>> db.close()
    >>> @wait_until
    ... def check_for_test_label_2():
    ...     for line in open('server.log'):
    ...         if 'test-label-2' in line:
    ...             print(line.split()[1:4])
    ...             return True
    ['INFO', 'ZEO.StorageServer', '(test-label-2']

    """

def invalidate_client_cache_entry_on_server_commit_error():
    """

When the serials returned during commit includes an error, typically a
conflict error, invalidate the cache entry.  This is important when
the cache is messed up.

    >>> addr, _ = start_server()
    >>> conn1 = ZEO.connection(addr)
    >>> conn1.root.x = conn1.root().__class__()
    >>> transaction.commit()
    >>> conn1.root.x
    {}

    >>> cs = ZEO.ClientStorage.ClientStorage(addr, client='cache')
    >>> conn2 = ZODB.connection(cs)
    >>> conn2.root.x
    {}

    >>> conn2.close()
    >>> cs.close()

    >>> conn1.root.x['x'] = 1
    >>> transaction.commit()
    >>> conn1.root.x
    {'x': 1}

Now, let's screw up the cache by making it have a last tid that is later than
the root serial.

    >>> import ZEO.cache
    >>> cache = ZEO.cache.ClientCache('cache-1.zec')
    >>> cache.setLastTid(p64(u64(conn1.root.x._p_serial)+1))
    >>> cache.close()

We'll also update the server so that it's last tid is newer than the cache's:

    >>> conn1.root.y = 1
    >>> transaction.commit()
    >>> conn1.root.y = 2
    >>> transaction.commit()

Now, if we reopen the client storage, we'll get the wrong root:

    >>> cs = ZEO.ClientStorage.ClientStorage(addr, client='cache')
    >>> conn2 = ZODB.connection(cs)
    >>> conn2.root.x
    {}

And, we'll get a conflict error if we try to modify it:

    >>> conn2.root.x['y'] = 1
    >>> transaction.commit() # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    ConflictError: ...

But, if we abort, we'll get up to date data and we'll see the changes.

    >>> transaction.abort()
    >>> conn2.root.x
    {'x': 1}
    >>> conn2.root.x['y'] = 1
    >>> transaction.commit()
    >>> sorted(conn2.root.x.items())
    [('x', 1), ('y', 1)]

    >>> conn2.close()
    >>> cs.close()
    >>> conn1.close()
    """


script_template = """
import sys
sys.path[:] = %(path)r

%(src)s

"""
def generate_script(name, src):
    with open(name, 'w') as f:
        f.write(script_template % dict(
            exe=sys.executable,
            path=sys.path,
            src=src,
        ))

def read(filename):
    with open(filename) as f:
        return f.read()

def runzeo_logrotate_on_sigusr2():
    """
    >>> from ZEO.tests.forker import get_port
    >>> port = get_port()
    >>> with open('c', 'w') as r:
    ...    _ = r.write('''
    ... <zeo>
    ...    address %s
    ... </zeo>
    ... <mappingstorage>
    ... </mappingstorage>
    ... <eventlog>
    ...    <logfile>
    ...       path l
    ...    </logfile>
    ... </eventlog>
    ... ''' % port)
    >>> generate_script('s', '''
    ... import ZEO.runzeo
    ... ZEO.runzeo.main()
    ... ''')
    >>> import subprocess, signal
    >>> p = subprocess.Popen([sys.executable, 's', '-Cc'], close_fds=True)
    >>> wait_until('started',
    ...       lambda : os.path.exists('l') and ('listening on' in read('l'))
    ...     )

    >>> oldlog = read('l')
    >>> os.rename('l', 'o')
    >>> os.kill(p.pid, signal.SIGUSR2)

    >>> s = ClientStorage(port)
    >>> s.close()
    >>> wait_until('See logging', lambda : ('Log files ' in read('l')))
    >>> read('o') == oldlog  # No new data in old log
    True

    # Cleanup:

    >>> os.kill(p.pid, signal.SIGKILL)
    >>> _ = p.wait()
    """

def unix_domain_sockets():
    """Make sure unix domain sockets work

    >>> addr, _ = start_server(port='./sock')

    >>> c = ZEO.connection(addr)
    >>> c.root.x = 1
    >>> transaction.commit()
    >>> c.close()
    """

def gracefully_handle_abort_while_storing_many_blobs():
    r"""

    >>> import logging, sys
    >>> old_level = logging.getLogger().getEffectiveLevel()
    >>> logging.getLogger().setLevel(logging.ERROR)
    >>> handler = logging.StreamHandler(sys.stdout)
    >>> logging.getLogger().addHandler(handler)

    >>> addr, _ = start_server(blob_dir='blobs')
    >>> client = ZEO.client(addr, blob_dir='cblobs')
    >>> c = ZODB.connection(client)
    >>> c.root.x = ZODB.blob.Blob(b'z'*(1<<20))
    >>> c.root.y = ZODB.blob.Blob(b'z'*(1<<2))
    >>> t = c.transaction_manager.get()
    >>> c.tpc_begin(t)
    >>> c.commit(t)

We've called commit, but the blob sends are queued.  We'll call abort
right away, which will delete the temporary blob files.  The queued
iterators will try to open these files.

    >>> c.tpc_abort(t)

Now we'll try to use the connection, mainly to wait for everything to
get processed. Before we fixed this by making tpc_finish a synchronous
call to the server. we'd get some sort of error here.

    >>> _ = client._call('loadBefore', b'\0'*8, maxtid)

    >>> c.close()

    >>> logging.getLogger().removeHandler(handler)
    >>> logging.getLogger().setLevel(old_level)



    """

def ClientDisconnected_errors_are_TransientErrors():
    """
    >>> from ZEO.Exceptions import ClientDisconnected
    >>> from transaction.interfaces import TransientError
    >>> issubclass(ClientDisconnected, TransientError)
    True
    """

if not os.environ.get('ZEO4_SERVER'):
    if os.environ.get('ZEO_MSGPACK'):
        def test_runzeo_msgpack_support():
            """
            >>> import ZEO

            >>> a, s = ZEO.server(threaded=False)
            >>> conn = ZEO.connection(a)
            >>> str(conn.db().storage.protocol_version.decode('ascii'))
            'M5'
            >>> conn.close(); s()
            """
    else:
        def test_runzeo_msgpack_support():
            """
            >>> import ZEO

            >>> a, s = ZEO.server(threaded=False)
            >>> conn = ZEO.connection(a)
            >>> str(conn.db().storage.protocol_version.decode('ascii'))
            'Z5'
            >>> conn.close(); s()

            >>> a, s = ZEO.server(zeo_conf=dict(msgpack=True), threaded=False)
            >>> conn = ZEO.connection(a)
            >>> str(conn.db().storage.protocol_version.decode('ascii'))
            'M5'
            >>> conn.close(); s()
            """

if sys.platform.startswith('win'):
    del runzeo_logrotate_on_sigusr2
    del unix_domain_sockets

def work_with_multiprocessing_process(name, addr, q):
    conn = ZEO.connection(addr)
    q.put((name, conn.root.x))
    conn.close()

class MultiprocessingTests(unittest.TestCase):

    layer = ZODB.tests.util.MininalTestLayer('work_with_multiprocessing')

    def test_work_with_multiprocessing(self):
        "Client storage should work with multi-processing."

        # Gaaa, zope.testing.runner.FakeInputContinueGenerator has no close
        if not hasattr(sys.stdin, 'close'):
            sys.stdin.close = lambda : None
        if not hasattr(sys.stdin, 'fileno'):
            sys.stdin.fileno = lambda : -1

        self.globs = {}
        forker.setUp(self)
        addr, adminaddr = self.globs['start_server']()
        conn = ZEO.connection(addr)
        conn.root.x = 1
        transaction.commit()
        q = multiprocessing.Queue()
        processes = [multiprocessing.Process(
            target=work_with_multiprocessing_process,
            args=(i, addr, q))
                        for i in range(3)]
        _ = [p.start() for p in processes]
        self.assertEqual(sorted(q.get(timeout=300) for p in processes),
                            [(0, 1), (1, 1), (2, 1)])

        _ = [p.join(30) for p in processes]
        conn.close()
        zope.testing.setupstack.tearDown(self)

@forker.skip_if_testing_client_against_zeo4
def quick_close_doesnt_kill_server():
    r"""

    Start a server:

    >>> from .testssl import server_config, client_ssl
    >>> addr, _ = start_server(zeo_conf=server_config)

    Now connect and immediately disconnect. This caused the server to
    die in the past:

    >>> import socket, struct
    >>> for i in range(5):
    ...     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ...     s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
    ...                  struct.pack('ii', 1, 0))
    ...     s.connect(addr)
    ...     s.close()


    >>> print("\n\nXXX WARNING: running quick_close_doesnt_kill_server with ssl as hack pending http://bugs.python.org/issue27386\n", file=sys.stderr) # Intentional long line to be annoying till this is fixed

    Now we should be able to connect as normal:

    >>> db = ZEO.DB(addr, ssl=client_ssl())
    >>> db.storage.is_connected()
    True

    >>> db.close()
    """

def can_use_empty_string_for_local_host_on_client():
    """We should be able to spell localhost with ''.

    >>> (_, port), _ = start_server()
    >>> conn = ZEO.connection(('', port))
    >>> conn.root()
    {}
    >>> conn.root.x = 1
    >>> transaction.commit()

    >>> conn.close()
    """

slow_test_classes = [
    BlobAdaptedFileStorageTests, BlobWritableCacheTests,
    MappingStorageTests, DemoStorageTests,
    FileStorageTests,
    FileStorageHexTests, FileStorageClientHexTests,
    ]
if not forker.ZEO4_SERVER:
    slow_test_classes.append(FileStorageSSLTests)

quick_test_classes = [FileStorageRecoveryTests, ZRPCConnectionTests]

class ServerManagingClientStorage(ClientStorage):

    def __init__(self, name, blob_dir, shared=False, extrafsoptions=''):
        if shared:
            server_blob_dir = blob_dir
        else:
            server_blob_dir = 'server-'+blob_dir
        self.globs = {}
        addr, stop = forker.start_zeo_server(
            """
            <blobstorage>
                blob-dir %s
                <filestorage>
                   path %s
                   %s
                </filestorage>
            </blobstorage>
            """ % (server_blob_dir, name+'.fs', extrafsoptions),
            )
        zope.testing.setupstack.register(self, stop)
        if shared:
            ClientStorage.__init__(self, addr, blob_dir=blob_dir,
                                   shared_blob_dir=True)
        else:
            ClientStorage.__init__(self, addr, blob_dir=blob_dir)

    def close(self):
        ClientStorage.close(self)
        zope.testing.setupstack.tearDown(self)

def create_storage_shared(name, blob_dir):
    return ServerManagingClientStorage(name, blob_dir, True)

class ServerManagingClientStorageForIExternalGCTest(
    ServerManagingClientStorage):

    def pack(self, t=None, referencesf=None):
        ServerManagingClientStorage.pack(self, t, referencesf, wait=True)
        # Packing doesn't clear old versions out of zeo client caches,
        # so we'll clear the caches.
        self._cache.clear()
        ZEO.ClientStorage._check_blob_cache_size(self.blob_dir, 0)

def test_suite():
    suite = unittest.TestSuite()

    zeo = unittest.TestSuite()
    zeo.addTest(unittest.makeSuite(ZODB.tests.util.AAAA_Test_Runner_Hack))
    patterns = [
        (re.compile(r"u?'start': u?'[^\n]+'"), 'start'),
        (re.compile(r"u?'last-transaction': u?'[0-9a-f]+'"),
         'last-transaction'),
        (re.compile("ZODB.POSException.ConflictError"), "ConflictError"),
        (re.compile("ZODB.POSException.POSKeyError"), "POSKeyError"),
        (re.compile("ZEO.Exceptions.ClientStorageError"), "ClientStorageError"),
        (re.compile(r"\[Errno \d+\]"), '[Errno N]'),
        (re.compile(r"loads=\d+\.\d+"), 'loads=42.42'),
        # Python 3 drops the u prefix
        (re.compile("u('.*?')"), r"\1"),
        (re.compile('u(".*?")'), r"\1")
        ]
    if not PY3:
        patterns.append((re.compile("^'(blob[^']*)'"), r"b'\1'"))
        patterns.append((re.compile("^'Z308'"), "b'Z308'"))
    zeo.addTest(doctest.DocTestSuite(
        setUp=forker.setUp, tearDown=zope.testing.setupstack.tearDown,
        checker=renormalizing.RENormalizing(patterns),
        ))
    zeo.addTest(doctest.DocTestSuite(
            ZEO.tests.IterationTests,
            setUp=forker.setUp, tearDown=zope.testing.setupstack.tearDown,
            checker=renormalizing.RENormalizing((
                    (re.compile("ZEO.Exceptions.ClientDisconnected"),
                     "ClientDisconnected"),
                    )),
            ))
    if not forker.ZEO4_SERVER:
        # ZEO 4 doesn't support client-side conflict resolution
        zeo.addTest(unittest.makeSuite(ClientConflictResolutionTests, 'check'))
    zeo.layer = ZODB.tests.util.MininalTestLayer('testZeo-misc')
    suite.addTest(zeo)

    zeo = unittest.TestSuite()
    zeo.addTest(
        doctest.DocFileSuite(
            'zdoptions.test',
            'drop_cache_rather_than_verify.txt', 'client-config.test',
            'protocols.test', 'zeo_blob_cache.test', 'invalidation-age.txt',
            '../nagios.rst',
            setUp=forker.setUp, tearDown=zope.testing.setupstack.tearDown,
            checker=renormalizing.RENormalizing(patterns),
            globs={'print_function': print_function},
            ),
        )
    zeo.addTest(PackableStorage.IExternalGC_suite(
        lambda :
        ServerManagingClientStorageForIExternalGCTest(
            'data.fs', 'blobs', extrafsoptions='pack-gc false')
        ))
    for klass in quick_test_classes:
        zeo.addTest(unittest.makeSuite(klass, "check"))
    zeo.layer = ZODB.tests.util.MininalTestLayer('testZeo-misc2')
    suite.addTest(zeo)

    # tests that often fail, maybe if they have their own layers
    for name in 'zeo-fan-out.test', 'new_addr.test':
        zeo = unittest.TestSuite()
        zeo.addTest(
            doctest.DocFileSuite(
                name,
                setUp=forker.setUp, tearDown=zope.testing.setupstack.tearDown,
                checker=renormalizing.RENormalizing(patterns),
                globs={'print_function': print_function},
                ),
            )
        zeo.layer = ZODB.tests.util.MininalTestLayer('testZeo-' + name)
        suite.addTest(zeo)

    suite.addTest(unittest.makeSuite(MultiprocessingTests))

    # Put the heavyweights in their own layers
    for klass in slow_test_classes:
        sub = unittest.makeSuite(klass, "check")
        sub.layer = ZODB.tests.util.MininalTestLayer(klass.__name__)
        suite.addTest(sub)

    suite.addTest(ZODB.tests.testblob.storage_reusable_suite(
        'ClientStorageNonSharedBlobs', ServerManagingClientStorage))
    suite.addTest(ZODB.tests.testblob.storage_reusable_suite(
        'ClientStorageSharedBlobs', create_storage_shared))

    if not forker.ZEO4_SERVER:
        from .threaded import threaded_server_tests
        dynamic_server_ports_suite = doctest.DocFileSuite(
            'dynamic_server_ports.test',
            setUp=forker.setUp, tearDown=zope.testing.setupstack.tearDown,
            checker=renormalizing.RENormalizing(patterns),
            globs={'print_function': print_function},
            )
        dynamic_server_ports_suite.layer = threaded_server_tests
        suite.addTest(dynamic_server_ports_suite)

    return suite


if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
