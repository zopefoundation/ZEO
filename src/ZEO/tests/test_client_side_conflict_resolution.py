import os
import shutil
import tempfile
import unittest

import zope.testing.setupstack
from BTrees.Length import Length
from ZODB import serialize
from ZODB.broken import find_global
from ZODB.DemoStorage import DemoStorage
from ZODB.utils import maxtid
from ZODB.utils import z64

import ZEO

from .utils import StorageServer


class Var:
    def __eq__(self, other):
        self.value = other
        return True


class ClientSideConflictResolutionTests(zope.testing.setupstack.TestCase):

    def test_server_side(self):
        # First, verify default conflict resolution.
        server = StorageServer(self, DemoStorage())
        zs = server.zs

        reader = serialize.ObjectReader(
            factory=lambda conn, *args: find_global(*args))
        writer = serialize.ObjectWriter()
        ob = Length(0)
        ob._p_oid = z64

        # 2 non-conflicting transactions:

        zs.tpc_begin(1, '', '', {})
        zs.storea(ob._p_oid, z64, writer.serialize(ob), 1)
        self.assertEqual(zs.vote(1), [])
        tid1 = server.unpack_result(zs.tpc_finish(1))
        server.assert_calls(self, ('info', {'length': 1, 'size': Var()}))

        ob.change(1)
        zs.tpc_begin(2, '', '', {})
        zs.storea(ob._p_oid, tid1, writer.serialize(ob), 2)
        self.assertEqual(zs.vote(2), [])
        tid2 = server.unpack_result(zs.tpc_finish(2))
        server.assert_calls(self, ('info', {'size':  Var(), 'length': 1}))

        # Now, a cnflicting one:
        zs.tpc_begin(3, '', '', {})
        zs.storea(ob._p_oid, tid1, writer.serialize(ob), 3)

        # Vote returns the object id, indicating that a conflict was resolved.
        self.assertEqual(zs.vote(3), [ob._p_oid])
        tid3 = server.unpack_result(zs.tpc_finish(3))

        p, serial, next_serial = zs.loadBefore(ob._p_oid, maxtid)
        self.assertEqual((serial, next_serial), (tid3, None))
        self.assertEqual(reader.getClassName(p), 'BTrees.Length.Length')
        self.assertEqual(reader.getState(p), 2)

        # Now, we'll create a server that expects the client to
        # resolve conflicts:

        server = StorageServer(
            self, DemoStorage(), client_conflict_resolution=True)
        zs = server.zs

        # 2 non-conflicting transactions:

        zs.tpc_begin(1, '', '', {})
        zs.storea(ob._p_oid, z64, writer.serialize(ob), 1)
        self.assertEqual(zs.vote(1), [])
        tid1 = server.unpack_result(zs.tpc_finish(1))
        server.assert_calls(self, ('info', {'size': Var(), 'length': 1}))

        ob.change(1)
        zs.tpc_begin(2, '', '', {})
        zs.storea(ob._p_oid, tid1, writer.serialize(ob), 2)
        self.assertEqual(zs.vote(2), [])
        tid2 = server.unpack_result(zs.tpc_finish(2))
        server.assert_calls(self, ('info', {'length': 1, 'size': Var()}))

        # Now, a conflicting one:
        zs.tpc_begin(3, '', '', {})
        zs.storea(ob._p_oid, tid1, writer.serialize(ob), 3)

        # Vote returns an object, indicating that a conflict was not resolved.
        self.assertEqual(
            zs.vote(3),
            [dict(oid=ob._p_oid,
                  serials=(tid2, tid1),
                  data=writer.serialize(ob),
                  )],
            )

        # Now, it's up to the client to resolve the conflict. It can
        # do this by making another store call. In this call, we use
        # tid2 as the starting tid:
        ob.change(1)
        zs.storea(ob._p_oid, tid2, writer.serialize(ob), 3)
        self.assertEqual(zs.vote(3), [])
        tid3 = server.unpack_result(zs.tpc_finish(3))
        server.assert_calls(self, ('info', {'size': Var(), 'length': 1}))

        p, serial, next_serial = zs.loadBefore(ob._p_oid, maxtid)
        self.assertEqual((serial, next_serial), (tid3, None))
        self.assertEqual(reader.getClassName(p), 'BTrees.Length.Length')
        self.assertEqual(reader.getState(p), 3)

    def test_client_side(self):
        # First, traditional:
        path = tempfile.mkdtemp(prefix='zeo-test-')
        self.addCleanup(shutil.rmtree, path)
        addr, stop = ZEO.server(os.path.join(path, 'data.fs'), threaded=False)
        db = ZEO.DB(addr, wait_timeout=2)
        with db.transaction() as conn:
            conn.root.len = Length(0)
        conn2 = db.open()
        conn2.root.len.change(1)
        with db.transaction() as conn:
            conn.root.len.change(1)

        conn2.transaction_manager.commit()

        self.assertEqual(conn2.root.len.value, 2)

        db.close()
        stop()

        # Now, do conflict resolution on the client.
        addr2, stop = ZEO.server(
            storage_conf='<mappingstorage>\n</mappingstorage>\n',
            zeo_conf=dict(client_conflict_resolution=True),
            threaded=False,
            )

        db = ZEO.DB(addr2)
        with db.transaction() as conn:
            conn.root.len = Length(0)
        conn2 = db.open()
        conn2.root.len.change(1)
        with db.transaction() as conn:
            conn.root.len.change(1)

        self.assertEqual(conn2.root.len.value, 1)
        conn2.transaction_manager.commit()

        self.assertEqual(conn2.root.len.value, 2)

        db.close()
        stop()
