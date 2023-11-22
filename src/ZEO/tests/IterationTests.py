##############################################################################
#
# Copyright (c) 2008 Zope Foundation and Contributors.
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
"""ZEO iterator protocol tests."""

import gc

import transaction
from ZODB.Connection import TransactionMetaData

from ..asyncio.testing import AsyncRPC


class IterationTests:

    def _assertIteratorIdsEmpty(self):
        # Account for the need to run a GC collection
        # under non-refcounted implementations like PyPy
        # for storage._iterator_gc to fully do its job.
        # First, confirm that it ran
        self.assertTrue(self._storage._iterators._last_gc > 0)
        gc_enabled = gc.isenabled()
        # make sure there's no race conditions cleaning out the weak refs
        gc.disable()
        try:
            self.assertEqual(0, len(self._storage._iterator_ids))
        except AssertionError:
            # Ok, we have ids. That should also mean that the
            # weak dictionary has the same length.

            self.assertEqual(len(self._storage._iterators),
                             len(self._storage._iterator_ids))
            # Now if we do a collection and re-ask for iterator_gc
            # everything goes away as expected.
            gc.enable()
            gc.collect()
            gc.collect()  # sometimes PyPy needs it twice to clear weak refs

            self._storage._iterator_gc()

            self.assertEqual(len(self._storage._iterators),
                             len(self._storage._iterator_ids))
            self.assertEqual(0, len(self._storage._iterator_ids))
        finally:
            if gc_enabled:
                gc.enable()
            else:
                gc.disable()

    def checkIteratorGCProtocol(self):
        # Test garbage collection on protocol level.
        server = AsyncRPC(self._storage._server)

        iid = server.iterator_start(None, None)
        # None signals the end of iteration.
        self.assertEqual(None, server.iterator_next(iid))
        # The server has disposed the iterator already.
        self.assertRaises(KeyError, server.iterator_next, iid)

        iid = server.iterator_start(None, None)
        # This time, we tell the server to throw the iterator away.
        server.iterator_gc([iid])
        self.assertRaises(KeyError, server.iterator_next, iid)

    def checkIteratorExhaustionStorage(self):
        # Test the storage's garbage collection mechanism.
        self._dostore()
        iterator = self._storage.iterator()

        # At this point, a wrapping iterator might not have called the CS
        # iterator yet. We'll consume one item to make sure this happens.
        next(iterator)
        self.assertEqual(1, len(self._storage._iterator_ids))
        iid = list(self._storage._iterator_ids)[0]
        self.assertEqual([], list(iterator))
        self.assertEqual(0, len(self._storage._iterator_ids))

        # The iterator has run through, so the server has already disposed it.
        self.assertRaises(KeyError, self._storage._call, 'iterator_next', iid)

    def checkIteratorGCSpanTransactions(self):
        # Keep a hard reference to the iterator so it won't be automatically
        # garbage collected at the transaction boundary.
        self._dostore()
        iterator = self._storage.iterator()
        self._dostore()
        # As the iterator was not garbage collected, we can still use it. (We
        # don't see the transaction we just wrote being picked up, because
        # iterators only see the state from the point in time when they were
        # created.)
        self.assertTrue(list(iterator))

    def checkIteratorGCStorageCommitting(self):
        # We want the iterator to be garbage-collected, so we don't keep any
        # hard references to it. The storage tracks its ID, though.

        # The odd little jig we do below arises from the fact that the
        # CS iterator may not be constructed right away if the CS is wrapped.
        # We need to actually do some iteration to get the iterator created.
        # We do a store to make sure the iterator isn't exhausted right away.
        self._dostore()
        next(self._storage.iterator())

        self.assertEqual(1, len(self._storage._iterator_ids))
        iid = list(self._storage._iterator_ids)[0]

        # GC happens at the transaction boundary. After that, both the storage
        # and the server have forgotten the iterator.
        self._storage._iterators._last_gc = -1
        self._dostore()
        self._assertIteratorIdsEmpty()
        self.assertRaises(KeyError, self._storage._call, 'iterator_next', iid)

    def checkIteratorGCStorageTPCAborting(self):
        # Disabling GC to prevent a race condition in PyPy later on:
        gc.disable()
        # The odd little jig we do below arises from the fact that the
        # CS iterator may not be constructed right away if the CS is wrapped.
        # We need to actually do some iteration to get the iterator created.
        # We do a store to make sure the iterator isn't exhausted right away.
        self._dostore()
        next(self._storage.iterator())

        iid = list(self._storage._iterator_ids)[0]

        t = TransactionMetaData()
        self._storage._iterators._last_gc = -1
        self._storage.tpc_begin(t)
        self._storage.tpc_abort(t)
        self._assertIteratorIdsEmpty()
        self.assertRaises(KeyError, self._storage._call, 'iterator_next', iid)
        gc.enable()

    def checkIteratorGCStorageDisconnect(self):

        # The odd little jig we do below arises from the fact that the
        # CS iterator may not be constructed right away if the CS is wrapped.
        # We need to actually do some iteration to get the iterator created.
        # We do a store to make sure the iterator isn't exhausted right away.
        self._dostore()
        next(self._storage.iterator())

        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        # Show that after disconnecting, the client side GCs the iterators
        # as well. I'm calling this directly to avoid accidentally
        # calling tpc_abort implicitly.
        self._storage.notify_disconnected()
        self.assertEqual(0, len(self._storage._iterator_ids))
        # maybe, ``notify_disconnected`` should automatically clean up
        self._storage.tpc_abort(t)  # avoid ``ResourceWarning``

    def checkIteratorParallel(self):
        self._dostore()
        self._dostore()
        iter1 = self._storage.iterator()
        iter2 = self._storage.iterator()
        txn_info1 = next(iter1)
        txn_info2 = next(iter2)
        self.assertEqual(txn_info1.tid, txn_info2.tid)
        txn_info1 = next(iter1)
        txn_info2 = next(iter2)
        self.assertEqual(txn_info1.tid, txn_info2.tid)
        self.assertRaises(StopIteration, next, iter1)
        self.assertRaises(StopIteration, next, iter2)


def iterator_sane_after_reconnect():
    r"""Make sure that iterators are invalidated on disconnect.

Start a server:

    >>> addr, adminaddr = start_server(  # NOQA: F821 undefined
    ...     '<filestorage>\npath fs\n</filestorage>', keep=1)

Open a client storage to it and commit a some transactions:

    >>> import ZEO, ZODB
    >>> client = ZEO.client(addr)
    >>> db = ZODB.DB(client)
    >>> conn = db.open()
    >>> for i in range(10):
    ...     conn.root().i = i
    ...     transaction.commit()

Create an iterator:

    >>> it = client.iterator()
    >>> tid1 = it.next().tid

Restart the storage:

    >>> stop_server(adminaddr)  # NOQA: F821 undefined
    >>> wait_disconnected(client)  # NOQA: F821 undefined
    >>> _ = start_server(  # NOQA: F821 undefined
    ...         '<filestorage>\npath fs\n</filestorage>', addr=addr)
    >>> wait_connected(client)  # NOQA: F821 undefined

Now, we'll create a second iterator:

    >>> it2 = client.iterator()

If we try to advance the first iterator, we should get an error:

    >>> it.next().tid > tid1
    Traceback (most recent call last):
    ...
    ClientDisconnected: Disconnected iterator

The second iterator should be peachy:

    >>> it2.next().tid == tid1
    True

Cleanup:

    >>> db.close()
    """
