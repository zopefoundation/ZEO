##############################################################################
#
# Copyright (c) 2002 Zope Foundation and Contributors.
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
"""Tests of the distributed commit lock."""

import threading
import time

from persistent.TimeStamp import TimeStamp
from ZODB.Connection import TransactionMetaData
from ZODB.tests.StorageTestBase import MinPO
from ZODB.tests.StorageTestBase import zodb_pickle

from ZEO.Exceptions import ClientDisconnected
from ZEO.tests.TestThread import TestThread


ZERO = b'\0'*8


class WorkerThread(TestThread):

    # run the entire test in a thread so that the blocking call for
    # tpc_vote() doesn't hang the test suite.

    def __init__(self, test, storage, trans):
        self.storage = storage
        self.trans = trans
        self.ready = threading.Event()
        TestThread.__init__(self, test)

    def testrun(self):
        try:
            self.storage.tpc_begin(self.trans)
            oid = self.storage.new_oid()
            p = zodb_pickle(MinPO("c"))
            self.storage.store(oid, ZERO, p, '', self.trans)
            oid = self.storage.new_oid()
            p = zodb_pickle(MinPO("c"))
            self.storage.store(oid, ZERO, p, '', self.trans)
            self.myvote()
            self.storage.tpc_finish(self.trans)
        except ClientDisconnected:
            self.storage.tpc_abort(self.trans)
        except Exception:
            self.storage.tpc_abort(self.trans)
            raise

    def myvote(self):
        # The vote() call is synchronous, which makes it difficult to
        # coordinate the action of multiple threads that all call
        # vote().  This method sends the vote call, then sets the
        # event saying vote was called, then waits for the vote
        # response.

        future = self.storage._server.call('vote', id(self.trans), wait=False)
        self.ready.set()
        future.result(9)


class CommitLockTests:

    NUM_CLIENTS = 5

    # The commit lock tests verify that the storage successfully
    # blocks and restarts transactions when there is contention for a
    # single storage.  There are a lot of cases to cover.

    # The general flow of these tests is to start a transaction by
    # getting far enough into 2PC to acquire the commit lock.  Then
    # begin one or more other connections that also want to commit.
    # This causes the commit lock code to be exercised.  Once the
    # other connections are started, the first transaction completes.

    txn = None

    def _cleanup(self):
        if self.txn is not None:
            self._storage.tpc_abort(self.txn)
        for store, trans in self._storages:
            store.tpc_abort(trans)
            store.close()
        self._storages = []

    def _start_txn(self):
        txn = TransactionMetaData()
        self._storage.tpc_begin(txn)
        oid = self._storage.new_oid()
        self._storage.store(oid, ZERO, zodb_pickle(MinPO(1)), '', txn)
        self.txn = txn
        return oid, txn

    def _begin_threads(self):
        # Start a second transaction on a different connection without
        # blocking the test thread.  Returns only after each thread has
        # set it's ready event.
        self._storages = []
        self._threads = []
        self._exc = None

        for i in range(self.NUM_CLIENTS):
            storage = self._new_storage_client()
            txn = TransactionMetaData()
            self._get_timestamp()

            t = WorkerThread(self, storage, txn)
            self._threads.append(t)
            t.start()
            try:
                t.ready.wait(2)  # fail if this takes unreasonably long
            except Exception as exc:
                self._exc = exc
                return

            # Close one of the connections abnormally to test server response
            if i == 0:
                storage.close()
            else:
                self._storages.append((storage, txn))

    def _finish_threads(self):
        for t in self._threads:
            t.cleanup()

    def _get_timestamp(self):
        t = time.time()
        t = TimeStamp(*time.gmtime(t)[:5]+(t % 60,))
        return repr(t)


class CommitLockVoteTests(CommitLockTests):

    def checkCommitLockVoteFinish(self):
        oid, txn = self._start_txn()
        self._storage.tpc_vote(txn)

        self._begin_threads()
        self.assertIsNone(self._exc)

        self._storage.tpc_finish(txn)
        self._storage.load(oid, '')

        self._finish_threads()

        self._dostore()
        self._cleanup()

    def checkCommitLockVoteAbort(self):
        oid, txn = self._start_txn()
        self._storage.tpc_vote(txn)

        self._begin_threads()
        self.assertIsNone(self._exc)

        self._storage.tpc_abort(txn)

        self._finish_threads()

        self._dostore()
        self._cleanup()

    def checkCommitLockVoteClose(self):
        oid, txn = self._start_txn()
        self._storage.tpc_vote(txn)

        self._begin_threads()
        self.assertIsNone(self._exc)

        self._storage.close()

        self._finish_threads()
        self._cleanup()
