##############################################################################
#
# Copyright Zope Foundation and Contributors.
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
import logging
import pprint
import re
import sys
import unittest

import transaction
import ZODB.blob
import ZODB.FileStorage
import ZODB.tests.util
import ZODB.utils
from zope.testing import renormalizing
from zope.testing import setupstack

import ZEO.StorageServer
import ZEO.tests.servertesting


def proper_handling_of_blob_conflicts():
    r"""

Conflict errors weren't properly handled when storing blobs, the
result being that the storage was left in a transaction.

We originally saw this when restarting a blob transaction, although
it doesn't really matter.

Set up the storage with some initial blob data.

    >>> fs = ZODB.FileStorage.FileStorage('t.fs', blob_dir='t.blobs')
    >>> db = ZODB.DB(fs)
    >>> conn = db.open()
    >>> conn.root.b = ZODB.blob.Blob(b'x')
    >>> transaction.commit()

Get the oid and first serial. We'll use the serial later to provide
out-of-date data.

    >>> oid = conn.root.b._p_oid
    >>> serial = conn.root.b._p_serial
    >>> with conn.root.b.open('w') as file:
    ...     _ = file.write(b'y')
    >>> transaction.commit()
    >>> data = fs.load(oid)[0]

Create the server:

    >>> server = ZEO.tests.servertesting.StorageServer('x', {'1': fs})

And an initial client.

    >>> zs1 = ZEO.tests.servertesting.client(server, 1)
    >>> zs1.tpc_begin('0', '', '', {})
    >>> zs1.storea(ZODB.utils.p64(99), ZODB.utils.z64, b'x', '0')
    >>> _ = zs1.vote('0') # doctest: +ELLIPSIS

In a second client, we'll try to commit using the old serial. This
will conflict. It will be blocked at the vote call.

    >>> zs2 = ZEO.tests.servertesting.client(server, 2)
    >>> zs2.tpc_begin('1', '', '', {})
    >>> zs2.storeBlobStart()
    >>> zs2.storeBlobChunk(b'z')
    >>> zs2.storeBlobEnd(oid, serial, data, '1')
    >>> delay = zs2.vote('1')

    >>> class Sender:
    ...     def send_reply(self, id, reply):
    ...         print('reply', id, reply)
    ...     def send_error(self, id, err):
    ...         print('error', id, err)
    >>> delay.set_sender(1, Sender())

    >>> logger = logging.getLogger('ZEO')
    >>> handler = logging.StreamHandler(sys.stdout)
    >>> logger.setLevel(logging.INFO)
    >>> logger.addHandler(handler)

Now, when we abort the transaction for the first client. The second
client will be restarted.  It will get a conflict error, that is
raised to the client:

    >>> zs1.tpc_abort('0') # doctest: +ELLIPSIS
    error 1 database conflict error ...

The transaction is aborted by the server:

    >>> fs.tpc_transaction() is None
    True

    >>> zs2.connected
    True

    >>> logger.setLevel(logging.NOTSET)
    >>> logger.removeHandler(handler)
    >>> zs2.tpc_abort('1')
    >>> fs.close()

    >>> server.close()
    """


def proper_handling_of_errors_in_restart():
    r"""

It's critical that if there is an error in vote that the
storage isn't left in tpc.

    >>> fs = ZODB.FileStorage.FileStorage('t.fs', blob_dir='t.blobs')
    >>> server = ZEO.tests.servertesting.StorageServer('x', {'1': fs})

And an initial client.

    >>> zs1 = ZEO.tests.servertesting.client(server, 1)
    >>> zs1.tpc_begin('0', '', '', {})
    >>> zs1.storea(ZODB.utils.p64(99), ZODB.utils.z64, b'x', '0')

Intentionally break zs1:

    >>> zs1._store = lambda : None
    >>> _ = zs1.vote('0') # doctest: +ELLIPSIS +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    TypeError: <lambda>() takes no arguments (3 given)

We're not in a transaction:

    >>> fs.tpc_transaction() is None
    True

We can start another client and get the storage lock.

    >>> zs1 = ZEO.tests.servertesting.client(server, 1)
    >>> zs1.tpc_begin('1', '', '', {})
    >>> zs1.storea(ZODB.utils.p64(99), ZODB.utils.z64, b'x', '1')
    >>> _ = zs1.vote('1') # doctest: +ELLIPSIS

    >>> zs1.tpc_finish('1').set_sender(0, zs1.connection)

    >>> fs.close()
    >>> server.close()
    """


def errors_in_vote_should_clear_lock():
    """

So, we arrange to get an error in vote:

    >>> import ZODB.MappingStorage
    >>> vote_should_fail = True
    >>> class MappingStorage(ZODB.MappingStorage.MappingStorage):
    ...     def tpc_vote(*args):
    ...         if vote_should_fail:
    ...             raise ValueError
    ...         return ZODB.MappingStorage.MappingStorage.tpc_vote(*args)

    >>> server = ZEO.tests.servertesting.StorageServer(
    ...      'x', {'1': MappingStorage()})
    >>> zs = ZEO.tests.servertesting.client(server, 1)
    >>> zs.tpc_begin('0', '', '', {})
    >>> zs.storea(ZODB.utils.p64(99), ZODB.utils.z64, 'x', '0')

    >>> zs.vote('0')
    Traceback (most recent call last):
    ...
    ValueError

When we do, the storage server's transaction lock shouldn't be held:

    >>> zs.lock_manager.locked is not None
    False

Of course, if vote suceeds, the lock will be held:

    >>> vote_should_fail = False
    >>> zs.tpc_begin('1', '', '', {})
    >>> zs.storea(ZODB.utils.p64(99), ZODB.utils.z64, 'x', '1')
    >>> _ = zs.vote('1') # doctest: +ELLIPSIS

    >>> zs.lock_manager.locked is not None
    True

    >>> zs.tpc_abort('1')

    >>> server.close()
    """


def some_basic_locking_tests():
    r"""

    >>> itid = 0
    >>> def start_trans(zs):
    ...     global itid
    ...     itid += 1
    ...     tid = str(itid)
    ...     zs.tpc_begin(tid, '', '', {})
    ...     zs.storea(ZODB.utils.p64(99), ZODB.utils.z64, 'x', tid)
    ...     return tid

    >>> server = ZEO.tests.servertesting.StorageServer()

    >>> handler = logging.StreamHandler(sys.stdout)
    >>> handler.setFormatter(logging.Formatter(
    ...     '%(name)s %(levelname)s\n%(message)s'))
    >>> logging.getLogger('ZEO').addHandler(handler)
    >>> logging.getLogger('ZEO').setLevel(logging.DEBUG)

Work around the fact that ZODB registers level names backwards:

    >>> import logging
    >>> from ZODB.loglevels import BLATHER
    >>> logging.addLevelName(BLATHER, "BLATHER")

We start a transaction and vote, this leads to getting the lock.

    >>> zs1 = ZEO.tests.servertesting.client(server, '1')
    ZEO.asyncio.base INFO
    Connected ZEO.asyncio.server.ServerProtocol('test-addr-1')
    ZEO.asyncio.server INFO
    received handshake 'Z5'
    >>> tid1 = start_trans(zs1)
    >>> resolved1 = zs1.vote(tid1) # doctest: +ELLIPSIS
    ZEO.StorageServer DEBUG
    (test-addr-1) ('1') lock: transactions waiting: 0
    ZEO.StorageServer BLATHER
    (test-addr-1) Preparing to commit transaction: 1 objects, ... bytes

If another client tried to vote, it's lock request will be queued and
a delay will be returned:

    >>> zs2 = ZEO.tests.servertesting.client(server, '2')
    ZEO.asyncio.base INFO
    Connected ZEO.asyncio.server.ServerProtocol('test-addr-2')
    ZEO.asyncio.server INFO
    received handshake 'Z5'
    >>> tid2 = start_trans(zs2)
    >>> delay = zs2.vote(tid2)
    ZEO.StorageServer DEBUG
    (test-addr-2) ('1') queue lock: transactions waiting: 1

    >>> delay.set_sender(0, zs2.connection)

When we end the first transaction, the queued vote gets the lock.

    >>> zs1.tpc_abort(tid1) # doctest: +ELLIPSIS
    ZEO.StorageServer DEBUG
    (test-addr-1) ('1') unlock: transactions waiting: 1
    ZEO.StorageServer DEBUG
    (test-addr-2) ('1') lock: transactions waiting: 0
    ZEO.StorageServer BLATHER
    (test-addr-2) Preparing to commit transaction: 1 objects, ... bytes

Let's try again with the first client. The vote will be queued:

    >>> tid1 = start_trans(zs1)
    >>> delay = zs1.vote(tid1)
    ZEO.StorageServer DEBUG
    (test-addr-1) ('1') queue lock: transactions waiting: 1

If the queued transaction is aborted, it will be dequeued:

    >>> zs1.tpc_abort(tid1) # doctest: +ELLIPSIS
    ZEO.StorageServer DEBUG
    (test-addr-1) ('1') dequeue lock: transactions waiting: 0

BTW, voting multiple times will error:

    >>> zs2.vote(tid2)
    Traceback (most recent call last):
    ...
    StorageTransactionError: Already voting (locked)

    >>> tid1 = start_trans(zs1)
    >>> delay = zs1.vote(tid1)
    ZEO.StorageServer DEBUG
    (test-addr-1) ('1') queue lock: transactions waiting: 1

    >>> delay.set_sender(0, zs1.connection)

    >>> zs1.vote(tid1)
    Traceback (most recent call last):
    ...
    StorageTransactionError: Already voting (waiting)

Note that the locking activity is logged at debug level to avoid
cluttering log files, however, as the number of waiting votes
increased, so does the logging level:

    >>> clients = []
    >>> for i in range(9):
    ...     client = ZEO.tests.servertesting.client(server, str(i+10))
    ...     tid = start_trans(client)
    ...     delay = client.vote(tid)
    ...     clients.append(client)
    ZEO.asyncio.base INFO
    Connected ZEO.asyncio.server.ServerProtocol('test-addr-10')
    ZEO.asyncio.server INFO
    received handshake 'Z5'
    ZEO.StorageServer DEBUG
    (test-addr-10) ('1') queue lock: transactions waiting: 2
    ZEO.asyncio.base INFO
    Connected ZEO.asyncio.server.ServerProtocol('test-addr-11')
    ZEO.asyncio.server INFO
    received handshake 'Z5'
    ZEO.StorageServer DEBUG
    (test-addr-11) ('1') queue lock: transactions waiting: 3
    ZEO.asyncio.base INFO
    Connected ZEO.asyncio.server.ServerProtocol('test-addr-12')
    ZEO.asyncio.server INFO
    received handshake 'Z5'
    ZEO.StorageServer WARNING
    (test-addr-12) ('1') queue lock: transactions waiting: 4
    ZEO.asyncio.base INFO
    Connected ZEO.asyncio.server.ServerProtocol('test-addr-13')
    ZEO.asyncio.server INFO
    received handshake 'Z5'
    ZEO.StorageServer WARNING
    (test-addr-13) ('1') queue lock: transactions waiting: 5
    ZEO.asyncio.base INFO
    Connected ZEO.asyncio.server.ServerProtocol('test-addr-14')
    ZEO.asyncio.server INFO
    received handshake 'Z5'
    ZEO.StorageServer WARNING
    (test-addr-14) ('1') queue lock: transactions waiting: 6
    ZEO.asyncio.base INFO
    Connected ZEO.asyncio.server.ServerProtocol('test-addr-15')
    ZEO.asyncio.server INFO
    received handshake 'Z5'
    ZEO.StorageServer WARNING
    (test-addr-15) ('1') queue lock: transactions waiting: 7
    ZEO.asyncio.base INFO
    Connected ZEO.asyncio.server.ServerProtocol('test-addr-16')
    ZEO.asyncio.server INFO
    received handshake 'Z5'
    ZEO.StorageServer WARNING
    (test-addr-16) ('1') queue lock: transactions waiting: 8
    ZEO.asyncio.base INFO
    Connected ZEO.asyncio.server.ServerProtocol('test-addr-17')
    ZEO.asyncio.server INFO
    received handshake 'Z5'
    ZEO.StorageServer WARNING
    (test-addr-17) ('1') queue lock: transactions waiting: 9
    ZEO.asyncio.base INFO
    Connected ZEO.asyncio.server.ServerProtocol('test-addr-18')
    ZEO.asyncio.server INFO
    received handshake 'Z5'
    ZEO.StorageServer CRITICAL
    (test-addr-18) ('1') queue lock: transactions waiting: 10

If a client with the transaction lock disconnects, it will abort and
release the lock and one of the waiting clients will get the lock.

    >>> zs2.notify_disconnected() # doctest: +ELLIPSIS
    ZEO.StorageServer INFO
    (test-addr-...) disconnected during locked transaction
    ZEO.StorageServer CRITICAL
    (test-addr-...) ('1') unlock: transactions waiting: 10
    ZEO.StorageServer WARNING
    (test-addr-...) ('1') lock: transactions waiting: 9
    ZEO.StorageServer BLATHER
    (test-addr-...) Preparing to commit transaction: 1 objects, ... bytes

(In practice, waiting clients won't necessarily get the lock in order.)

We can find out about the current lock state, and get other server
statistics using the server_status method:

    >>> pprint.pprint(zs1.server_status(), width=40)
    {'aborts': 3,
     'active_txns': 10,
     'commits': 0,
     'conflicts': 0,
     'conflicts_resolved': 0,
     'connections': 10,
     'last-transaction': '0000000000000000',
     'loads': 0,
     'lock_time': 1272653598.693882,
     'start': 'Fri Apr 30 14:53:18 2010',
     'stores': 13,
     'timeout-thread-is-alive': 'stub',
     'waiting': 9}

If clients disconnect while waiting, they will be dequeued:

    >>> for client in clients:
    ...     client.notify_disconnected() # doctest: +ELLIPSIS
    ZEO.StorageServer INFO
    (test-addr-10) disconnected during...locked transaction
    ZEO.StorageServer WARNING
    (test-addr-10) ('1') ... lock: transactions waiting: ...

    >>> zs1.server_status()['waiting']
    0

    >>> zs1.tpc_abort(tid1)
    ZEO.StorageServer DEBUG
    (test-addr-1) ('1') unlock: transactions waiting: 0

    >>> logging.getLogger('ZEO').setLevel(logging.NOTSET)
    >>> logging.getLogger('ZEO').removeHandler(handler)
    >>> server.close()
    """


def lock_sanity_check():
    r"""
On one occasion with 3.10.0a1 in production, we had a case where a
transaction lock wasn't released properly.  One possibility, fron
scant log information, is that the server and ZEOStorage had different
ideas about whether the ZEOStorage was locked. The timeout thread
properly closed the ZEOStorage's connection, but the ZEOStorage didn't
release it's lock, presumably because it thought it wasn't locked. I'm
not sure why this happened.  I've refactored the logic quite a bit to
try to deal with this, but the consequences of this failure are so
severe, I'm adding some sanity checking when queueing lock requests.

Helper to manage transactions:

    >>> itid = 0
    >>> def start_trans(zs):
    ...     global itid
    ...     itid += 1
    ...     tid = str(itid)
    ...     zs.tpc_begin(tid, '', '', {})
    ...     zs.storea(ZODB.utils.p64(99), ZODB.utils.z64, 'x', tid)
    ...     return tid

Set up server and logging:

    >>> server = ZEO.tests.servertesting.StorageServer()

    >>> handler = logging.StreamHandler(sys.stdout)
    >>> handler.setFormatter(logging.Formatter(
    ...     '%(name)s %(levelname)s\n%(message)s'))
    >>> logging.getLogger('ZEO').addHandler(handler)
    >>> logging.getLogger('ZEO').setLevel(logging.DEBUG)

Work around the fact that ZODB registers level names backwards:

    >>> import logging
    >>> from ZODB.loglevels import BLATHER
    >>> logging.addLevelName(BLATHER, "BLATHER")

Now, we'll start a transaction, get the lock and then mark the
ZEOStorage as closed and see if trying to get a lock cleans it up:

    >>> zs1 = ZEO.tests.servertesting.client(server, '1')
    ZEO.asyncio.base INFO
    Connected ZEO.asyncio.server.ServerProtocol('test-addr-1')
    ZEO.asyncio.server INFO
    received handshake 'Z5'
    >>> tid1 = start_trans(zs1)
    >>> resolved1 = zs1.vote(tid1) # doctest: +ELLIPSIS
    ZEO.StorageServer DEBUG
    (test-addr-1) ('1') lock: transactions waiting: 0
    ZEO.StorageServer BLATHER
    (test-addr-1) Preparing to commit transaction: 1 objects, ... bytes

    >>> zs1.connection.connection_lost(None)
    ZEO.StorageServer INFO
    (test-addr-1) disconnected during locked transaction
    ZEO.StorageServer DEBUG
    (test-addr-1) ('1') unlock: transactions waiting: 0

    >>> zs2 = ZEO.tests.servertesting.client(server, '2')
    ZEO.asyncio.base INFO
    Connected ZEO.asyncio.server.ServerProtocol('test-addr-2')
    ZEO.asyncio.server INFO
    received handshake 'Z5'
    >>> tid2 = start_trans(zs2)
    >>> resolved2 = zs2.vote(tid2) # doctest: +ELLIPSIS
    ZEO.StorageServer DEBUG
    (test-addr-2) ('1') lock: transactions waiting: 0
    ZEO.StorageServer BLATHER
    (test-addr-2) Preparing to commit transaction: 1 objects, ... bytes

    >>> zs2.tpc_abort(tid2)
    ZEO.StorageServer DEBUG
    (test-addr-2) ('1') unlock: transactions waiting: 0

    >>> logging.getLogger('ZEO').setLevel(logging.NOTSET)
    >>> logging.getLogger('ZEO').removeHandler(handler)

    >>> server.close()
    """


def test_suite():
    return unittest.TestSuite((
        doctest.DocTestSuite(
            setUp=ZODB.tests.util.setUp, tearDown=setupstack.tearDown,
            checker=renormalizing.RENormalizing([
                (re.compile(r'\d+/test-addr'), ''),
                (re.compile(r"'lock_time': \d+.\d+"), 'lock_time'),
                (re.compile(r"'start': '[^\n]+'"), 'start'),
                (re.compile('ZODB.POSException.StorageTransactionError'),
                 'StorageTransactionError'),
                ]),
            ),
        ))
