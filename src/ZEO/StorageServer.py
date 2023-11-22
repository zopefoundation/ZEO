##############################################################################
#
# Copyright (c) 2001, 2002, 2003 Zope Foundation and Contributors.
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
"""The StorageServer class and the exception that it may raise.

This server acts as a front-end for one or more real storages, like
file storage or Berkeley storage.

TODO:  Need some basic access control-- a declaration of the methods
exported for invocation by the server.
"""
import codecs
import itertools
import logging
import os
import sys
import tempfile
import threading
import time
import warnings

import ZODB.blob
import ZODB.event
import ZODB.serialize
import ZODB.TimeStamp
import zope.interface
from ZODB.Connection import TransactionMetaData
from ZODB.loglevels import BLATHER
from ZODB.POSException import ConflictError
from ZODB.POSException import ReadOnlyError
from ZODB.POSException import StorageError
from ZODB.POSException import StorageTransactionError
from ZODB.POSException import TransactionError
from ZODB.serialize import referencesf
from ZODB.utils import Lock
from ZODB.utils import RLock
from ZODB.utils import p64
from ZODB.utils import u64
from ZODB.utils import z64
from zodbpickle.pickle import Pickler

from ZEO._compat import Unpickler
from ZEO.asyncio.server import Acceptor
from ZEO.asyncio.server import Delay
from ZEO.asyncio.server import MTDelay
from ZEO.asyncio.server import Result
from ZEO.monitor import StorageStats


# multi-threaded acceptor was opt-in option, but later was deprecated and
# removed. Warn users that try to activate multi-threaded server mode to
# explicitly let them know they no longer get it and why.
if os.environ.get("ZEO_MTACCEPTOR"):
    warnings.warn('The mtacceptor module is no longer supported because it '
                  'was subject to data corruption bugs. $ZEO_MTACCEPTOR no '
                  'longer has any effect. Please see '
                  'github.com/zopefoundation/ZEO/issues/209 for details.',
                  DeprecationWarning)

logger = logging.getLogger('ZEO.StorageServer')


def log(message, level=logging.INFO, label='', exc_info=False):
    """Internal helper to log a message."""
    if label:
        message = f'({label}) {message}'
    logger.log(level, message, exc_info=exc_info)


class StorageServerError(StorageError):
    """Error reported when an unpicklable exception is raised."""


registered_methods = {
    'get_info', 'lastTransaction',
     'getInvalidations', 'new_oids', 'pack', 'loadBefore', 'storea',
     'checkCurrentSerialInTransaction', 'restorea', 'storeBlobStart',
     'storeBlobChunk', 'storeBlobEnd', 'storeBlobShared',
     'deleteObject', 'tpc_begin', 'vote', 'tpc_finish', 'tpc_abort',
     'history', 'record_iternext', 'sendBlob', 'getTid', 'loadSerial',
     'new_oid', 'undoa', 'undoLog', 'undoInfo', 'iterator_start',
     'iterator_next', 'iterator_record_start', 'iterator_record_next',
     'iterator_gc', 'server_status', 'set_client_label', 'ping'}


class ZEOStorage:
    """Proxy to underlying storage for a single remote client."""

    connected = connection = stats = storage = storage_id = transaction = None
    blob_tempfile = None
    log_label = 'unconnected'
    locked = False             # Don't have storage lock
    verifying = 0

    def __init__(self, server, read_only=0):
        self.server = server
        self.client_conflict_resolution = server.client_conflict_resolution
        # timeout and stats will be initialized in register()
        self.read_only = read_only
        self._iterators = {}
        self._iterator_ids = itertools.count()
        # Stores the last item that was handed out for a
        # transaction iterator.
        self._txn_iterators_last = {}

    def set_database(self, database):
        self.database = database

    def notify_connected(self, conn):
        self.connection = conn
        self.call_soon_threadsafe = conn.call_soon_threadsafe
        self.connected = True
        assert conn.protocol_version is not None
        self.log_label = _addr_label(conn.addr)
        self.async_ = conn.async_
        self.async_threadsafe = conn.async_threadsafe

    def notify_disconnected(self):
        # When this storage closes, we must ensure that it aborts
        # any pending transaction.
        if self.transaction is not None:
            self.log("disconnected during %s transaction"
                     % (self.locked and 'locked' or 'unlocked'))
            self.tpc_abort(self.transaction.id)
        else:
            self.log("disconnected")

        self.connected = False
        self.server.close_conn(self)

    def __repr__(self):
        tid = self.transaction and repr(self.transaction.id)
        if self.storage:
            stid = (self.tpc_transaction() and
                    repr(self.tpc_transaction().id))
        else:
            stid = None
        name = self.__class__.__name__
        return f'<{name} {id(self):X} trans={tid} s_trans={stid}>'

    def log(self, msg, level=logging.INFO, exc_info=False):
        log(msg, level=level, label=self.log_label, exc_info=exc_info)

    def setup_delegation(self):
        """Delegate several methods to the storage
        """
        # Called from register

        storage = self.storage

        info = self.get_info()

        if not info['supportsUndo']:
            self.undoLog = self.undoInfo = lambda *a, **k: ()

        # XXX deprecated: but ZODB tests use getTid. They shouldn't
        self.getTid = storage.getTid

        self.loadSerial = storage.loadSerial
        record_iternext = getattr(storage, 'record_iternext', None)
        if record_iternext is not None:
            self.record_iternext = record_iternext
        self.lastTransaction = storage.lastTransaction

        try:
            self.tpc_transaction = storage.tpc_transaction
        except AttributeError:
            if hasattr(storage, '_transaction'):
                log("Storage %r doesn't have a tpc_transaction method.\n"
                    "See ZEO.interfaces.IServeable."
                    "Falling back to using _transaction attribute, which\n."
                    "is icky.",
                    logging.ERROR)
                self.tpc_transaction = lambda: storage._transaction
            else:
                raise

        self.connection.methods = registered_methods

    def history(self, tid, size=1):
        # This caters for storages which still accept
        # a version parameter.
        return self.storage.history(tid, size=size)

    def _check_tid(self, tid, exc=None):
        if self.read_only:
            raise ReadOnlyError()
        if self.transaction is None:
            caller = sys._getframe().f_back.f_code.co_name
            # ``tpc_abort`` is allowed to be called with invalid transaction
            # ``vote`` relies on this
            if caller != "tpc_abort":
                self.log("no current transaction: %s()" % caller,
                         level=logging.WARNING)
            if exc is not None:
                raise exc(None, tid)
            else:
                return 0
        if self.transaction.id != tid:
            caller = sys._getframe().f_back.f_code.co_name
            # ``tpc_abort`` is allowed to be called with invalid transaction
            if caller != "tpc_abort":
                self.log("%s(%s) invalid; current transaction = %s" %
                         (caller, repr(tid), repr(self.transaction.id)),
                         logging.WARNING)
            if exc is not None:
                raise exc(self.transaction.id, tid)
            else:
                return 0
        return 1

    def register(self, storage_id, read_only):
        """Select the storage that this client will use

        This method must be the first one called by the client.
        For authenticated storages this method will be called by the client
        immediately after authentication is finished.
        """
        if self.storage is not None:
            self.log("duplicate register() call")
            raise ValueError("duplicate register() call")

        storage = self.server.storages.get(storage_id)
        if storage is None:
            self.log("unknown storage_id: %s" % storage_id)
            raise ValueError("unknown storage: %s" % storage_id)

        if not read_only and (self.read_only or storage.isReadOnly()):
            raise ReadOnlyError()

        self.read_only = self.read_only or read_only
        self.storage_id = storage_id
        self.storage = storage
        self.setup_delegation()
        self.stats = self.server.register_connection(storage_id, self)
        self.lock_manager = self.server.lock_managers[storage_id]

        return self.lastTransaction()

    def get_info(self):
        storage = self.storage

        supportsUndo = (getattr(storage, 'supportsUndo', lambda: False)()
                        and self.connection.protocol_version[1:] >= b'310')

        # Communicate the backend storage interfaces to the client
        storage_provides = zope.interface.providedBy(storage)
        interfaces = []
        for candidate in storage_provides.__iro__:
            interfaces.append((candidate.__module__, candidate.__name__))

        return {'length': len(storage),
                'size': storage.getSize(),
                'name': storage.getName(),
                'supportsUndo': supportsUndo,
                'supports_record_iternext': hasattr(self, 'record_iternext'),
                'interfaces': tuple(interfaces),
                }

    def get_size_info(self):
        return {'length': len(self.storage),
                'size': self.storage.getSize(),
                }

    def loadBefore(self, oid, tid):
        self.stats.loads += 1
        return self.storage.loadBefore(oid, tid)

    def getInvalidations(self, tid):
        invtid, invlist = self.server.get_invalidations(self.storage_id, tid)
        if invtid is None:
            return None
        self.log("Return %d invalidations up to tid %s"
                 % (len(invlist), u64(invtid)))
        return invtid, invlist

    def pack(self, time, wait=1):
        # Yes, you can pack a read-only server or storage!
        if wait:
            return run_in_thread(self._pack_impl, time)
        else:
            # If the client isn't waiting for a reply, start a thread
            # and forget about it.
            t = threading.Thread(target=self._pack_impl, args=(time,))
            t.name = "zeo storage packing thread"
            t.start()
            return None

    def _pack_impl(self, time):
        self.log("pack(time=%s) started..." % repr(time))
        self.storage.pack(time, referencesf)
        self.log("pack(time=%s) complete" % repr(time))
        # Broadcast new size statistics
        self.server.broadcast_info(self.storage_id, self.get_size_info())

    def new_oids(self, n=100):
        """Return a sequence of n new oids, where n defaults to 100"""
        n = min(n, 100)
        if self.read_only:
            raise ReadOnlyError()
        if n <= 0:
            n = 1
        return [self.storage.new_oid() for i in range(n)]

    # undoLog and undoInfo are potentially slow methods

    def undoInfo(self, first, last, spec):
        return run_in_thread(self.storage.undoInfo, first, last, spec)

    def undoLog(self, first, last):
        return run_in_thread(self.storage.undoLog, first, last)

    def tpc_begin(self, id, user, description, ext, tid=None, status=" "):
        if self.read_only:
            raise ReadOnlyError()
        if self.transaction is not None:
            if self.transaction.id == id:
                self.log("duplicate tpc_begin(%s)" % repr(id))
                return
            else:
                raise StorageTransactionError("Multiple simultaneous tpc_begin"
                                              " requests from one client.")

        t = TransactionMetaData(user, description, ext)
        t.id = id

        self.serials = []
        self.conflicts = {}
        self.invalidated = []
        self.txnlog = CommitLog()
        self.blob_log = []
        self.tid = tid
        self.status = status
        self.stats.active_txns += 1

        # Assign the transaction attribute last. This is so we don't
        # think we've entered TPC until everything is set.  Why?
        # Because if we have an error after this, the server will
        # think it is in TPC and the client will think it isn't.  At
        # that point, the client will keep trying to enter TPC and
        # server won't let it.  Errors *after* the tpc_begin call will
        # cause the client to abort the transaction.
        # (Also see https://bugs.launchpad.net/zodb/+bug/374737.)
        self.transaction = t

    def tpc_finish(self, id):
        if not self._check_tid(id):
            return
        assert self.locked, "finished called wo lock"

        self.stats.commits += 1
        self.storage.tpc_finish(self.transaction, self._invalidate)
        self.async_('info', self.get_size_info())
        # Note that the tid is still current because we still hold the
        # commit lock. We'll relinquish it in _clear_transaction.
        tid = self.storage.lastTransaction()
        # Return the tid, for cache invalidation optimization
        return Result(tid, self._clear_transaction)

    def _invalidate(self, tid):
        self.server.invalidate(self, self.storage_id, tid, self.invalidated)

    def tpc_abort(self, tid):
        if not self._check_tid(tid):
            return
        self.stats.aborts += 1
        self.storage.tpc_abort(self.transaction)
        self._clear_transaction()

    def _clear_transaction(self):
        # Common code at end of tpc_finish() and tpc_abort()
        self.lock_manager.release(self)
        self.transaction = None
        self.stats.active_txns -= 1
        if self.txnlog is not None:
            self.txnlog.close()
            self.txnlog = None
            for oid, oldserial, data, blobfilename in self.blob_log:
                ZODB.blob.remove_committed(blobfilename)
            del self.blob_log

    def vote(self, tid):
        self._check_tid(tid, exc=StorageTransactionError)
        return self.lock_manager.lock(self, self._vote)

    def _vote(self, delay=None):
        # Called from client thread

        if not self.connected:
            return  # We're disconnected

        try:
            self.log(
                "Preparing to commit transaction: %d objects, %d bytes"
                % (self.txnlog.stores, self.txnlog.size()),
                level=BLATHER)

            if (self.tid is not None) or (self.status != ' '):
                self.storage.tpc_begin(self.transaction,
                                       self.tid, self.status)
            else:
                self.storage.tpc_begin(self.transaction)

            for op, args in self.txnlog:
                getattr(self, op)(*args)

            # Blob support
            while self.blob_log:
                oid, oldserial, data, blobfilename = self.blob_log.pop()
                self._store(oid, oldserial, data, blobfilename)

            if not self.conflicts:
                try:
                    serials = self.storage.tpc_vote(self.transaction)
                except ConflictError as err:
                    if self.client_conflict_resolution and \
                       err.oid and err.serials and err.data:
                        self.conflicts[err.oid] = dict(
                            oid=err.oid, serials=err.serials, data=err.data)
                    else:
                        raise
                else:
                    if serials:
                        self.serials.extend(serials)

            if self.conflicts:
                self.storage.tpc_abort(self.transaction)
                return list(self.conflicts.values())
            else:
                self.locked = True  # signal to lock manager to hold lock
                return self.serials

        except Exception as err:
            self.storage.tpc_abort(self.transaction)
            self._clear_transaction()

            if isinstance(err, ConflictError):
                self.stats.conflicts += 1
                self.log("conflict error %s" % err, BLATHER)

            if not isinstance(err, TransactionError):
                logger.exception("While voting")

            raise

    # The public methods of the ZEO client API do not do the real work.
    # They defer work until after the storage lock has been acquired.
    # Most of the real implementations are in methods beginning with
    # an _.

    def deleteObject(self, oid, serial, id):
        self._check_tid(id, exc=StorageTransactionError)
        self.stats.stores += 1
        self.txnlog.delete(oid, serial)

    def storea(self, oid, serial, data, id):
        self._check_tid(id, exc=StorageTransactionError)
        self.stats.stores += 1
        self.txnlog.store(oid, serial, data)

    def checkCurrentSerialInTransaction(self, oid, serial, id):
        self._check_tid(id, exc=StorageTransactionError)
        self.txnlog.checkread(oid, serial)

    def restorea(self, oid, serial, data, prev_txn, id):
        self._check_tid(id, exc=StorageTransactionError)
        self.stats.stores += 1
        self.txnlog.restore(oid, serial, data, prev_txn)

    def storeBlobStart(self):
        assert self.blob_tempfile is None
        self.blob_tempfile = tempfile.mkstemp(
            dir=self.storage.temporaryDirectory())

    def storeBlobChunk(self, chunk):
        os.write(self.blob_tempfile[0], chunk)

    def storeBlobEnd(self, oid, serial, data, id):
        self._check_tid(id, exc=StorageTransactionError)
        assert self.txnlog is not None  # effectively not allowed after undo
        fd, tempname = self.blob_tempfile
        self.blob_tempfile = None
        os.close(fd)
        self.blob_log.append((oid, serial, data, tempname))

    def storeBlobShared(self, oid, serial, data, filename, id):
        self._check_tid(id, exc=StorageTransactionError)
        assert self.txnlog is not None  # effectively not allowed after undo

        # Reconstruct the full path from the filename in the OID directory
        if os.path.sep in filename or \
           not (filename.endswith('.tmp') or filename[:-1].endswith('.tmp')):
            logger.critical(
                "We're under attack! (bad filename to storeBlobShared, %r)",
                filename)
            raise ValueError(filename)

        filename = os.path.join(self.storage.fshelper.getPathForOID(oid),
                                filename)
        self.blob_log.append((oid, serial, data, filename))

    def sendBlob(self, oid, serial):
        logger.debug("Blob requested %r, %r", oid, serial)
        blobfilename = self.storage.loadBlob(oid, serial)
        logger.debug("Blob ready %r, %r", oid, serial)

        def store():
            logger.debug("send `receiveBlobStart` for %r, %r", oid, serial)
            yield ('receiveBlobStart', (oid, serial))
            with open(blobfilename, 'rb') as f:
                while 1:
                    chunk = f.read(59000)
                    if not chunk:
                        break
                    logger.debug("send `receiveBlobChunk` for %r, %r",
                                 oid, serial)
                    yield ('receiveBlobChunk', (oid, serial, chunk, ))
            logger.debug("send `receiveBlobStop` for %r, %r", oid, serial)
            yield ('receiveBlobStop', (oid, serial))

        self.connection.call_async_iter(store())

    def undo(*a, **k):
        raise NotImplementedError

    def undoa(self, trans_id, tid):
        self._check_tid(tid, exc=StorageTransactionError)
        self.txnlog.undo(trans_id)

    def _delete(self, oid, serial):
        self.storage.deleteObject(oid, serial, self.transaction)

    def _checkread(self, oid, serial):
        self.storage.checkCurrentSerialInTransaction(
            oid, serial, self.transaction)

    def _store(self, oid, serial, data, blobfile=None):
        try:
            if blobfile is None:
                self.storage.store(oid, serial, data, '', self.transaction)
            else:
                self.storage.storeBlob(
                    oid, serial, data, blobfile, '', self.transaction)
        except ConflictError as err:
            if self.client_conflict_resolution and err.serials:
                self.conflicts[oid] = dict(
                    oid=oid, serials=err.serials, data=data)
            else:
                raise
        else:
            if oid in self.conflicts:
                del self.conflicts[oid]

            self.invalidated.append(oid)

    def _restore(self, oid, serial, data, prev_txn):
        self.storage.restore(oid, serial, data, '', prev_txn,
                             self.transaction)

    def _undo(self, trans_id):
        tid, oids = self.storage.undo(trans_id, self.transaction)
        self.invalidated.extend(oids)
        self.serials.extend(oids)

    # IStorageIteration support

    def iterator_start(self, start, stop):
        iid = next(self._iterator_ids)
        self._iterators[iid] = iter(self.storage.iterator(start, stop))
        return iid

    def iterator_next(self, iid):
        iterator = self._iterators[iid]
        try:
            info = next(iterator)
        except StopIteration:
            del self._iterators[iid]
            item = None
            if iid in self._txn_iterators_last:
                del self._txn_iterators_last[iid]
        else:
            item = (info.tid,
                    info.status,
                    info.user,
                    info.description,
                    info.extension)
            # Keep a reference to the last iterator result to allow starting a
            # record iterator off it.
            self._txn_iterators_last[iid] = info
        return item

    def iterator_record_start(self, txn_iid, tid):
        record_iid = next(self._iterator_ids)
        txn_info = self._txn_iterators_last[txn_iid]
        if txn_info.tid != tid:
            raise Exception(
                'Out-of-order request for record iterator for transaction %r'
                % tid)
        self._iterators[record_iid] = iter(txn_info)
        return record_iid

    def iterator_record_next(self, iid):
        iterator = self._iterators[iid]
        try:
            info = next(iterator)
        except StopIteration:
            del self._iterators[iid]
            item = None
        else:
            item = (info.oid,
                    info.tid,
                    info.data,
                    info.data_txn)
        return item

    def iterator_gc(self, iids):
        for iid in iids:
            it = self._iterators.pop(iid, None)
            if hasattr(it, "close"):
                it.close()

    def server_status(self):
        return self.server.server_status(self.storage_id)

    def set_client_label(self, label):
        self.log_label = str(label)+' '+_addr_label(self.connection.addr)

    def ruok(self):
        return self.server.ruok()

    def ping(self):
        pass


class StorageServerDB:
    """Adapts (StorageServer, storage_id) to ZODB.interfaces.IStorageWrapper.

    The class is used as ``DB`` emulation in a ``registerDB`` call;
    it allows the storage server to keep its cached data about a storage
    up to date and to keep the respective connections informed.

    In particular, the class is used in a ZEO fan-out situation,
    where a storage server calls registerDB on a ClientStorage.
    Note that in this case the methods are called from the Client-storage's
    IO thread, a separate thread from the storge-server connections.
    Thus, the methods need to be thread safe.
    """

    def __init__(self, server, storage_id):
        self.server = server
        self.storage_id = storage_id
        self.references = ZODB.serialize.referencesf

    def invalidate(self, tid, oids, version=''):
        if version:
            raise StorageServerError("Versions aren't supported.")
        storage_id = self.storage_id
        self.server.invalidate(None, storage_id, tid, oids)

    def invalidateCache(self):
        self.server._invalidateCache(self.storage_id)

    transform_record_data = untransform_record_data = lambda self, data: data


class StorageServer:

    """The server side implementation of ZEO.

    The StorageServer is the 'manager' for incoming connections.  Each
    connection is associated with its own ZEOStorage instance (defined
    below).  The StorageServer may handle multiple storages; each
    ZEOStorage instance only handles a single storage.
    """

    def __init__(self, addr, storages,
                 read_only=0,
                 invalidation_queue_size=100,
                 invalidation_age=None,
                 transaction_timeout=None,
                 ssl=None,
                 client_conflict_resolution=False,
                 Acceptor=Acceptor,
                 msgpack=False,
                 ):
        """StorageServer constructor.

        This is typically invoked from the start.py script.

        Arguments (the first two are required and positional):

        addr -- the address at which the server should listen.  This
            can be a tuple (host, port) to signify a TCP/IP connection
            or a pathname string to signify a Unix domain socket
            connection.  A hostname may be a DNS name or a dotted IP
            address.

        storages -- a dictionary giving the storage(s) to handle.  The
            keys are the storage names, the values are the storage
            instances, typically FileStorage or Berkeley storage
            instances.  By convention, storage names are typically
            strings representing small integers starting at '1'.

        read_only -- an optional flag saying whether the server should
            operate in read-only mode.  Defaults to false.  Note that
            even if the server is operating in writable mode,
            individual storages may still be read-only.  But if the
            server is in read-only mode, no write operations are
            allowed, even if the storages are writable.  Note that
            pack() is considered a read-only operation.

        invalidation_queue_size -- The storage server keeps a queue
            of the objects modified by the last N transactions, where
            N == invalidation_queue_size.  This queue is used to
            speed client cache verification when a client disconnects
            for a short period of time.

        invalidation_age --
            If the invalidation queue isn't big enough to support a
            quick verification, but the last transaction seen by a
            client is younger than the invalidation age, then
            invalidations will be computed by iterating over
            transactions later than the given transaction.

        transaction_timeout -- The maximum amount of time to wait for
            a transaction to commit after acquiring the storage lock.
            If the transaction takes too long, the client connection
            will be closed and the transaction aborted.
        """

        self.storages = storages
        msg = ", ".join(
            ["{}:{}:{}".format(name, storage.isReadOnly() and "RO" or "RW",
                           storage.getName())
             for name, storage in storages.items()])
        log("%s created %s with storages: %s" %
            (self.__class__.__name__, read_only and "RO" or "RW", msg))

        self._lock = Lock()
        self.ssl = ssl  # For dev convenience

        self.read_only = read_only
        self.database = None

        # A list, by server, of at most invalidation_queue_size invalidations.
        # The list is kept in sorted order with the most recent
        # invalidation at the front.  The list never has more than
        # self.invq_bound elements.
        self.invq_bound = invalidation_queue_size
        self.invq = {}

        self.zeo_storages_by_storage_id = {}  # {storage_id -> [ZEOStorage]}
        self.lock_managers = {}  # {storage_id -> LockManager}
        self.stats = {}  # {storage_id -> StorageStats}
        for name, storage in storages.items():
            self._setup_invq(name, storage)
            storage.registerDB(StorageServerDB(self, name))
            if client_conflict_resolution:
                # XXX this may go away later, when storages grow
                # configuration for this.
                storage.tryToResolveConflict = never_resolve_conflict
            self.zeo_storages_by_storage_id[name] = []
            self.stats[name] = stats = StorageStats(
                self.zeo_storages_by_storage_id[name])
            if transaction_timeout is None:
                # An object with no-op methods
                timeout = StubTimeoutThread()
            else:
                timeout = TimeoutThread(transaction_timeout)
                timeout.name = f'TimeoutThread for {name}'
                timeout.start()
            self.lock_managers[name] = LockManager(name, stats, timeout)

        self.invalidation_age = invalidation_age
        self.client_conflict_resolution = client_conflict_resolution

        if addr is not None:
            self.acceptor = Acceptor(self, addr, ssl, msgpack)
            if isinstance(addr, tuple) and addr[0]:
                self.addr = self.acceptor.addr
            else:
                self.addr = addr
            self.loop = self.acceptor.loop
            ZODB.event.notify(Serving(self, address=self.acceptor.addr))

    def create_client_handler(self):
        return ZEOStorage(self, self.read_only)

    def _setup_invq(self, name, storage):
        lastInvalidations = getattr(storage, 'lastInvalidations', None)
        if lastInvalidations is None:
            # Using None below doesn't look right, but the first
            # element in invq is never used.  See get_invalidations.
            # (If it was used, it would generate an error, which would
            # be good. :) Doing this allows clients that were up to
            # date when a server was restarted to pick up transactions
            # it subsequently missed.
            self.invq[name] = [(storage.lastTransaction() or z64, None)]
        else:
            self.invq[name] = list(lastInvalidations(self.invq_bound))
            self.invq[name].reverse()

    def register_connection(self, storage_id, zeo_storage):
        """Internal: register a ZEOStorage with a particular storage.

        This is called by ZEOStorage.register().

        The dictionary self.zeo_storages_by_storage_id maps each
        storage name to a list of current ZEOStorages for that
        storage; this information is needed to handle invalidation.
        This function updates this dictionary.

        Returns the timeout and stats objects for the appropriate storage.
        """
        self.zeo_storages_by_storage_id[storage_id].append(zeo_storage)
        return self.stats[storage_id]

    def _invalidateCache(self, storage_id):
        """We need to invalidate any caches we have.

        This basically means telling our clients to
        invalidate/revalidate their caches. We do this by closing them
        and making them reconnect.
        """

        # This method is called from foreign threads.  We have to
        # worry about interaction with the main thread.

        # Rebuild invq
        self._setup_invq(storage_id, self.storages[storage_id])

        # Make a copy since we are going to be mutating the
        # connections indirectoy by closing them.  We don't care about
        # later transactions since they will have to validate their
        # caches anyway.
        for zs in self.zeo_storages_by_storage_id[storage_id][:]:
            zs.call_soon_threadsafe(zs.connection.close)

    def invalidate(self, zeo_storage, storage_id, tid, invalidated):
        """Internal: broadcast invalidations to clients.

        This is called from several ZEOStorage methods.

        invalidated is a sequence of oids.
        """

        # This method can be called from foreign threads.  We have to
        # worry about interaction with the main thread.

        invq = self.invq[storage_id]
        if len(invq) >= self.invq_bound:
            invq.pop()
        invq.insert(0, (tid, invalidated))

        for zs in self.zeo_storages_by_storage_id[storage_id]:
            if zs is not zeo_storage:
                zs.async_threadsafe('invalidateTransaction', tid, invalidated)

    def broadcast_info(self, storage_id, info):
        """Internal: broadcast info to clients.
        """
        for zs in self.zeo_storages_by_storage_id[storage_id]:
            zs.async_threadsafe('info', info)

    def get_invalidations(self, storage_id, tid):
        """Return a tid and list of all objects invalidation since tid.

        The tid is the most recent transaction id seen by the client.

        Returns None if it is unable to provide a complete list
        of invalidations for tid.  In this case, client should
        do full cache verification.

        XXX This API is stupid.  It would be better to simply return a
        list of oid-tid pairs. With this API, we can't really use the
        tid returned and have to discard all versions for an OID. If
        we used the max tid, then loadBefore results from the cache
        might be incorrect.
        """

        # We make a copy of invq because it might be modified by a
        # foreign (other than main thread) calling invalidate above.
        invq = self.invq[storage_id][:]

        oids = set()
        latest_tid = None
        if invq and invq[-1][0] <= tid:
            # We have needed data in the queue
            for _tid, L in invq:
                if _tid <= tid:
                    break
                oids.update(L)
            latest_tid = invq[0][0]
        elif (self.invalidation_age and
              (self.invalidation_age >
               (time.time()-ZODB.TimeStamp.TimeStamp(tid).timeTime())
               )
              ):
            for t in self.storages[storage_id].iterator(p64(u64(tid)+1)):
                for r in t:
                    oids.add(r.oid)
                latest_tid = t.tid
        elif not invq:
            log("invq empty")
        else:
            log(f"tid to old for invq {u64(tid)} < {u64(invq[-1][0])}")

        return latest_tid, list(oids)

    __thread = None

    def start_thread(self, daemon=True):
        self.__thread = thread = threading.Thread(target=self.loop)
        thread.name = "StorageServer(%s)" % _addr_label(self.addr)
        thread.daemon = daemon
        thread.start()

    __closed = False

    def close(self, join_timeout=1):
        """Close the dispatcher so that there are no new connections.

        This is only called from the test suite, AFAICT.
        """
        if self.__closed:
            return
        self.__closed = True

        # Stop accepting connections
        self.acceptor.close()

        ZODB.event.notify(Closed(self))

        # Close open client connections
        for sid, zeo_storages in self.zeo_storages_by_storage_id.items():
            for zs in zeo_storages[:]:
                try:
                    logger.debug("Closing %s", zs.connection)
                    zs.call_soon_threadsafe(zs.connection.close)
                except Exception:
                    logger.exception("closing connection %r", zs)

        for name, storage in self.storages.items():
            logger.info("closing storage %r", name)
            storage.close()

        if self.__thread is not None:
            self.__thread.join(join_timeout)

        self.acceptor = self.loop = None  # break reference cycles

    def close_conn(self, zeo_storage):
        """Remove the given zeo_storage from self.zeo_storages_by_storage_id.

        This is the inverse of register_connection().
        """
        for zeo_storages in self.zeo_storages_by_storage_id.values():
            if zeo_storage in zeo_storages:
                zeo_storages.remove(zeo_storage)

    def server_status(self, storage_id):
        status = self.stats[storage_id].__dict__.copy()
        status['connections'] = len(status['connections'])
        lock_manager = self.lock_managers[storage_id]
        status['waiting'] = len(lock_manager.waiting)
        status['timeout-thread-is-alive'] = lock_manager.timeout.is_alive()
        last_transaction = self.storages[storage_id].lastTransaction()
        last_transaction_hex = codecs.encode(last_transaction, 'hex_codec')
        # doctests and maybe clients expect a str, not bytes
        last_transaction_hex = str(last_transaction_hex, 'ascii')
        status['last-transaction'] = last_transaction_hex
        return status

    def ruok(self):
        return {storage_id: self.server_status(storage_id)
                    for storage_id in self.storages}


class StubTimeoutThread:

    def begin(self, client):
        pass

    def end(self, client):
        pass

    def is_alive(self):
        return 'stub'


class TimeoutThread(threading.Thread):
    """Monitors transaction progress and generates timeouts."""

    # There is one TimeoutThread per storage, because there's one
    # transaction lock per storage.

    def __init__(self, timeout):
        threading.Thread.__init__(self)
        self.name = "TimeoutThread"
        self.daemon = True
        self._timeout = timeout
        self._client = None
        self._deadline = None
        self._cond = threading.Condition()  # Protects _client and _deadline

    def begin(self, client):
        # Called from the restart code the "main" thread, whenever the
        # storage lock is being acquired.
        with self._cond:
            assert self._client is None
            self._client = client
            self._deadline = time.time() + self._timeout
            self._cond.notify()

    def end(self, client):
        # Called from the "main" thread whenever the storage lock is
        # being released.
        with self._cond:
            assert self._client is not None
            assert self._client is client
            self._client = None
            self._deadline = None

    def run(self):
        # Code running in the thread.
        while 1:
            with self._cond:
                while self._deadline is None:
                    self._cond.wait()
                howlong = self._deadline - time.time()
                if howlong <= 0:
                    # Prevent reporting timeout more than once
                    self._deadline = None
                client = self._client  # For the howlong <= 0 branch below

            if howlong <= 0:
                client.log("Transaction timeout after %s seconds" %
                           self._timeout, logging.CRITICAL)
                try:
                    client.call_soon_threadsafe(client.connection.close)
                except:  # NOQA: E722 bare except
                    client.log("Timeout failure", logging.CRITICAL,
                               exc_info=sys.exc_info())
                    self.end(client)
            else:
                time.sleep(howlong)


def run_in_thread(method, *args):
    t = SlowMethodThread(method, args)
    t.start()
    return t.delay


class SlowMethodThread(threading.Thread):
    """Thread to run potentially slow storage methods.

    Clients can use the delay attribute to access the MTDelay object
    used to send a zrpc response at the right time.
    """

    # Some storage methods can take a long time to complete.  If we
    # run these methods in response to an I/O event, they
    # will block all other server activity until they complete.  To
    # avoid blocking, we spawn a separate thread, return an MTDelay()
    # object, and have the thread reply() when it finishes.

    def __init__(self, method, args):
        threading.Thread.__init__(self)
        self.name = f'SlowMethodThread for {method.__name__}'
        self._method = method
        self._args = args
        self.delay = MTDelay()

    def run(self):
        try:
            result = self._method(*self._args)
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception:
            self.delay.error(sys.exc_info())
        else:
            self.delay.reply(result)


def _addr_label(addr):
    if isinstance(addr, bytes):
        return addr.decode('ascii')
    if isinstance(addr, str):
        return addr
    else:
        host, port = addr
        return str(host) + ":" + str(port)


class CommitLog:

    def __init__(self):
        self.file = tempfile.TemporaryFile(suffix=".comit-log")
        self.pickler = Pickler(self.file, 1)
        self.pickler.fast = 1
        self.stores = 0

    def size(self):
        return self.file.tell()

    def delete(self, oid, serial):
        self.pickler.dump(('_delete', (oid, serial)))
        self.stores += 1

    def checkread(self, oid, serial):
        self.pickler.dump(('_checkread', (oid, serial)))
        self.stores += 1

    def store(self, oid, serial, data):
        self.pickler.dump(('_store', (oid, serial, data)))
        self.stores += 1

    def restore(self, oid, serial, data, prev_txn):
        self.pickler.dump(('_restore', (oid, serial, data, prev_txn)))
        self.stores += 1

    def undo(self, transaction_id):
        self.pickler.dump(('_undo', (transaction_id, )))
        self.stores += 1

    def __iter__(self):
        self.file.seek(0)
        unpickler = Unpickler(self.file)
        for i in range(self.stores):
            yield unpickler.load()

    def close(self):
        if self.file:
            self.file.close()
            self.file = None


class ServerEvent:

    def __init__(self, server, **kw):
        self.__dict__.update(kw)
        self.server = server


class Serving(ServerEvent):
    pass


class Closed(ServerEvent):
    pass


def never_resolve_conflict(oid, committedSerial, oldSerial, newpickle,
                           committedData=b''):
    raise ConflictError(oid=oid, serials=(committedSerial, oldSerial),
                        data=newpickle)


class LockManager:

    def __init__(self, storage_id, stats, timeout):
        self.storage_id = storage_id
        self.stats = stats
        self.timeout = timeout
        self.locked = None
        self.waiting = {}  # {ZEOStorage -> (func, delay)}
        self._lock = RLock()

    def lock(self, zs, func):
        """Call the given function with the commit lock.

        If we can get the lock right away, return the result of
        calling the function.

        If we can't get the lock right away, return a delay

        The function must set ``locked`` on the zeo-storage to
        indicate that the zeo-storage should be locked.  Otherwise,
        the lock isn't held pas the call.
        """
        with self._lock:
            if self._can_lock(zs):
                self._locked(zs)
            else:
                if any(w for w in self.waiting if w is zs):
                    raise StorageTransactionError("Already voting (waiting)")

                delay = Delay()
                self.waiting[zs] = (func, delay)
                self._log_waiting(
                    zs, "(%r) queue lock: transactions waiting: %s")

                return delay

        try:
            result = func()
        except Exception:
            self.release(zs)
            raise
        else:
            if not zs.locked:
                self.release(zs)
            return result

    def _lock_waiting(self, zs):
        waiting = None
        with self._lock:
            if self.locked is zs:
                assert zs.locked
                return

            if self._can_lock(zs):
                waiting = self.waiting.pop(zs, None)
                if waiting:
                    self._locked(zs)

        if waiting:
            func, delay = waiting
            try:
                result = func()
            except Exception:
                delay.error(sys.exc_info())
                self.release(zs)
            else:
                delay.reply(result)
                if not zs.locked:
                    self.release(zs)

    def release(self, zs):
        with self._lock:
            locked = self.locked
            if locked is zs:
                self._unlocked(zs)

                for zs in list(self.waiting):
                    zs.call_soon_threadsafe(self._lock_waiting, zs)

            else:
                if self.waiting.pop(zs, None):
                    self._log_waiting(
                        zs, "(%r) dequeue lock: transactions waiting: %s")

    def _log_waiting(self, zs, message):
        length = len(self.waiting)
        zs.log(message % (self.storage_id, length),
               logging.CRITICAL if length > 9 else (
                   logging.WARNING if length > 3 else logging.DEBUG)
               )

    def _can_lock(self, zs):
        locked = self.locked

        if locked is zs:
            raise StorageTransactionError("Already voting (locked)")

        if locked is not None:
            if not locked.connected:
                locked.log("Still locked after disconnected. Unlocking.",
                           logging.CRITICAL)
                if locked.transaction:
                    locked.storage.tpc_abort(locked.transaction)

                self._unlocked(locked)
                locked = None

            # Note that locked.locked may not be true here, because
            # .lock may be set in the lock callback, but may not have
            # been set yet.  This aspect of the API may need more
            # thought. :/

        return locked is None

    def _locked(self, zs):
        self.locked = zs
        self.stats.lock_time = time.time()
        self._log_waiting(zs, "(%r) lock: transactions waiting: %s")
        self.timeout.begin(zs)
        return True

    def _unlocked(self, zs):
        assert self.locked is zs
        self.timeout.end(zs)
        self.locked = self.stats.lock_time = None
        zs.locked = False
        self._log_waiting(zs, "(%r) unlock: transactions waiting: %s")
