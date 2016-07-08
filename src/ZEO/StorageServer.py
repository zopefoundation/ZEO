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
import socket
import sys
import tempfile
import threading
import time
import transaction
import warnings
import ZEO.asyncio.server
import ZODB.blob
import ZODB.event
import ZODB.serialize
import ZODB.TimeStamp
import zope.interface
import six

from ZEO._compat import Pickler, Unpickler, PY3, BytesIO
from ZEO.Exceptions import AuthError
from ZEO.monitor import StorageStats
from ZEO.asyncio.server import Delay, MTDelay, Result
from ZODB.ConflictResolution import ResolvedSerial
from ZODB.loglevels import BLATHER
from ZODB.POSException import StorageError, StorageTransactionError
from ZODB.POSException import TransactionError, ReadOnlyError, ConflictError
from ZODB.serialize import referencesf
from ZODB.utils import oid_repr, p64, u64, z64

from .asyncio.server import Acceptor

logger = logging.getLogger('ZEO.StorageServer')

def log(message, level=logging.INFO, label='', exc_info=False):
    """Internal helper to log a message."""
    if label:
        message = "(%s) %s" % (label, message)
    logger.log(level, message, exc_info=exc_info)


class StorageServerError(StorageError):
    """Error reported when an unpicklable exception is raised."""

registered_methods = set(( 'get_info', 'lastTransaction',
    'getInvalidations', 'new_oids', 'pack', 'loadBefore', 'storea',
    'checkCurrentSerialInTransaction', 'restorea', 'storeBlobStart',
    'storeBlobChunk', 'storeBlobEnd', 'storeBlobShared',
    'deleteObject', 'tpc_begin', 'vote', 'tpc_finish', 'tpc_abort',
    'history', 'record_iternext', 'sendBlob', 'getTid', 'loadSerial',
    'new_oid', 'undoa', 'undoLog', 'undoInfo', 'iterator_start',
    'iterator_next', 'iterator_record_start', 'iterator_record_next',
    'iterator_gc', 'server_status', 'set_client_label'))

class ZEOStorage:
    """Proxy to underlying storage for a single remote client."""

    # A list of extension methods.  A subclass with extra methods
    # should override.
    extensions = []

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
        # The authentication protocol may define extra methods.
        self._extensions = {}
        for func in self.extensions:
            self._extensions[func.__name__] = None
        self._iterators = {}
        self._iterator_ids = itertools.count()
        # Stores the last item that was handed out for a
        # transaction iterator.
        self._txn_iterators_last = {}

    def set_database(self, database):
        self.database = database

    def notify_connected(self, conn):
        self.connection = conn
        self.connected = True
        assert conn.protocol_version is not None
        self.log_label = _addr_label(conn.addr)

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
        return "<%s %X trans=%s s_trans=%s>" % (name, id(self), tid, stid)

    def log(self, msg, level=logging.INFO, exc_info=False):
        log(msg, level=level, label=self.log_label, exc_info=exc_info)

    def setup_delegation(self):
        """Delegate several methods to the storage
        """
        # Called from register

        storage = self.storage

        info = self.get_info()

        if not info['supportsUndo']:
            self.undoLog = self.undoInfo = lambda *a,**k: ()

        self.getTid = storage.getTid
        self.load = storage.load
        self.loadSerial = storage.loadSerial
        record_iternext = getattr(storage, 'record_iternext', None)
        if record_iternext is not None:
            self.record_iternext = record_iternext

        try:
            fn = storage.getExtensionMethods
        except AttributeError:
            pass # no extension methods
        else:
            d = fn()
            self._extensions.update(d)
            for name in d:
                assert not hasattr(self, name)
                setattr(self, name, getattr(storage, name))
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
                self.tpc_transaction = lambda : storage._transaction
            else:
                raise

        self.connection.methods = registered_methods

    def history(self,tid,size=1):
        # This caters for storages which still accept
        # a version parameter.
        return self.storage.history(tid,size=size)

    def _check_tid(self, tid, exc=None):
        if self.read_only:
            raise ReadOnlyError()
        if self.transaction is None:
            caller = sys._getframe().f_back.f_code.co_name
            self.log("no current transaction: %s()" % caller,
                     level=logging.WARNING)
            if exc is not None:
                raise exc(None, tid)
            else:
                return 0
        if self.transaction.id != tid:
            caller = sys._getframe().f_back.f_code.co_name
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

    def get_info(self):
        storage = self.storage

        supportsUndo = (getattr(storage, 'supportsUndo', lambda : False)()
                        and self.connection.protocol_version >= b'Z310')

        # Communicate the backend storage interfaces to the client
        storage_provides = zope.interface.providedBy(storage)
        interfaces = []
        for candidate in storage_provides.__iro__:
            interfaces.append((candidate.__module__, candidate.__name__))

        return {'length': len(storage),
                'size': storage.getSize(),
                'name': storage.getName(),
                'supportsUndo': supportsUndo,
                'extensionMethods': self.getExtensionMethods(),
                'supports_record_iternext': hasattr(self, 'record_iternext'),
                'interfaces': tuple(interfaces),
                }

    def get_size_info(self):
        return {'length': len(self.storage),
                'size': self.storage.getSize(),
                }

    def getExtensionMethods(self):
        return self._extensions

    def loadEx(self, oid):
        self.stats.loads += 1
        return self.storage.load(oid, '')

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
            t.setName("zeo storage packing thread")
            t.start()
            return None

    def _pack_impl(self, time):
        self.log("pack(time=%s) started..." % repr(time))
        self.storage.pack(time, referencesf)
        self.log("pack(time=%s) complete" % repr(time))
        # Broadcast new size statistics
        self.server.invalidate(0, self.storage_id, None,
                               (), self.get_size_info())

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

        t = transaction.Transaction()
        t.id = id
        t.user = user
        t.description = description
        t._extension = ext

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
        self.connection.async('info', self.get_size_info())
        # Note that the tid is still current because we still hold the
        # commit lock. We'll relinquish it in _clear_transaction.
        tid = self.storage.lastTransaction()
        # Return the tid, for cache invalidation optimization
        return Result(tid, self._clear_transaction)

    def _invalidate(self, tid):
        if self.invalidated:
            self.server.invalidate(self, self.storage_id, tid, self.invalidated)

    def tpc_abort(self, tid):
        if not self._check_tid(tid):
            return
        self.stats.aborts += 1
        self.storage.tpc_abort(self.transaction)
        self._clear_transaction()

    def _clear_transaction(self):
        # Common code at end of tpc_finish() and tpc_abort()
        if self.locked:
            self.server.unlock_storage(self)
            self.locked = 0
        if self.transaction is not None:
            self.server.stop_waiting(self)
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
        if self.locked or self.server.already_waiting(self):
            raise StorageTransactionError(
                'Already voting (%s)' % (self.locked and 'locked' or 'waiting')
                )
        return self._try_to_vote()

    def _try_to_vote(self, delay=None):
        if not self.connected:
            return # We're disconnected

        if delay is not None and delay.sent:
            # as a consequence of the unlocking strategy, _try_to_vote
            # may be called multiple times for delayed
            # transactions. The first call will mark the delay as
            # sent. We should skip if the delay was already sent.
            return

        self.locked, delay = self.server.lock_storage(self, delay)
        if self.locked:
            result = None
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
                        if (self.client_conflict_resolution and
                            err.oid and err.serials and err.data
                            ):
                            self.conflicts[err.oid] = dict(
                                oid=err.oid, serials=err.serials, data=err.data)
                        else:
                            raise
                    else:
                        if serials:
                            self.serials.extend(serials)
                        result = self.serials

                if self.conflicts:
                    result = list(self.conflicts.values())
                    self.storage.tpc_abort(self.transaction)
                    self.server.unlock_storage(self)
                    self.locked = False
                    self.server.stop_waiting(self)

            except Exception as err:
                self.storage.tpc_abort(self.transaction)
                self._clear_transaction()

                if isinstance(err, ConflictError):
                    self.stats.conflicts += 1
                    self.log("conflict error %s" % err, BLATHER)
                if not isinstance(err, TransactionError):
                    logger.exception("While voting")

                if delay is not None:
                    delay.error(sys.exc_info())
                else:
                    raise
            else:
                if delay is not None:
                    delay.reply(result)
                else:
                    return result

        else:
            return delay

    def _unlock_callback(self, delay):
        if self.connected:
            self.connection.call_soon_threadsafe(self._try_to_vote, delay)
        else:
            self.server.stop_waiting(self)

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
        assert self.txnlog is not None # effectively not allowed after undo
        fd, tempname = self.blob_tempfile
        self.blob_tempfile = None
        os.close(fd)
        self.blob_log.append((oid, serial, data, tempname))

    def storeBlobShared(self, oid, serial, data, filename, id):
        self._check_tid(id, exc=StorageTransactionError)
        assert self.txnlog is not None # effectively not allowed after undo

        # Reconstruct the full path from the filename in the OID directory
        if (os.path.sep in filename
            or not (filename.endswith('.tmp')
                    or filename[:-1].endswith('.tmp')
                    )
            ):
            logger.critical(
                "We're under attack! (bad filename to storeBlobShared, %r)",
                filename)
            raise ValueError(filename)

        filename = os.path.join(self.storage.fshelper.getPathForOID(oid),
                                filename)
        self.blob_log.append((oid, serial, data, filename))

    def sendBlob(self, oid, serial):
        blobfilename = self.storage.loadBlob(oid, serial)

        def store():
            yield ('receiveBlobStart', (oid, serial))
            with open(blobfilename, 'rb') as f:
                while 1:
                    chunk = f.read(59000)
                    if not chunk:
                        break
                    yield ('receiveBlobChunk', (oid, serial, chunk, ))
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

            if serial != b"\0\0\0\0\0\0\0\0":
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
            self._iterators.pop(iid, None)

    def server_status(self):
        return self.server.server_status(self.storage_id)

    def set_client_label(self, label):
        self.log_label = str(label)+' '+_addr_label(self.connection.addr)

    def ruok(self):
        return self.server.ruok()

class StorageServerDB:
    """Adapter from StorageServerDB to ZODB.interfaces.IStorageWrapper

    This is used in a ZEO fan-out situation, where a storage server
    calls registerDB on a ClientStorage.

    Note that this is called from the Client-storage's IO thread, so
    always a separate thread from the storge-server connections.
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
            ["%s:%s:%s" % (name, storage.isReadOnly() and "RO" or "RW",
                           storage.getName())
             for name, storage in storages.items()])
        log("%s created %s with storages: %s" %
            (self.__class__.__name__, read_only and "RO" or "RW", msg))


        self._lock = threading.Lock()
        self._commit_locks = {}
        self._waiting = dict((name, []) for name in storages)

        self.read_only = read_only
        self.database = None
        # A list, by server, of at most invalidation_queue_size invalidations.
        # The list is kept in sorted order with the most recent
        # invalidation at the front.  The list never has more than
        # self.invq_bound elements.
        self.invq_bound = invalidation_queue_size
        self.invq = {}
        for name, storage in storages.items():
            self._setup_invq(name, storage)
            storage.registerDB(StorageServerDB(self, name))
            if client_conflict_resolution:
                # XXX this may go away later, when storages grow
                # configuration for this.
                storage.tryToResolveConflict = never_resolve_conflict
        self.invalidation_age = invalidation_age
        self.zeo_storages_by_storage_id = {} # {storage_id -> [ZEOStorage]}
        self.client_conflict_resolution = client_conflict_resolution

        if addr is not None:
            self.acceptor = Acceptor(self, addr, ssl)
            if isinstance(addr, tuple) and addr[0]:
                self.addr = self.acceptor.addr
            else:
                self.addr = addr
            self.loop = self.acceptor.loop
            ZODB.event.notify(Serving(self, address=self.acceptor.addr))

        self.stats = {}
        self.timeouts = {}
        for name in self.storages.keys():
            self.zeo_storages_by_storage_id[name] = []
            self.stats[name] = StorageStats(
                self.zeo_storages_by_storage_id[name])
            if transaction_timeout is None:
                # An object with no-op methods
                timeout = StubTimeoutThread()
            else:
                timeout = TimeoutThread(transaction_timeout)
                timeout.setName("TimeoutThread for %s" % name)
                timeout.start()
            self.timeouts[name] = timeout

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

        # 1. We modify self.invq which is read by get_invalidations
        #    below. This is why get_invalidations makes a copy of
        #    self.invq.

        # 2. We access connections.  There are two dangers:
        #
        # a. We miss a new connection.  This is not a problem because
        #    if a client connects after we get the list of connections,
        #    then it will have to read the invalidation queue, which
        #    has already been reset.
        #
        # b. A connection is closes while we are iterating.  This
        #    doesn't matter, bacause we can call should_close on a closed
        #    connection.

        # Rebuild invq
        self._setup_invq(storage_id, self.storages[storage_id])

        # Make a copy since we are going to be mutating the
        # connections indirectoy by closing them.  We don't care about
        # later transactions since they will have to validate their
        # caches anyway.
        for zs in self.zeo_storages_by_storage_id[storage_id][:]:
            zs.connection.call_soon_threadsafe(zs.connection.close)

    def invalidate(
        self, zeo_storage, storage_id, tid, invalidated=(), info=None):
        """Internal: broadcast info and invalidations to clients.

        This is called from several ZEOStorage methods.

        invalidated is a sequence of oids.

        This can do three different things:

        - If the invalidated argument is non-empty, it broadcasts
          invalidateTransaction() messages to all clients of the given
          storage except the current client (the zeo_storage argument).

        - If the invalidated argument is empty and the info argument
          is a non-empty dictionary, it broadcasts info() messages to
          all clients of the given storage, including the current
          client.

        - If both the invalidated argument and the info argument are
          non-empty, it broadcasts invalidateTransaction() messages to all
          clients except the current, and sends an info() message to
          the current client.

        """

        # This method can be called from foreign threads.  We have to
        # worry about interaction with the main thread.

        # 1. We modify self.invq which is read by get_invalidations
        #    below. This is why get_invalidations makes a copy of
        #    self.invq.

        # 2. We access connections.  There are two dangers:
        #
        # a. We miss a new connection.  This is not a problem because
        #    we are called while the storage lock is held.  A new
        #    connection that tries to read data won't read committed
        #    data without first recieving an invalidation.  Also, if a
        #    client connects after getting the list of connections,
        #    then it will have to read the invalidation queue, which
        #    has been updated to reflect the invalidations.
        #
        # b. A connection is closes while we are iterating. We'll need
        #    to cactch and ignore Disconnected errors.


        if invalidated:
            invq = self.invq[storage_id]
            if len(invq) >= self.invq_bound:
                invq.pop()
            invq.insert(0, (tid, invalidated))
            # serialize invalidation message, so we don't have to to
            # it over and over

        for zs in self.zeo_storages_by_storage_id[storage_id]:
            connection = zs.connection
            if invalidated and zs is not zeo_storage:
                connection.call_soon_threadsafe(
                    connection.async, 'invalidateTransaction', tid, invalidated)
            elif info is not None:
                connection.call_soon_threadsafe(
                    connection.async, 'info', info)

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
            log("tid to old for invq %s < %s" % (u64(tid), u64(invq[-1][0])))

        return latest_tid, list(oids)

    __thread = None
    def start_thread(self, daemon=True):
        self.__thread = thread = threading.Thread(target=self.loop)
        thread.setName("StorageServer(%s)" % _addr_label(self.addr))
        thread.setDaemon(daemon)
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
                    zs.connection.call_soon_threadsafe(
                        zs.connection.close)
                except Exception:
                    logger.exception("closing connection %r", zs)

        for name, storage in six.iteritems(self.storages):
            logger.info("closing storage %r", name)
            storage.close()

        if self.__thread is not None:
            self.__thread.join(join_timeout)

    def close_conn(self, zeo_storage):
        """Remove the given zeo_storage from self.zeo_storages_by_storage_id.

        This is the inverse of register_connection().
        """
        for zeo_storages in self.zeo_storages_by_storage_id.values():
            if zeo_storage in zeo_storages:
                zeo_storages.remove(zeo_storage)

    def lock_storage(self, zeostore, delay):
        storage_id = zeostore.storage_id
        waiting = self._waiting[storage_id]
        with self._lock:

            if storage_id in self._commit_locks:
                # The lock is held by another zeostore

                locked = self._commit_locks[storage_id]

                assert locked is not zeostore, (storage_id, delay)

                if not locked.connected:
                    locked.log("Still locked after disconnected. Unlocking.",
                               logging.CRITICAL)
                    if locked.transaction:
                        locked.storage.tpc_abort(locked.transaction)
                    del self._commit_locks[storage_id]
                    # yuck: have to manipulate lock to appease with :(
                    self._lock.release()
                    try:
                        return self.lock_storage(zeostore, delay)
                    finally:
                        self._lock.acquire()

                if delay is None:
                    # New request, queue it
                    assert not [i for i in waiting if i[0] is zeostore
                                ], "already waiting"
                    delay = Delay()
                    waiting.append((zeostore, delay))
                    zeostore.log("(%r) queue lock: transactions waiting: %s"
                                 % (storage_id, len(waiting)),
                                 _level_for_waiting(waiting)
                                 )

                return False, delay
            else:
                self._commit_locks[storage_id] = zeostore
                self.timeouts[storage_id].begin(zeostore)
                self.stats[storage_id].lock_time = time.time()
                if delay is not None:
                    # we were waiting, stop
                    waiting[:] = [i for i in waiting if i[0] is not zeostore]
                zeostore.log("(%r) lock: transactions waiting: %s"
                             % (storage_id, len(waiting)),
                             _level_for_waiting(waiting)
                             )
                return True, delay

    def unlock_storage(self, zeostore):
        storage_id = zeostore.storage_id
        waiting = self._waiting[storage_id]
        with self._lock:
            assert self._commit_locks[storage_id] is zeostore
            del self._commit_locks[storage_id]
            self.timeouts[storage_id].end(zeostore)
            self.stats[storage_id].lock_time = None
            callbacks = waiting[:]

        if callbacks:
            assert not [i for i in waiting if i[0] is zeostore
                        ], "waiting while unlocking"
            zeostore.log("(%r) unlock: transactions waiting: %s"
                         % (storage_id, len(callbacks)),
                         _level_for_waiting(callbacks)
                         )

            for zeostore, delay in callbacks:
                try:
                    zeostore._unlock_callback(delay)
                except (SystemExit, KeyboardInterrupt):
                    raise
                except Exception:
                    logger.exception("Calling unlock callback")


    def stop_waiting(self, zeostore):
        storage_id = zeostore.storage_id
        waiting = self._waiting[storage_id]
        with self._lock:
            new_waiting = [i for i in waiting if i[0] is not zeostore]
            if len(new_waiting) == len(waiting):
                return
            waiting[:] = new_waiting

        zeostore.log("(%r) dequeue lock: transactions waiting: %s"
                     % (storage_id, len(waiting)),
                     _level_for_waiting(waiting)
                     )

    def already_waiting(self, zeostore):
        storage_id = zeostore.storage_id
        waiting = self._waiting[storage_id]
        with self._lock:
            return bool([i for i in waiting if i[0] is zeostore])

    def server_status(self, storage_id):
        status = self.stats[storage_id].__dict__.copy()
        status['connections'] = len(status['connections'])
        status['waiting'] = len(self._waiting[storage_id])
        status['timeout-thread-is-alive'] = self.timeouts[storage_id].isAlive()
        last_transaction = self.storages[storage_id].lastTransaction()
        last_transaction_hex = codecs.encode(last_transaction, 'hex_codec')
        if PY3:
            # doctests and maybe clients expect a str, not bytes
            last_transaction_hex = str(last_transaction_hex, 'ascii')
        status['last-transaction'] = last_transaction_hex
        return status

    def ruok(self):
        return dict((storage_id, self.server_status(storage_id))
                    for storage_id in self.storages)


def _level_for_waiting(waiting):
    if len(waiting) > 9:
        return logging.CRITICAL
    if len(waiting) > 3:
        return logging.WARNING
    else:
        return logging.DEBUG

class StubTimeoutThread:

    def begin(self, client):
        pass

    def end(self, client):
        pass

    isAlive = lambda self: 'stub'


class TimeoutThread(threading.Thread):
    """Monitors transaction progress and generates timeouts."""

    # There is one TimeoutThread per storage, because there's one
    # transaction lock per storage.

    def __init__(self, timeout):
        threading.Thread.__init__(self)
        self.setName("TimeoutThread")
        self.setDaemon(1)
        self._timeout = timeout
        self._client = None
        self._deadline = None
        self._cond = threading.Condition() # Protects _client and _deadline

    def begin(self, client):
        # Called from the restart code the "main" thread, whenever the
        # storage lock is being acquired.  (Serialized by asyncore.)
        with self._cond:
            assert self._client is None
            self._client = client
            self._deadline = time.time() + self._timeout
            self._cond.notify()

    def end(self, client):
        # Called from the "main" thread whenever the storage lock is
        # being released.  (Serialized by asyncore.)
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
                client = self._client # For the howlong <= 0 branch below

            if howlong <= 0:
                client.log("Transaction timeout after %s seconds" %
                           self._timeout, logging.CRITICAL)
                try:
                    client.connection.call_soon_threadsafe(
                        client.connection.close)
                except:
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
    # run these methods via a standard asyncore read handler, they
    # will block all other server activity until they complete.  To
    # avoid blocking, we spawn a separate thread, return an MTDelay()
    # object, and have the thread reply() when it finishes.

    def __init__(self, method, args):
        threading.Thread.__init__(self)
        self.setName("SlowMethodThread for %s" % method.__name__)
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
    if isinstance(addr, six.binary_type):
        return addr.decode('ascii')
    if isinstance(addr, six.string_types):
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
