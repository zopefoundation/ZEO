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
"""The ClientStorage class and the exceptions that it may raise.

Public contents of this module:

ClientStorage -- the main class, implementing the Storage API

"""
import logging
import os
import re
import socket
import stat
import sys
import threading
import time
import weakref
from binascii import hexlify
from threading import get_ident

import BTrees.OOBTree
import zc.lockfile
import ZODB
import ZODB.BaseStorage
import ZODB.ConflictResolution
import ZODB.interfaces
import zope.interface
from persistent.TimeStamp import TimeStamp
from ZODB import POSException
from ZODB import utils

import ZEO.asyncio.client
import ZEO.cache
from ZEO._compat import WIN
from ZEO.Exceptions import ClientDisconnected
from ZEO.TransactionBuffer import TransactionBuffer


logger = logging.getLogger(__name__)


def tid2time(tid):
    return str(TimeStamp(tid))


def get_timestamp(prev_ts=None):
    """Internal helper to return a unique TimeStamp instance.

    If the optional argument is not None, it must be a TimeStamp; the
    return value is then guaranteed to be at least 1 microsecond later
    the argument.

    """
    t = time.time()
    t = TimeStamp(*time.gmtime(t)[:5] + (t % 60,))
    if prev_ts is not None:
        t = t.laterThan(prev_ts)
    return t


MB = 1024**2


@zope.interface.implementer(ZODB.interfaces.IMultiCommitStorage)
class ClientStorage(ZODB.ConflictResolution.ConflictResolvingStorage):
    """A storage class that is a network client to a remote storage.

    This is a faithful implementation of the Storage API.

    This class is thread-safe; transactions are serialized in
    tpc_begin().

    """

    def __init__(self, addr, storage='1', cache_size=20 * MB,
                 name='', wait_timeout=None,
                 disconnect_poll=None,
                 read_only=0, read_only_fallback=0,
                 blob_dir=None, shared_blob_dir=False,
                 blob_cache_size=None, blob_cache_size_check=10,
                 client_label=None,
                 cache=None,
                 ssl=None, ssl_server_hostname=None,
                 # Mostly ignored backward-compatability options
                 client=None, var=None,
                 min_disconnect_poll=1, max_disconnect_poll=None,
                 wait=True,
                 drop_cache_rather_verify=True,
                 credentials=None,
                 server_sync=False,
                 # The ZODB-define ZConfig support may ball these:
                 username=None, password=None, realm=None,
                 # For tests:
                 _client_factory=ZEO.asyncio.client.ClientThread,
                 ):
        """ClientStorage constructor.

        This is typically invoked from a custom_zodb.py file.

        All arguments except addr should be keyword arguments.

        Arguments:

        addr
            The server address(es).  This is either a list of
            addresses or a single address.  Each address can be a
            (hostname, port) tuple to signify a TCP/IP connection or
            a pathname string to signify a Unix domain socket
            connection.  A hostname may be a DNS name or a dotted IP
            address.  Required.

            All addresses are assumed to serve (essentially)
            the same (potentially replicated) storage.
            A connection tries to connect to those addresses;
            the first successful connection establishment with
            the called for ("read_only" or "writable") capabilities
            is selected and used for storage interaction until
            the connection is lost. In that case, a
            reconnection is tried.
            If ``ClientStorage`` calls for the "writable" capability
            but allows for a "read only" fallback,,
            a read only connection can be used as a fallback;
            if a writable connection becomes available later, a
            switch to this connection is performed.

        storage
            The server storage name, defaulting to '1'.  The name must
            match one of the storage names supported by the server(s)
            specified by the addr argument.

        cache_size
            The disk cache size, defaulting to 20 megabytes.
            This is passed to the ClientCache constructor.

        name
            The storage name, defaulting to a combination of the
            address and the server storage name.  This is used to
            construct the response to getName()

        wait_timeout
            Maximum time (seconds) to wait for connections,
            defaulting to 30.
            Note: the timeout applies only to [re]connect.
            Normal operations can take arbitrary long. This
            is important for long running operations, such as ``pack``.

        read_only
            A flag indicating whether this should be a
            read-only storage, defaulting to false (i.e. writing is
            allowed by default).

        read_only_fallback
            A flag indicating whether a read-only
            remote storage should be acceptable as a fallback when no
            writable storages are available.  Defaults to false.  At
            most one of read_only and read_only_fallback should be
            true.

        blob_dir
            directory path for blob data.  'blob data' is data that
            is retrieved via the loadBlob API.

        shared_blob_dir
            Flag whether the blob_dir is a server-shared filesystem
            that should be used instead of transferring blob data over
            the ZEO protocol.

        blob_cache_size
            Maximum size of the ZEO blob cache, in bytes.  If not set, then
            the cache size isn't checked and the blob directory will
            grow without bound.

            This option is ignored if shared_blob_dir is true.

        blob_cache_size_check
            Cache check size as percent of blob_cache_size.  The ZEO
            cache size will be checked when this many bytes have been
            loaded into the cache. Defaults to 10% of the blob cache
            size.   This option is ignored if shared_blob_dir is true.

        client_label
            A label to include in server log messages for the client.

        cache
            A cache object or a file path (relative or absolute).
            Defaults to None, in which case the cache is determined
            from client and var.

        ssl
            An ssl client context (i.e. with purpose "ServerAuth")
            to call for SSL connections.

        ssl_server_hostname
            The server hostname - used during the SSL authentication check

        client
        var
            If cache is None, client determines the cache:
            if it is None, then a non persistent cache is used;
            otherwise, client is used together with var (defaults
            to the current working directory) to construct the
            file path for the persistent cache file

        wait
            Wait for server connection, defaulting to true.

        server_sync
            Whether sync() should make a server round trip, thus causing client
            to wait for outstanding invalidations.

            The `sync` is called in `transaction.begin`. A server round trip
            at this place guarantees that the transaction takes notice of all
            prior modifications.

            This may be important when several client processes share the same
            ZODB as the following examples demonstrate.

            Example 1: Assume that a user issues requests req1 and then req2
            where req1 modifies data read by req2. If req1 and req2 are
            processed by different Zope processes, then the transaction started
            for req2 processing may see a ZODB state which does not yet include
            the req1 modifications. A server round trip avoids this.

            Example 2: A similar situation arises when 2 Zope processes
            communicate via non ZODB means to inform the other about a state
            change. If this communication happens to be faster than the ZODB
            internal state change propagation then the target process again
            risks to not yet see the changed state. A server round trip, again,
            avoids this.

            Defaults to false.

        credentials
        username
        password
        realm
        disconnect_poll
        min_disconnect_poll
        max_disconnect_poll
        drop_cache_rather_verify
            ignored; retained (as parameters) for compatibility
        """

        if isinstance(addr, int):
            addr = ('127.0.0.1', addr)

        self.__name__ = name or str(addr)  # Standard convention for storages

        if isinstance(addr, str):
            if WIN:
                raise ValueError("Unix sockets are not available on Windows")
            addr = [addr]
        elif (isinstance(addr, tuple) and len(addr) == 2 and
              isinstance(addr[0], str) and
              isinstance(addr[1], int)):
            addr = [addr]

        logger.info(
            "%s %s (pid=%d) created %s/%s for storage: %r",
            self.__name__,
            self.__class__.__name__,
            os.getpid(),
            read_only and "RO" or "RW",
            read_only_fallback and "fallback" or "normal",
            storage,
            )

        self._is_read_only = read_only
        self._read_only_fallback = read_only_fallback

        self._addr = addr  # For tests

        self._iterators = weakref.WeakValueDictionary()
        self._iterator_ids = set()
        self._storage = storage

        # _server_addr is used by sortKey()
        self._server_addr = None

        self._client_label = client_label

        self._info = {'length': 0, 'size': 0, 'name': 'ZEO Client',
                      'supportsUndo': 0, 'interfaces': ()}

        self._db = None

        self._oids = []  # List of pre-fetched oids from server

        cache = self._cache = open_cache(
            cache, var, client, storage, cache_size)

        # XXX need to check for POSIX-ness here
        self.blob_dir = blob_dir
        self.shared_blob_dir = shared_blob_dir

        if blob_dir is not None:
            # Avoid doing this import unless we need it, as it
            # currently requires pywin32 on Windows.
            import ZODB.blob
            if shared_blob_dir:
                self.fshelper = ZODB.blob.FilesystemHelper(blob_dir)
            else:
                if 'zeocache' not in ZODB.blob.LAYOUTS:
                    ZODB.blob.LAYOUTS['zeocache'] = BlobCacheLayout()
                self.fshelper = ZODB.blob.FilesystemHelper(
                    blob_dir, layout_name='zeocache')
                self.fshelper.create()
        else:
            self.fshelper = None

        self._blob_cache_size = blob_cache_size
        self._blob_data_bytes_loaded = 0
        if blob_cache_size is not None:
            assert blob_cache_size_check < 100
            self._blob_cache_size_check = (
                blob_cache_size * blob_cache_size_check // 100)
            self._check_blob_size()

        self.server_sync = server_sync

        self._server = _client_factory(
            addr, self, cache, storage,
            ZEO.asyncio.client.Fallback if read_only_fallback else read_only,
            wait_timeout or 30,
            ssl=ssl, ssl_server_hostname=ssl_server_hostname,
            )
        self._call = self._server.call
        self._async = self._server.async_
        self._async_iter = self._server.async_iter
        self._wait = self._server.wait

        self._commit_lock = threading.Lock()

        if wait:
            try:
                self._wait()
            except Exception:
                # No point in keeping the server going if the storage
                # creation fails
                self._server.close()
                raise

    def new_addr(self, addr):
        self._addr = addr
        self._server.new_addrs(self._normalize_addr(addr))

    def _normalize_addr(self, addr):
        if isinstance(addr, int):
            addr = ('127.0.0.1', addr)

        if isinstance(addr, str):
            addr = [addr]
        elif (isinstance(addr, tuple) and len(addr) == 2 and
              isinstance(addr[0], str) and isinstance(addr[1], int)):
            addr = [addr]
        return addr

    def close(self):
        "Storage API: finalize the storage, releasing external resources."
        self._server.close()

        if self._check_blob_size_thread is not None:
            self._check_blob_size_thread.join()

        self.ping = self.sync = None  # break reference cycles

    _check_blob_size_thread = None

    def _check_blob_size(self, bytes=None):
        if self._blob_cache_size is None:
            return
        if self.shared_blob_dir or not self.blob_dir:
            return

        if (bytes is not None) and (bytes < self._blob_cache_size_check):
            return

        self._blob_data_bytes_loaded = 0

        target = max(self._blob_cache_size - self._blob_cache_size_check, 0)

        check_blob_size_thread = threading.Thread(
            target=_check_blob_cache_size,
            args=(self.blob_dir, target),
            name="%s zeo client check blob size thread" % self.__name__,
            )
        check_blob_size_thread.daemon = True
        check_blob_size_thread.start()
        self._check_blob_size_thread = check_blob_size_thread

    def registerDB(self, db):
        """Storage API: register a database for invalidation messages.

        This is called by ZODB.DB (and by some tests).

        The storage isn't really ready to use until after this call.
        """
        super().registerDB(db)
        self._db = db

    def is_connected(self, test=False):
        """Return whether the storage is currently connected to a server."""
        return self._server.is_connected()

    def sync(self):
        # The separate async thread should keep us up to date
        pass

    _connection_generation = 0

    def notify_connected(self, conn, info):
        """The connection is about to be established via *conn*.

        *conn* is an ``asyncio.client.ClientIO`` instance.
        We can use it already for asynchronous server calls
        but must not make synchronous calls or
        use the normal server call API (would lead to deadlock
        or ``ClientDisconnected``).

        *info* is a ``dict`` providing information about the server
        (and its associated storage).
        """
        self.set_server_addr(conn.get_peername())
        self.protocol_version = conn.protocol_version
        self._is_read_only = conn.is_read_only()

        # invalidate our db cache
        if self._db is not None:
            self._db.invalidateCache()

        logger.info("%s %s to storage: %s",
                    self.__name__,
                    'Reconnected' if self._connection_generation
                    else 'Connected',
                    self._server_addr)

        self._connection_generation += 1

        if self._client_label:
            # Note: we cannot yet use ``_async`` (connection not yet
            # officially established) but can already use
            # ``ClientIO.call_async``
            conn.call_async('set_client_label', (self._client_label,))

        self._info.update(info)

        for iface in (ZODB.interfaces.IStorageRestoreable,
                      ZODB.interfaces.IStorageIteration,
                      ZODB.interfaces.IStorageUndoable,
                      ZODB.interfaces.IStorageCurrentRecordIteration,
                      ZODB.interfaces.IBlobStorage,
                      ZODB.interfaces.IExternalGC):
            if (iface.__module__, iface.__name__) in \
               self._info.get('interfaces', ()):
                zope.interface.alsoProvides(self, iface)

        if self.protocol_version[1:] >= b'5':
            self.ping = lambda: self._call('ping')
        else:
            self.ping = lambda: self._call('lastTransaction')

        if self.server_sync:
            self.sync = self.ping

    def set_server_addr(self, addr):
        # Normalize server address and convert to string
        if isinstance(addr, str):
            self._server_addr = addr
        else:
            assert isinstance(addr, tuple)
            # If the server is on a remote host, we need to guarantee
            # that all clients used the same name for the server.  If
            # they don't, the sortKey() may be different for each client.
            # The best solution seems to be the official name reported
            # by gethostbyaddr().
            host = addr[0]
            try:
                canonical, aliases, addrs = socket.gethostbyaddr(host)
            except OSError as err:
                logger.debug("%s Error resolving host: %s (%s)",
                             self.__name__, host, err)
                canonical = host
            self._server_addr = str((canonical, addr[1]))

    def sortKey(self):
        # XXX sortKey should be explicit, possibly based on database name.

        # If the client isn't connected to anything, it can't have a
        # valid sortKey().  Raise an error to stop the transaction early.
        if self._server_addr is None:
            raise ClientDisconnected
        else:
            return f'{self._storage}:{self._server_addr}'

    def notify_disconnected(self):
        """Internal: notify that the server connection was terminated.

        This is called by ConnectionManager when the connection is
        closed or when certain problems with the connection occur.

        """
        logger.info("%s Disconnected from storage: %r",
                    self.__name__, self._server_addr)
        self._iterator_gc(True)
        self._connection_generation += 1
        self._is_read_only = self._server.is_read_only()

    def __len__(self):
        """Return the size of the storage."""
        # TODO:  Is this method used?
        return self._info['length']

    def getName(self):
        """Storage API: return the storage name as a string.

        The return value consists of two parts: the name as determined
        by the name and addr argments to the ClientStorage
        constructor, and the string 'connected' or 'disconnected' in
        parentheses indicating whether the storage is (currently)
        connected.

        """
        return "{} ({})".format(
            self.__name__,
            self.is_connected() and "connected" or "disconnected")

    def getSize(self):
        """Storage API: an approximate size of the database, in bytes."""
        return self._info['size']

    def supportsUndo(self):
        """Storage API: return whether we support undo."""
        return self._info['supportsUndo']

    def is_read_only(self):
        """Storage API: return whether we are in read-only mode.
        """
        return self._is_read_only or self._server.is_read_only()

    isReadOnly = is_read_only

    def _check_trans(self, trans, meth):
        """Internal helper to check a transaction argument for sanity."""
        if self._is_read_only:
            raise POSException.ReadOnlyError()

        try:
            buf = trans.data(self)
        except KeyError:
            buf = None

        if buf is None:
            raise POSException.StorageTransactionError(
                "Transaction not committing", meth, trans)

        if buf.connection_generation != self._connection_generation:
            # We were disconnected, so this one is poisoned
            raise ClientDisconnected(meth, 'on a disconnected transaction')

        return buf

    def history(self, oid, size=1):
        """Storage API: return a sequence of HistoryEntry objects.
        """
        return self._call('history', oid, size)

    def record_iternext(self, next=None):
        """Storage API: get the next database record.

        This is part of the conversion-support API.
        """
        return self._call('record_iternext', next)

    def getTid(self, oid):
        # XXX deprecated: but ZODB tests use this. They shouldn't
        return self._call('getTid', oid)

    def loadSerial(self, oid, serial):
        """Storage API: load a historical revision of an object."""
        return self._call('loadSerial', oid, serial)

    def load(self, oid, version=''):
        # Note: cache correctness is guaranteed only upto
        # ``_cache.getLastTid()``. The following call therefore
        # may return outdated information.
        result = self.loadBefore(oid, utils.maxtid)
        if result is None:
            raise POSException.POSKeyError(oid)
        return result[:2]

    def loadBefore(self, oid, tid):
        # Note: cache correctness is guaranteed only for
        # ``tid <= _cache.getLastTid()``.
        # For larger *tid*, the cache may contain outdated information.
        result = self._cache.loadBefore(oid, tid)
        if result:
            return result

        return self._server.load_before(oid, tid)

    def prefetch(self, oids, tid):
        self._server.prefetch(oids, tid)

    def new_oid(self):
        """Storage API: return a new object identifier.
        """
        if self._is_read_only:
            raise POSException.ReadOnlyError()

        while 1:
            try:
                return self._oids.pop()
            except IndexError:
                pass  # We ran out. We need to get some more.

            self._oids[:0] = reversed(self._call('new_oids'))

    def pack(self, t=None, referencesf=None, wait=1, days=0):
        """Storage API: pack the storage.

        Deviations from the Storage API: the referencesf argument is
        ignored; two additional optional arguments wait and days are
        provided:

        wait -- a flag indicating whether to wait for the pack to
            complete; defaults to true.

        days -- a number of days to subtract from the pack time;
            defaults to zero.

        """
        # TODO: Is it okay that read-only connections allow pack()?
        # rf argument ignored; server will provide its own implementation
        if t is None:
            t = time.time()
        t = t - (days * 86400)
        return self._call('pack', t, wait)

    def store(self, oid, serial, data, version, txn):
        """Storage API: store data for an object."""
        assert not version

        tbuf = self._check_trans(txn, 'store')
        self._async('storea', oid, serial, data, id(txn))
        tbuf.store(oid, data)

    def checkCurrentSerialInTransaction(self, oid, serial, transaction):
        self._check_trans(transaction, 'checkCurrentSerialInTransaction')
        self._async(
            'checkCurrentSerialInTransaction', oid, serial, id(transaction))

    def storeBlob(self, oid, serial, data, blobfilename, version, txn):
        """Storage API: store a blob object."""
        assert not version
        tbuf = self._check_trans(txn, 'storeBlob')

        # Grab the file right away. That way, if we don't have enough
        # room for a copy, we'll know now rather than in tpc_finish.
        # Also, this releaves the client of having to manage the file
        # (or the directory contianing it).
        self.fshelper.getPathForOID(oid, create=True)
        fd, target = self.fshelper.blob_mkstemp(oid, serial)
        os.close(fd)

        # It's a bit odd (and impossible on windows) to rename over
        # an existing file.  We'll use the temporary file name as a base.
        target += '-'
        ZODB.blob.rename_or_copy_blob(blobfilename, target)
        os.remove(target[:-1])

        serials = self.store(oid, serial, data, '', txn)
        if self.shared_blob_dir:
            self._async(
                'storeBlobShared',
                oid, serial, data, os.path.basename(target), id(txn))
        else:

            # Store a blob to the server.  We don't want to read all of
            # the data into memory, so we use a message iterator.  This
            # allows us to read the blob data as needed.

            def store():
                yield ('storeBlobStart', ())
                f = open(target, 'rb')
                while 1:
                    chunk = f.read(59000)
                    if not chunk:
                        break
                    yield ('storeBlobChunk', (chunk, ))
                f.close()
                yield ('storeBlobEnd', (oid, serial, data, id(txn)))

            self._async_iter(store())
            tbuf.storeBlob(oid, target)

        return serials

    def receiveBlobStart(self, oid, serial):
        logger.debug("receiveBlobStart for %r, %r", oid, serial)
        blob_filename = self.fshelper.getBlobFilename(oid, serial)
        assert not os.path.exists(blob_filename)
        lockfilename = os.path.join(os.path.dirname(blob_filename), '.lock')
        assert os.path.exists(lockfilename)
        blob_filename += '.dl'
        assert not os.path.exists(blob_filename)
        f = open(blob_filename, 'wb')
        f.close()

    def receiveBlobChunk(self, oid, serial, chunk):
        logger.debug("receiveBlobChunk for %r, %r", oid, serial)
        blob_filename = self.fshelper.getBlobFilename(oid, serial)+'.dl'
        assert os.path.exists(blob_filename)
        f = open(blob_filename, 'r+b')
        f.seek(0, 2)
        f.write(chunk)
        f.close()
        self._blob_data_bytes_loaded += len(chunk)
        self._check_blob_size(self._blob_data_bytes_loaded)

    def receiveBlobStop(self, oid, serial):
        logger.debug("receiveBlobStop for %r, %r", oid, serial)
        blob_filename = self.fshelper.getBlobFilename(oid, serial)
        os.rename(blob_filename+'.dl', blob_filename)
        os.chmod(blob_filename, stat.S_IREAD)

    def deleteObject(self, oid, serial, txn):
        tbuf = self._check_trans(txn, 'deleteObject')
        self._async('deleteObject', oid, serial, id(txn))
        tbuf.store(oid, None)

    def loadBlob(self, oid, serial):
        # Load a blob.  If it isn't present and we have a shared blob
        # directory, then assume that it doesn't exist on the server
        # and return None.

        if self.fshelper is None:
            raise POSException.Unsupported("No blob cache directory is "
                                           "configured.")

        blob_filename = self.fshelper.getBlobFilename(oid, serial)
        if self.shared_blob_dir:
            if os.path.exists(blob_filename):
                return blob_filename
            else:
                # We're using a server shared cache.  If the file isn't
                # here, it's not anywhere.
                raise POSException.POSKeyError(
                        "No blob file at %s" % blob_filename, oid, serial)

        if os.path.exists(blob_filename):
            return _accessed(blob_filename)

        # First, we'll create the directory for this oid, if it doesn't exist.
        self.fshelper.createPathForOID(oid)

        # OK, it's not here and we (or someone) needs to get it.  We
        # want to avoid getting it multiple times.  We want to avoid
        # getting it multiple times even accross separate client
        # processes on the same machine. We'll use file locking.

        lock = _lock_blob(blob_filename)
        try:
            # We got the lock, so it's our job to download it.  First,
            # we'll double check that someone didn't download it while we
            # were getting the lock:

            if os.path.exists(blob_filename):
                return _accessed(blob_filename)

            # Ask the server to send it to us.  When this function
            # returns, it will have been sent. (The recieving will
            # have been handled by the IO thread.)

            self._call('sendBlob', oid, serial)

            if os.path.exists(blob_filename):
                return _accessed(blob_filename)

            raise POSException.POSKeyError("No blob file", oid, serial)

        finally:
            lock.close()

    def openCommittedBlobFile(self, oid, serial, blob=None):
        blob_filename = self.loadBlob(oid, serial)
        try:
            if blob is None:
                return open(blob_filename, 'rb')
            else:
                return ZODB.blob.BlobFile(blob_filename, 'r', blob)
        except (OSError):
            # The file got removed while we were opening.
            # Fall through and try again with the protection of the lock.
            pass

        lock = _lock_blob(blob_filename)
        try:
            blob_filename = self.fshelper.getBlobFilename(oid, serial)
            if not os.path.exists(blob_filename):
                if self.shared_blob_dir:
                    # We're using a server shared cache.  If the file isn't
                    # here, it's not anywhere.
                    raise POSException.POSKeyError("No blob file", oid, serial)
                self._call('sendBlob', oid, serial)
                if not os.path.exists(blob_filename):
                    raise POSException.POSKeyError("No blob file", oid, serial)

            _accessed(blob_filename)
            if blob is None:
                return open(blob_filename, 'rb')
            else:
                return ZODB.blob.BlobFile(blob_filename, 'r', blob)
        finally:
            lock.close()

    def temporaryDirectory(self):
        return self.fshelper.temp_dir

    def tpc_vote(self, txn):
        """Storage API: vote on a transaction.
        """
        tbuf = self._check_trans(txn, 'tpc_vote')
        try:

            conflicts = True
            vote_attempts = 0
            while conflicts and vote_attempts < 9:  # 9? Mainly avoid inf. loop
                conflicts = False
                for oid in self._call('vote', id(txn)) or ():
                    if isinstance(oid, dict):
                        # Conflict, let's try to resolve it
                        conflicts = True
                        conflict = oid
                        oid = conflict['oid']
                        committed, read = conflict['serials']
                        data = self.tryToResolveConflict(
                            oid, committed, read, conflict['data'])
                        self._async('storea', oid, committed, data, id(txn))
                        tbuf.resolve(oid, data)
                    else:
                        tbuf.server_resolve(oid)

                vote_attempts += 1

        except POSException.StorageTransactionError:
            # Hm, we got disconnected and reconnected bwtween
            # _check_trans and voting. Let's chack the transaction again:
            self._check_trans(txn, 'tpc_vote')
            raise

        except POSException.ConflictError as err:
            oid = getattr(err, 'oid', None)
            if oid is not None:
                # This is a band-aid to help recover from a situation
                # that shouldn't happen.  A Client somehow misses some
                # invalidations and has out of date data in its
                # cache. We need some whay to invalidate the cache
                # entry without invalidations. So, if we see a
                # (unresolved) conflict error, we assume that the
                # cache entry is bad and invalidate it.
                self._cache.invalidate(oid, None)
            raise

        if tbuf.exception:
            raise tbuf.exception

        if tbuf.server_resolved or tbuf.client_resolved:
            return list(tbuf.server_resolved) + list(tbuf.client_resolved)
        else:
            return None

    def tpc_transaction(self):
        return self._transaction

    def tpc_begin(self, txn, tid=None, status=' '):
        """Storage API: begin a transaction."""
        if self._is_read_only:
            raise POSException.ReadOnlyError()

        try:
            tbuf = txn.data(self)
        except AttributeError:
            # Gaaaa. This is a recovery transaction. Work around this
            # until we can think of something better. XXX
            tb = {}
            txn.data = tb.__getitem__
            txn.set_data = tb.__setitem__
        except KeyError:
            pass
        else:
            if tbuf is not None:
                raise POSException.StorageTransactionError(
                    "Duplicate tpc_begin calls for same transaction")

        txn.set_data(self, TransactionBuffer(self._connection_generation))

        # XXX we'd like to allow multiple transactions at a time at some point,
        # but for now, due to server limitations, TCBOO.
        self._commit_lock.acquire()
        self._tbuf = txn.data(self)

        try:
            self._async(
                'tpc_begin', id(txn),
                txn.user, txn.description, txn.extension, tid, status)
        except ClientDisconnected:
            self.tpc_end(txn)
            raise

    def tpc_end(self, txn):
        tbuf = txn.data(self)
        if tbuf is not None:
            tbuf.close()
            txn.set_data(self, None)
            self._commit_lock.release()

    def lastTransaction(self):
        return self._cache.getLastTid()

    def tpc_abort(self, txn, timeout=None):
        """Storage API: abort a transaction.

        (The timeout keyword argument is for tests to wait longer than
        they normally would.)
        """
        try:
            tbuf = txn.data(self)  # NOQA: F841 unused variable
        except KeyError:
            return

        try:
            # Caution:  Are there any exceptions that should prevent an
            # abort from occurring?  It seems wrong to swallow them
            # all, yet you want to be sure that other abort logic is
            # executed regardless.
            try:
                # It's tempting to make an asynchronous call here, but
                # it's useful for it to be synchronous because, if we
                # failed due to a disconnect, synchronous calls will
                # wait a little while in hopes of reconnecting.  If
                # we're able to reconnect and retry the transaction,
                # ten it might succeed!
                self._call('tpc_abort', id(txn), timeout=timeout)
            except ClientDisconnected:
                logger.debug("%s ClientDisconnected in tpc_abort() ignored",
                             self.__name__)
        finally:
            self._iterator_gc()
            self.tpc_end(txn)

    def tpc_finish(self, txn, f=lambda tid: None):
        """Storage API: finish a transaction."""
        tbuf = self._check_trans(txn, 'tpc_finish')

        try:
            tid = self._server.tpc_finish(id(txn), tbuf, f)
        finally:
            self.tpc_end(txn)
            self._iterator_gc()

        self._update_blob_cache(tbuf, tid)

        return tid

    def _update_blob_cache(self, tbuf, tid):
        """Internal helper move blobs updated by a transaction to the cache.
        """

        # Not sure why _update_cache() would be called on a closed storage.
        if self._cache is None:
            return

        if self.fshelper is not None:
            blobs = tbuf.blobs
            had_blobs = False
            while blobs:
                oid, blobfilename = blobs.pop()
                self._blob_data_bytes_loaded += os.stat(blobfilename).st_size
                self.fshelper.getPathForOID(oid, create=True)
                target_blob_file_name = self.fshelper.getBlobFilename(oid, tid)
                lock = _lock_blob(target_blob_file_name)
                try:
                    ZODB.blob.rename_or_copy_blob(
                        blobfilename,
                        target_blob_file_name,
                        )
                finally:
                    lock.close()
                had_blobs = True

            if had_blobs:
                self._check_blob_size(self._blob_data_bytes_loaded)

    def undo(self, trans_id, txn):
        """Storage API: undo a transaction.

        This is executed in a transactional context.  It has no effect
        until the transaction is committed.  It can be undone itself.

        Zope uses this to implement undo unless it is not supported by
        a storage.

        """
        self._check_trans(txn, 'undo')
        self._async('undoa', trans_id, id(txn))

    def undoInfo(self, first=0, last=-20, specification=None):
        """Storage API: return undo information."""
        return self._call('undoInfo', first, last, specification)

    def undoLog(self, first=0, last=-20, filter=None):
        """Storage API: return a sequence of TransactionDescription objects.

        The filter argument should be None or left unspecified, since
        it is impossible to pass the filter function to the server to
        be executed there.  If filter is not None, an empty sequence
        is returned.

        """
        if filter is not None:
            return []
        return self._call('undoLog', first, last)

    # Recovery support

    def copyTransactionsFrom(self, other, verbose=0):
        """Copy transactions from another storage.

        This is typically used for converting data from one storage to
        another.  `other` must have an .iterator() method.
        """
        ZODB.BaseStorage.copy(other, self, verbose)

    def restore(self, oid, serial, data, version, prev_txn, transaction):
        """Write data already committed in a separate database."""
        assert not version
        self._check_trans(transaction, 'restore')
        self._async('restorea', oid, serial, data, prev_txn, id(transaction))

    # Below are methods invoked by the StorageServer

    def info(self, dict):
        """Server callback to update the info dictionary."""
        self._info.update(dict)

    def invalidateCache(self):
        if self._db is not None:
            self._db.invalidateCache()

    def invalidateTransaction(self, tid, oids):
        """Server callback: Invalidate objects modified by tid."""
        if self._db is not None:
            self._db.invalidate(tid, oids)

    # IStorageIteration

    def iterator(self, start=None, stop=None):
        """Return an IStorageTransactionInformation iterator."""
        # iids are "iterator IDs" that can be used to query an iterator whose
        # status is held on the server.
        iid = self._call('iterator_start', start, stop)
        return self._setup_iterator(TransactionIterator, iid)

    def _setup_iterator(self, factory, iid, *args):
        self._iterators[iid] = iterator = factory(self, iid, *args)
        self._iterator_ids.add(iid)
        return iterator

    def _forget_iterator(self, iid):
        self._iterators.pop(iid, None)
        self._iterator_ids.remove(iid)

    def _iterator_gc(self, disconnected=False):
        if not self._iterator_ids:
            return

        if disconnected:
            for i in self._iterators.values():
                i._iid = -1
            self._iterators.clear()
            self._iterator_ids.clear()
            return

        # Recall that self._iterators is a WeakValueDictionary. Under
        # non-refcounted implementations like PyPy, this means that
        # unreachable iterators (and their IDs) may still be in this
        # map for some arbitrary period of time (until the next
        # garbage collection occurs.) This is fine: the server
        # supports being asked to GC the same iterator ID more than
        # once. Iterator ids can be reused, but only after a server
        # restart, after which we had already been called with
        # `disconnected` True and so had cleared out our map anyway,
        # plus we simply replace whatever is in the map if we get a
        # duplicate id---and duplicates at that point would be dead
        # objects waiting to be cleaned up. So there's never any risk
        # of confusing TransactionIterator objects that are in use.
        iids = self._iterator_ids - set(self._iterators)
        # let tests know we've been called:
        self._iterators._last_gc = time.time()
        if iids:
            try:
                self._async('iterator_gc', list(iids))
            except ClientDisconnected:
                # If we get disconnected, all of the iterators on the
                # server are thrown away.  We should clear ours too:
                return self._iterator_gc(True)
            self._iterator_ids -= iids

    def server_status(self, timeout=None):
        """Retrieve status dictionary from the server.

        (The timeout keyword argument is for tests)
        """
        return self._call('server_status', timeout=timeout)


class TransactionIterator:

    def __init__(self, storage, iid, *args):
        self._storage = storage
        self._iid = iid
        self._ended = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._ended:
            raise StopIteration()

        if self._iid < 0:
            raise ClientDisconnected("Disconnected iterator")

        tx_data = self._storage._call('iterator_next', self._iid)
        if tx_data is None:
            # The iterator is exhausted, and the server has already
            # disposed it.
            self._ended = True
            self._storage._forget_iterator(self._iid)
            raise StopIteration()

        return ClientStorageTransactionInformation(
            self._storage, self, *tx_data)

    next = __next__


class ClientStorageTransactionInformation(ZODB.BaseStorage.TransactionRecord):

    def __init__(self, storage, txiter, tid, status, user, description,
                 extension):
        self._storage = storage
        self._txiter = txiter
        self._completed = False
        self._riid = None

        self.tid = tid
        self.status = status
        self.user = user
        self.description = description
        self.extension = extension

    def __iter__(self):
        riid = self._storage._call('iterator_record_start',
                                   self._txiter._iid, self.tid)
        return self._storage._setup_iterator(RecordIterator, riid)


class RecordIterator:

    def __init__(self, storage, riid):
        self._riid = riid
        self._completed = False
        self._storage = storage

    def __iter__(self):
        return self

    def __next__(self):
        if self._completed:
            # We finished iteration once already and the server can't know
            # about the iteration anymore.
            raise StopIteration()
        item = self._storage._call('iterator_record_next', self._riid)
        if item is None:
            # The iterator is exhausted, and the server has already
            # disposed it.
            self._completed = True
            raise StopIteration()
        return ZODB.BaseStorage.DataRecord(*item)

    next = __next__


class BlobCacheLayout:

    size = 997

    def oid_to_path(self, oid):
        return str(utils.u64(oid) % self.size)

    def getBlobFilePath(self, oid, tid):
        base, rem = divmod(utils.u64(oid), self.size)
        return os.path.join(
            str(rem),
            "{}.{}{}".format(base, hexlify(tid).decode('ascii'),
                         ZODB.blob.BLOB_SUFFIX)
            )


def _accessed(filename):
    try:
        os.utime(filename, (time.time(), os.stat(filename).st_mtime))
    except OSError:
        pass  # We tried. :)
    return filename


cache_file_name = re.compile(r'\d+$').match


def _check_blob_cache_size(blob_dir, target):

    logger = logging.getLogger(__name__+'.check_blob_cache')

    with open(os.path.join(blob_dir, ZODB.blob.LAYOUT_MARKER)) as layout_file:
        layout = layout_file.read().strip()
    if not layout == 'zeocache':
        logger.critical("Invalid blob directory layout %s", layout)
        raise ValueError("Invalid blob directory layout", layout)

    attempt_path = os.path.join(blob_dir, 'check_size.attempt')

    try:
        check_lock = zc.lockfile.LockFile(
            os.path.join(blob_dir, 'check_size.lock'))
    except zc.lockfile.LockError:
        try:
            time.sleep(1)
            check_lock = zc.lockfile.LockFile(
                os.path.join(blob_dir, 'check_size.lock'))
        except zc.lockfile.LockError:
            # Someone is already cleaning up, so don't bother
            logger.debug("%s Another thread is checking the blob cache size.",
                         get_ident())
            open(attempt_path, 'w').close()  # Mark that we tried
            return

    logger.debug("%s Checking blob cache size. (target: %s)",
                 get_ident(), target)

    try:
        while 1:
            size = 0
            blob_suffix = ZODB.blob.BLOB_SUFFIX
            files_by_atime = BTrees.OOBTree.BTree()

            for dirname in os.listdir(blob_dir):
                if not cache_file_name(dirname):
                    continue
                base = os.path.join(blob_dir, dirname)
                if not os.path.isdir(base):
                    continue
                for file_name in os.listdir(base):
                    if not file_name.endswith(blob_suffix):
                        continue
                    file_path = os.path.join(base, file_name)
                    if not os.path.isfile(file_path):
                        continue
                    stat = os.stat(file_path)
                    size += stat.st_size
                    t = stat.st_atime
                    if t not in files_by_atime:
                        files_by_atime[t] = []
                    files_by_atime[t].append(os.path.join(dirname, file_name))

            logger.debug("%s   blob cache size: %s", get_ident(), size)

            if size <= target:
                if os.path.isfile(attempt_path):
                    try:
                        os.remove(attempt_path)
                    except OSError:
                        pass  # Sigh, windows
                    continue
                logger.debug("%s   -->", get_ident())
                break

            while size > target and files_by_atime:
                for file_name in files_by_atime.pop(files_by_atime.minKey()):
                    file_name = os.path.join(blob_dir, file_name)
                    lockfilename = os.path.join(os.path.dirname(file_name),
                                                '.lock')
                    try:
                        lock = zc.lockfile.LockFile(lockfilename)
                    except zc.lockfile.LockError:
                        logger.debug("%s Skipping locked %s",
                                     get_ident(),
                                     os.path.basename(file_name))
                        continue  # In use, skip

                    try:
                        fsize = os.stat(file_name).st_size
                        try:
                            ZODB.blob.remove_committed(file_name)
                        except OSError:
                            pass  # probably open on windows
                        else:
                            size -= fsize
                    finally:
                        lock.close()

                    if size <= target:
                        break

            logger.debug("%s   reduced blob cache size: %s",
                         get_ident(), size)

    finally:
        check_lock.close()


def check_blob_size_script(args=None):
    if args is None:
        args = sys.argv[1:]
    blob_dir, target = args
    _check_blob_cache_size(blob_dir, int(target))


class _FileLock:
    """Auxiliary class to provide for file lock logging."""
    def __init__(self, filename, log_failure):
        self.filename = filename
        try:
            self.lock = zc.lockfile.LockFile(filename)
            logger.debug("locked %s", filename)
        except zc.lockfile.LockError:
            if log_failure:
                logger.debug("failed to lock %s", filename)
            raise

    def close(self):
        self.lock.close()
        logger.debug("unlocked %s", self.filename)


def _lock_blob(path):
    lockfilename = os.path.join(os.path.dirname(path), '.lock')
    n = 0
    while 1:
        try:
            return _FileLock(lockfilename, n == 0)
        except zc.lockfile.LockError:
            time.sleep(0.01)
            n += 1
            if n > 60000:
                raise
        else:
            break


def open_cache(cache, var, client, storage, cache_size):
    if isinstance(cache, (None.__class__, str)):
        from ZEO.cache import ClientCache
        if cache is None:
            if client:
                cache = os.path.join(var or os.getcwd(),
                                     f'{client}-{storage}.zec')
            else:
                # ephemeral cache
                return ClientCache(None, cache_size)

        cache = ClientCache(cache, cache_size)

    return cache
