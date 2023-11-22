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
"""A TransactionBuffer store transaction updates until commit or abort.

A transaction may generate enough data that it is not practical to
always hold pending updates in memory.  Instead, a TransactionBuffer
is used to store the data until a commit or abort.
"""

# A faster implementation might store trans data in memory until it
# reaches a certain size.

import tempfile

from zodbpickle.pickle import Pickler
from zodbpickle.pickle import Unpickler


class TransactionBuffer:

    # The TransactionBuffer is used by client storage to hold update
    # data until the tpc_finish().  It is only used by a single
    # thread, because only one thread can be in the two-phase commit
    # at one time.

    def __init__(self, connection_generation):
        self.connection_generation = connection_generation
        self.file = tempfile.TemporaryFile(suffix=".tbuf")
        self.count = 0
        self.size = 0
        self.blobs = []
        # It's safe to use a fast pickler because the only objects
        # stored are builtin types -- strings or None.
        self.pickler = Pickler(self.file, 1)
        self.pickler.fast = 1
        self.server_resolved = set()  # {oid}
        self.client_resolved = {}  # {oid -> buffer_record_number}
        self.exception = None

    def close(self):
        self.file.close()

    def store(self, oid, data):
        """Store oid, version, data for later retrieval"""
        self.pickler.dump((oid, data))
        self.count += 1
        # Estimate per-record cache size
        self.size = self.size + (data and len(data) or 0) + 31

    def resolve(self, oid, data):
        """Record client-resolved data
        """
        self.store(oid, data)
        self.client_resolved[oid] = self.count - 1

    def server_resolve(self, oid):
        self.server_resolved.add(oid)

    def storeBlob(self, oid, blobfilename):
        self.blobs.append((oid, blobfilename))

    def __iter__(self):
        self.file.seek(0)
        unpickler = Unpickler(self.file)
        server_resolved = self.server_resolved
        client_resolved = self.client_resolved

        # Gaaaa, this is awkward. There can be entries in serials that
        # aren't in the buffer, because undo.  Entries can be repeated
        # in the buffer, because ZODB. (Maybe this is a bug now, but
        # it may be a feature later.

        seen = set()
        for i in range(self.count):
            oid, data = unpickler.load()
            if client_resolved.get(oid, i) == i:
                seen.add(oid)
                yield oid, data, oid in server_resolved

        # We may have leftover oids because undo
        for oid in server_resolved:
            if oid not in seen:
                yield oid, None, True
