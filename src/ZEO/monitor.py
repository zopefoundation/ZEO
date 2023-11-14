##############################################################################
#
# Copyright (c) 2003 Zope Foundation and Contributors.
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
"""Monitor behavior of ZEO server and record statistics.
"""

import time


zeo_version = 'unknown'
try:
    import pkg_resources
except ImportError:
    pass
else:
    zeo_dist = pkg_resources.working_set.find(
        pkg_resources.Requirement.parse('ZODB3')
        )
    if zeo_dist is not None:
        zeo_version = zeo_dist.version


class StorageStats:
    """Per-storage usage statistics."""

    def __init__(self, connections=None):
        self.connections = connections
        self.loads = 0
        self.stores = 0
        self.commits = 0
        self.aborts = 0
        self.active_txns = 0
        self.lock_time = None
        self.conflicts = 0
        self.conflicts_resolved = 0
        self.start = time.ctime()

    @property
    def clients(self):
        return len(self.connections)

    def parse(self, s):
        # parse the dump format
        lines = s.split("\n")
        for line in lines:
            field, value = line.split(":", 1)
            if field == "Server started":
                self.start = value
            elif field == "Clients":
                # Hack because we use this both on the server and on
                # the client where there are no connections.
                self.connections = [0] * int(value)
            elif field == "Clients verifying":
                self.verifying_clients = int(value)
            elif field == "Active transactions":
                self.active_txns = int(value)
            elif field == "Commit lock held for":
                # This assumes
                self.lock_time = time.time() - int(value)
            elif field == "Commits":
                self.commits = int(value)
            elif field == "Aborts":
                self.aborts = int(value)
            elif field == "Loads":
                self.loads = int(value)
            elif field == "Stores":
                self.stores = int(value)
            elif field == "Conflicts":
                self.conflicts = int(value)
            elif field == "Conflicts resolved":
                self.conflicts_resolved = int(value)

    def dump(self, f):
        print("Server started:", self.start, file=f)
        print("Clients:", self.clients, file=f)
        print("Clients verifying:", self.verifying_clients, file=f)
        print("Active transactions:", self.active_txns, file=f)
        if self.lock_time:
            howlong = time.time() - self.lock_time
            print("Commit lock held for:", int(howlong), file=f)
        print("Commits:", self.commits, file=f)
        print("Aborts:", self.aborts, file=f)
        print("Loads:", self.loads, file=f)
        print("Stores:", self.stores, file=f)
        print("Conflicts:", self.conflicts, file=f)
        print("Conflicts resolved:", self.conflicts_resolved, file=f)
