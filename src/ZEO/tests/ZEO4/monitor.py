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
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function

import asyncore
import socket
import time
import logging

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
        self.verifying_clients = 0
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

class StatsClient(asyncore.dispatcher):

    def __init__(self, sock, addr):
        asyncore.dispatcher.__init__(self, sock)
        self.buf = []
        self.closed = 0

    def close(self):
        self.closed = 1
        # The socket is closed after all the data is written.
        # See handle_write().

    def write(self, s):
        self.buf.append(s)

    def writable(self):
        return len(self.buf)

    def readable(self):
        return 0

    def handle_write(self):
        s = "".join(self.buf)
        self.buf = []
        n = self.socket.send(s.encode('ascii'))
        if n < len(s):
            self.buf.append(s[:n])

        if self.closed and not self.buf:
            asyncore.dispatcher.close(self)

class StatsServer(asyncore.dispatcher):

    StatsConnectionClass = StatsClient

    def __init__(self, addr, stats):
        asyncore.dispatcher.__init__(self)
        self.addr = addr
        self.stats = stats
        if type(self.addr) == tuple:
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.set_reuse_addr()
        logger = logging.getLogger('ZEO.monitor')
        logger.info("listening on %s", repr(self.addr))
        self.bind(self.addr)
        self.listen(5)

    def writable(self):
        return 0

    def readable(self):
        return 1

    def handle_accept(self):
        try:
            sock, addr = self.accept()
        except socket.error:
            return
        f = self.StatsConnectionClass(sock, addr)
        self.dump(f)
        f.close()

    def dump(self, f):
        print("ZEO monitor server version %s" % zeo_version, file=f)
        print(time.ctime(), file=f)
        print(file=f)

        L = sorted(self.stats.keys())
        for k in L:
            stats = self.stats[k]
            print("Storage:", k, file=f)
            stats.dump(f)
            print(file=f)
