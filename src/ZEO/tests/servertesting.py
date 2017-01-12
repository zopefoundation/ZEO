from __future__ import print_function
from __future__ import print_function
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

# Testing the current ZEO implementation is rather hard due to the
# architecture, which mixes concerns, especially between application
# and networking.  Still, it's not as bad as it could be.

# The 2 most important classes in the architecture are ZEOStorage and
# StorageServer. A ZEOStorage is created for each client connection.
# The StorageServer maintains data shared or needed for coordination
# among clients.

# The other important part of the architecture is connections.
# Connections are used by ZEOStorages to send messages or return data
# to clients.

# Here, we'll try to provide some testing infrastructure to isolate
# servers from the network.

import ZEO.asyncio.tests
import ZEO.StorageServer
import ZODB.MappingStorage

class StorageServer(ZEO.StorageServer.StorageServer):

    def __init__(self, addr='test_addr', storages=None, **kw):
        if storages is None:
            storages = {'1': ZODB.MappingStorage.MappingStorage()}
        ZEO.StorageServer.StorageServer.__init__(self, addr, storages, **kw)

def client(server, name='client'):
    zs = ZEO.StorageServer.ZEOStorage(server)
    protocol = ZEO.asyncio.tests.server_protocol(
        False, zs, protocol_version=b'Z5', addr='test-addr-%s' % name)
    zs.notify_connected(protocol)
    zs.register('1', 0)
    return zs
