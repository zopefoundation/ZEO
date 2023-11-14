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

import ZODB.MappingStorage

import ZEO.asyncio.tests
import ZEO.StorageServer


class StorageServer(ZEO.StorageServer.StorageServer):
    def __init__(self, addr='test_addr', storages=None, **kw):
        if storages is None:
            storages = {'1': ZODB.MappingStorage.MappingStorage()}
        ZEO.StorageServer.StorageServer.__init__(self, addr, storages, **kw)

    def close(self):
        if self.__closed:
            return
        # instances are typically not run in their own thread
        # therefore, the loop usually does not run and the
        # normal ``close`` does not work.
        loop = self.get_loop()
        if loop.is_running():
            return super().close()
        loop.call_soon_threadsafe(super().close)
        loop.run_forever()  # will stop automatically
        loop.close()

    def get_loop(self):
        return self.acceptor.event_loop  # might not work for ``MTAcceptor``


def client(server, name='client'):
    zs = ZEO.StorageServer.ZEOStorage(server)
    protocol = ZEO.asyncio.tests.server_protocol(
        False, zs, protocol_version=b'Z5', addr='test-addr-%s' % name)

    # ``server_protocol`` uses its own testing loop (not
    # that of *server*). As a consequence, ``protocol.close``
    # does not work correctly.
    # In addition, the artificial loop needs to get closed.
    pr_close = protocol.close

    def close(*args, **kw):
        pr_close()
        zs.notify_disconnected()
        protocol.loop.close()
        del protocol.close  # break reference cycle

    protocol.close = close  # install proper closing
    zs.notify_connected(protocol)
    zs.register('1', 0)
    return zs
