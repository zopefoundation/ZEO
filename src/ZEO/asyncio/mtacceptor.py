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
"""Multi-threaded server connectin acceptor

Each connection is run in it's own thread. Testing serveral years ago
suggsted that this was a win, but ZODB shootout and another
lower-level tests suggest otherwise.  It's really unclear, which is
why we're keeping this around for now.

Asyncio doesn't let you accept connections in one thread and handle
them in another.  To get around this, we have a listener implemented
using asyncore, but when we get a connection, we hand the socket to
asyncio.  This worked well until we added SSL support.  (Even then, it
worked on Mac OS X for some reason.)

SSL + non-blocking sockets requires special care, which asyncio
provides.  Unfortunately, create_connection, assumes it's creating a
client connection. It would be easy to fix this,
http://bugs.python.org/issue27392, but it's hard to justify the fix to
get it accepted, so we won't bother for now.  This currently uses a
horrible monley patch to work with SSL.

To use this module, replace::

  from .asyncio.server import Acceptor

with::

  from .asyncio.mtacceptor import Acceptor

in ZEO.StorageServer.
"""
import asyncore
import socket
import threading
import time

from .compat import asyncio, new_event_loop
from .server import ServerProtocol

# _has_dualstack: True if the dual-stack sockets are supported
try:
    # Check whether IPv6 sockets can be created
    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
except (socket.error, AttributeError):
    _has_dualstack = False
else:
    # Check whether enabling dualstack (disabling v6only) works
    try:
        s.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, False)
    except (socket.error, AttributeError):
        _has_dualstack = False
    else:
        _has_dualstack = True
    s.close()
    del s

import logging

logger = logging.getLogger(__name__)

class Acceptor(asyncore.dispatcher):
    """A server that accepts incoming RPC connections

    And creates a separate thread for each.
    """

    def __init__(self, storage_server, addr, ssl, msgpack):
        self.storage_server = storage_server
        self.addr = addr
        self.__socket_map = {}
        asyncore.dispatcher.__init__(self, map=self.__socket_map)

        self.ssl_context = ssl
        self.msgpack = msgpack
        self._open_socket()

    def _open_socket(self):
        addr = self.addr

        if type(addr) == tuple:
            if addr[0] == '' and _has_dualstack:
                # Wildcard listen on all interfaces, both IPv4 and
                # IPv6 if possible
                self.create_socket(socket.AF_INET6, socket.SOCK_STREAM)
                self.socket.setsockopt(
                    socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, False)
            elif ':' in addr[0]:
                self.create_socket(socket.AF_INET6, socket.SOCK_STREAM)
                if _has_dualstack:
                    # On Linux, IPV6_V6ONLY is off by default.
                    # If the user explicitly asked for IPv6, don't bind to IPv4
                    self.socket.setsockopt(
                        socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, True)
            else:
                self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)

        self.set_reuse_addr()

        for i in range(25):
            try:
                self.bind(addr)
            except Exception as exc:
                logger.info("bind on %s failed %s waiting", addr, i)
                if i == 24:
                    raise
                else:
                    time.sleep(5)
            except:
                logger.exception('binding')
                raise
            else:
                break

        if isinstance(addr, tuple) and addr[1] == 0:
            self.addr = addr = self.socket.getsockname()[:2]

        logger.info("listening on %s", str(addr))
        self.listen(5)

    def writable(self):
        return 0

    def readable(self):
        return 1

    def handle_accept(self):
        try:
            sock, addr = self.accept()
        except socket.error as msg:
            logger.info("accepted failed: %s", msg)
            return


        # We could short-circuit the attempt below in some edge cases
        # and avoid a log message by checking for addr being None.
        # Unfortunately, our test for the code below,
        # quick_close_doesnt_kill_server, causes addr to be None and
        # we'd have to write a test for the non-None case, which is
        # *even* harder to provoke. :/ So we'll leave things as they
        # are for now.

        # It might be better to check whether the socket has been
        # closed, but I don't see a way to do that. :(

        # Drop flow-info from IPv6 addresses
        if addr: # Sometimes None on Mac. See above.
            addr = addr[:2]

        try:
            logger.debug("new connection %s" % (addr,))

            def run():
                loop = new_event_loop()
                zs = self.storage_server.create_client_handler()
                protocol = ServerProtocol(loop, self.addr, zs, self.msgpack)
                protocol.stop = loop.stop

                if self.ssl_context is None:
                    cr = loop.create_connection((lambda : protocol), sock=sock)
                else:
                    if hasattr(loop, 'connect_accepted_socket'):
                        cr = loop.connect_accepted_socket(
                            (lambda : protocol), sock, ssl=self.ssl_context)
                    else:
                        #######################################################
                        # XXX See http://bugs.python.org/issue27392 :(
                        _make_ssl_transport = loop._make_ssl_transport
                        def make_ssl_transport(*a, **kw):
                            kw['server_side'] = True
                            return _make_ssl_transport(*a, **kw)
                        loop._make_ssl_transport = make_ssl_transport
                        #
                        #######################################################
                        cr = loop.create_connection(
                            (lambda : protocol), sock=sock,
                            ssl=self.ssl_context,
                            server_hostname=''
                            )

                asyncio.async(cr, loop=loop)
                loop.run_forever()
                loop.close()

            thread = threading.Thread(target=run, name='zeo_client_hander')
            thread.setDaemon(True)
            thread.start()
        except Exception:
            if sock.fileno() in self.__socket_map:
                del self.__socket_map[sock.fileno()]
            logger.exception("Error in handle_accept")
        else:
            logger.info("connect from %s", repr(addr))

    def loop(self, timeout=30.0):
        try:
            asyncore.loop(map=self.__socket_map, timeout=timeout)
        except Exception:
            if not self.__closed:
                raise # Unexpected exc

        logger.debug('acceptor %s loop stopped', self.addr)

    __closed = False
    def close(self):
        if not self.__closed:
            self.__closed = True
            asyncore.dispatcher.close(self)
            logger.debug("Closed accepter, %s", len(self.__socket_map))
