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
import asyncore
import socket

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
    """A server that accepts incoming RPC connections"""

    def __init__(self, addr, factory, ssl=None):
        self.__socket_map = {}
        asyncore.dispatcher.__init__(self, map=self.__socket_map)
        self.addr = addr

        self.__ssl = ssl
        if ssl is not None:
            from ssl import SSLError

            wrap_socket = ssl.wrap_socket
            def ssl_factory(sock, addr):
                try:
                    conn = wrap_socket(sock, server_side=True)
                except SSLError:
                    logger.debug("SSL failure", exc_info=True)
                else:
                    return factory(conn, addr)

            self.__factory = ssl_factory
        else:
            self.__factory = factory

        self._open_socket()

    __ssl_context = None
    def __ssl_wrap_socket(self, sock):
        return sock

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
            self.addr = addr = self.socket.getsockname()

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

        sock = self.__ssl_wrap_socket(sock)

        try:
            c = self.__factory(sock, addr)
        except Exception:
            if sock.fileno() in self.__socket_map:
                del self.__socket_map[sock.fileno()]
            logger.exception("Error in handle_accept")
        else:
            logger.info("connect from %s: %s", repr(addr), c)

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
