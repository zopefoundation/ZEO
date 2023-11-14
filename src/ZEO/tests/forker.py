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
"""Library for forking storage server and connecting client storage"""


import logging
import random
import socket
import time

import ZODB.tests.util
import zope.testing.setupstack

from ZEO import _forker
from ZEO._compat import WIN


logger = logging.getLogger('ZEO.tests.forker')

DEBUG = _forker.DEBUG

ZEOConfig = _forker.ZEOConfig


def encode_format(fmt):
    # The list of replacements mirrors
    # ZConfig.components.logger.handlers._control_char_rewrites
    for xform in (("\n", r"\n"), ("\t", r"\t"), ("\b", r"\b"),
                  ("\f", r"\f"), ("\r", r"\r")):
        fmt = fmt.replace(*xform)
    return fmt


runner = _forker.runner
stop_runner = _forker.stop_runner
start_zeo_server = _forker.start_zeo_server

if WIN:
    def _quote_arg(s):
        return '"%s"' % s
else:
    def _quote_arg(s):
        return s

shutdown_zeo_server = _forker.shutdown_zeo_server


def get_port(ignored=None):
    """Return a port that is not in use.

    Checks if a port is in use by trying to connect to it.  Assumes it
    is not in use if connect raises an exception. We actually look for
    2 consective free ports because most of the clients of this
    function will use the returned port and the next one.

    Raises RuntimeError after 10 tries.
    """

    for _i in range(10):
        port = random.randrange(20000, 30000)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            try:
                s.connect(('127.0.0.1', port))
            except OSError:
                pass  # Perhaps we should check value of error too.
            else:
                continue

            try:
                s1.connect(('127.0.0.1', port+1))
            except OSError:
                pass  # Perhaps we should check value of error too.
            else:
                continue

            return port

        finally:
            s.close()
            s1.close()
    raise RuntimeError("Can't find port")


def can_connect(port):
    c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        try:
            c.connect(('127.0.0.1', port))
        except OSError:
            return False  # Perhaps we should check value of error too.
        else:
            return True
    finally:
        c.close()


def setUp(test):
    ZODB.tests.util.setUp(test)

    servers = []

    def start_server(storage_conf=None, zeo_conf=None, port=None, keep=False,
                     addr=None, path='Data.fs', protocol=None, blob_dir=None,
                     suicide=True, debug=False, **kw):
        """Start a ZEO server.

        Return the server and admin addresses.
        """
        if port is None:
            if addr is None:
                port = 0
            else:
                port = addr[1]
        elif addr is not None:
            raise TypeError("Can't specify port and addr")
        addr, stop = start_zeo_server(
            storage_conf=storage_conf,
            zeo_conf=zeo_conf,
            port=port,
            keep=keep,
            path=path,
            protocol=protocol,
            blob_dir=blob_dir,
            suicide=suicide,
            debug=debug,
            **kw)
        servers.append(stop)
        return addr, stop

    test.globs['start_server'] = start_server

    def stop_server(stop):
        stop()
        servers.remove(stop)

    test.globs['stop_server'] = stop_server

    def cleanup_servers():
        for stop in list(servers):
            stop()

    zope.testing.setupstack.register(test, cleanup_servers)

    test.globs['wait_until'] = wait_until
    test.globs['wait_connected'] = wait_connected
    test.globs['wait_disconnected'] = wait_disconnected


def wait_until(label=None, func=None, timeout=3, onfail=None):
    if label is None:
        if func is not None:
            label = func.__name__
    elif not isinstance(label, str) and func is None:
        func = label
        label = func.__name__

    if func is None:
        def wait_decorator(f):
            wait_until(label, f, timeout, onfail)

        return wait_decorator

    giveup = time.time() + timeout
    while not func():
        if time.time() > giveup:
            if onfail is None:
                raise AssertionError("Timed out waiting for: ", label)
            else:
                return onfail()
        time.sleep(0.01)


def wait_connected(storage):
    wait_until("storage is connected", storage.is_connected)


def wait_disconnected(storage):
    wait_until("storage is disconnected",
               lambda: not storage.is_connected())


debug_logging = _forker.debug_logging
whine = _forker.whine
ThreadlessQueue = _forker.ThreadlessQueue
