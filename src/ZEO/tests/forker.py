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
from __future__ import print_function
import gc
import os
import random
import sys
import time
import errno
import multiprocessing
import socket
import subprocess
import logging
import tempfile
import six
from six.moves.queue import Empty
import ZODB.tests.util
import zope.testing.setupstack
from ZEO._compat import StringIO

logger = logging.getLogger('ZEO.tests.forker')

DEBUG = os.environ.get('ZEO_TEST_SERVER_DEBUG')

ZEO4_SERVER = os.environ.get('ZEO4_SERVER')
skip_if_testing_client_against_zeo4 = (
    (lambda func: None)
    if ZEO4_SERVER else
    (lambda func: func)
    )

class ZEOConfig:
    """Class to generate ZEO configuration file. """

    def __init__(self, addr, log=None, **options):
        if log:
            if isinstance(log, str):
                self.logpath = log
            elif isinstance(addr, str):
                self.logpath = addr+'.log'
            else:
                self.logpath = 'server.log'

        if not isinstance(addr, str):
            addr = '%s:%s' % addr

        self.log = log
        self.address = addr
        self.read_only = None
        self.loglevel = 'INFO'
        self.__dict__.update(options)

    def dump(self, f):
        print("<zeo>", file=f)
        print("address " + self.address, file=f)
        if self.read_only is not None:
            print("read-only", self.read_only and "true" or "false", file=f)

        for name in (
            'invalidation_queue_size', 'invalidation_age',
            'transaction_timeout', 'pid_filename', 'msgpack',
            'ssl_certificate', 'ssl_key', 'client_conflict_resolution',
            ):
            v = getattr(self, name, None)
            if v:
                print(name.replace('_', '-'), v, file=f)

        print("</zeo>", file=f)

        if self.log:
            print("""
            <eventlog>
              level %s
              <logfile>
                 path %s
              </logfile>
            </eventlog>
            """ % (self.loglevel, self.logpath), file=f)

    def __str__(self):
        f = StringIO()
        self.dump(f)
        return f.getvalue()


def encode_format(fmt):
    # The list of replacements mirrors
    # ZConfig.components.logger.handlers._control_char_rewrites
    for xform in (("\n", r"\n"), ("\t", r"\t"), ("\b", r"\b"),
                  ("\f", r"\f"), ("\r", r"\r")):
        fmt = fmt.replace(*xform)
    return fmt

def runner(config, qin, qout, timeout=None,
           debug=False, name=None,
           keep=False, protocol=None):

    if debug or DEBUG:
        debug_logging()

    old_protocol = None
    if protocol:
        import ZEO.asyncio.server
        old_protocol = ZEO.asyncio.server.best_protocol_version
        ZEO.asyncio.server.best_protocol_version = protocol
        old_protocols = ZEO.asyncio.server.ServerProtocol.protocols
        ZEO.asyncio.server.ServerProtocol.protocols = tuple(sorted(
            set(old_protocols) | set([protocol])
            ))

    try:
        import threading

        if ZEO4_SERVER:
            from .ZEO4 import runzeo
        else:
            from .. import runzeo

        options = runzeo.ZEOOptions()
        options.realize(['-C', config])
        server = runzeo.ZEOServer(options)
        globals()[(name if name else 'last') + '_server'] = server
        server.open_storages()
        server.clear_socket()
        server.create_server()
        logger.debug('SERVER CREATED')
        if ZEO4_SERVER:
            qout.put(server.server.addr)
        else:
            qout.put(server.server.acceptor.addr)
        logger.debug('ADDRESS SENT')
        thread = threading.Thread(
            target=server.server.loop, kwargs=dict(timeout=.2),
            name = None if name is None else name + '-server',
            )
        thread.setDaemon(True)
        thread.start()
        os.remove(config)

        try:
            qin.get(timeout=timeout) # wait for shutdown
        except Empty:
            pass
        server.server.close()
        thread.join(3)

        if not keep:
            # Try to cleanup storage files
            for storage in server.server.storages.values():
                try:
                    storage.cleanup()
                except AttributeError:
                    pass

        qout.put(thread.is_alive())

    except Exception:
        logger.exception("In server thread")

    finally:
        if old_protocol:
            ZEO.asyncio.server.best_protocol_version = old_protocol
            ZEO.asyncio.server.ServerProtocol.protocols = old_protocols

def stop_runner(thread, config, qin, qout, stop_timeout=19, pid=None):
    qin.put('stop')
    try:
        dirty = qout.get(timeout=stop_timeout)
    except Empty:
        print("WARNING Couldn't stop server", file=sys.stderr)
        if hasattr(thread, 'terminate'):
            thread.terminate()
            os.waitpid(thread.pid, 0)
    else:
        if dirty:
            print("WARNING SERVER DIDN'T STOP CLEANLY", file=sys.stderr)

            # The runner thread didn't stop. If it was a process,
            # give it some time to exit
            if hasattr(thread, 'pid') and thread.pid:
                os.waitpid(thread.pid, 0)

    thread.join(stop_timeout)

    gc.collect()

def start_zeo_server(storage_conf=None, zeo_conf=None, port=None, keep=False,
                     path='Data.fs', protocol=None, blob_dir=None,
                     suicide=True, debug=False,
                     threaded=False, start_timeout=33, name=None, log=None,
                     show_config=False
                     ):
    """Start a ZEO server in a separate process.

    Takes two positional arguments a string containing the storage conf
    and a ZEOConfig object.

    Returns the ZEO address, the test server address, the pid, and the path
    to the config file.
    """

    if not storage_conf:
        storage_conf = '<filestorage>\npath %s\n</filestorage>' % path

    if blob_dir:
        storage_conf = '<blobstorage>\nblob-dir %s\n%s\n</blobstorage>' % (
            blob_dir, storage_conf)

    if zeo_conf is None or isinstance(zeo_conf, dict):
        if port is None:
            port = 0

        if isinstance(port, int):
            addr = '127.0.0.1', port
        else:
            addr = port

        z = ZEOConfig(addr, log=log)
        if zeo_conf:
            z.__dict__.update(zeo_conf)
        zeo_conf = str(z)

    zeo_conf = str(zeo_conf) + '\n\n' + storage_conf
    if show_config:
        print(zeo_conf)

    # Store the config info in a temp file.
    tmpfile = tempfile.mktemp(".conf", dir=os.getcwd())
    fp = open(tmpfile, 'w')
    fp.write(zeo_conf)
    fp.close()

    if threaded:
        from threading import Thread
        from six.moves.queue import Queue
    else:
        from multiprocessing import Process as Thread
        Queue = ThreadlessQueue

    qin = Queue()
    qout = Queue()
    thread = Thread(
        target=runner,
        args=[tmpfile, qin, qout, 999 if suicide else None],
        kwargs=dict(debug=debug, name=name, protocol=protocol, keep=keep),
        name = None if name is None else name + '-server-runner',
        )
    thread.daemon = True
    thread.start()
    try:
        addr = qout.get(timeout=start_timeout)
    except Exception:
        whine("SERVER FAILED TO START")
        if thread.is_alive():
            whine("Server thread/process is still running")
        elif not threaded:
            whine("Exit status", thread.exitcode)
        raise

    def stop(stop_timeout=99):
        stop_runner(thread, tmpfile, qin, qout, stop_timeout)

    return addr, stop

if sys.platform[:3].lower() == "win":
    def _quote_arg(s):
        return '"%s"' % s
else:
    def _quote_arg(s):
        return s

def shutdown_zeo_server(stop):
    stop()

def get_port(ignored=None):
    """Return a port that is not in use.

    Checks if a port is in use by trying to connect to it.  Assumes it
    is not in use if connect raises an exception. We actually look for
    2 consective free ports because most of the clients of this
    function will use the returned port and the next one.

    Raises RuntimeError after 10 tries.
    """

    for i in range(10):
        port = random.randrange(20000, 30000)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            try:
                s.connect(('127.0.0.1', port))
            except socket.error:
                pass  # Perhaps we should check value of error too.
            else:
                continue

            try:
                s1.connect(('127.0.0.1', port+1))
            except socket.error:
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
        except socket.error:
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


def wait_until(label=None, func=None, timeout=30, onfail=None):
    if label is None:
        if func is not None:
            label = func.__name__
    elif not isinstance(label, six.string_types) and func is None:
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
               lambda : not storage.is_connected())

def debug_logging(logger='ZEO', stream='stderr', level=logging.DEBUG):
    handler = logging.StreamHandler(getattr(sys, stream))
    logger = logging.getLogger(logger)
    logger.addHandler(handler)
    logger.setLevel(level)

    def stop():
        logger.removeHandler(handler)
        logger.setLevel(logging.NOTSET)

    return stop

def whine(*message):
    print(*message, file=sys.stderr)
    sys.stderr.flush()

class ThreadlessQueue(object):

    def __init__(self):
        self.cin, self.cout = multiprocessing.Pipe(False)

    def put(self, v):
        self.cout.send(v)

    def get(self, timeout=None):
        if self.cin.poll(timeout):
            return self.cin.recv()
        else:
            raise Empty()
