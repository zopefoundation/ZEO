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
import gc
import logging
import multiprocessing
import os
import os.path
import sys
import tempfile
from io import StringIO
from queue import Empty


logger = logging.getLogger('ZEO.tests.forker')
DEBUG = os.environ.get('ZEO_TEST_SERVER_DEBUG')


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
                'ssl_certificate', 'ssl_key', 'client_conflict_resolution'):
            v = getattr(self, name, None)
            if v:
                print(name.replace('_', '-'), v, file=f)

        print("</zeo>", file=f)

        if self.log:
            print(f"""
            <eventlog>
              level {self.loglevel}
              <logfile>
                 path {self.logpath}
              </logfile>
            </eventlog>
            """, file=f)

    def __str__(self):
        f = StringIO()
        self.dump(f)
        return f.getvalue()


def runner(config, qin, qout, timeout=None,
           debug=False, name=None,
           keep=False, protocol=None):

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        # logging is not configured -- maybe because
        # the process has been ``spawn``ed.
        # Try to recreate the logging configuration
        logini = os.environ.get("ZOPE_TESTRUNNER_LOG_INI")
        if logini is not None and os.path.exists(logini):
            from logging.config import fileConfig
            fileConfig(logini, disable_existing_loggers=False)

    if debug or DEBUG:
        debug_logging()

    old_protocol = None
    if protocol:
        import ZEO.asyncio.server
        old_protocol = ZEO.asyncio.server.best_protocol_version
        ZEO.asyncio.server.best_protocol_version = protocol
        old_protocols = ZEO.asyncio.server.ServerProtocol.protocols
        ZEO.asyncio.server.ServerProtocol.protocols = tuple(sorted(
            set(old_protocols) | {protocol}
            ))

    try:
        import threading

        from . import runzeo

        options = runzeo.ZEOOptions()
        options.realize(['-C', config])
        server = runzeo.ZEOServer(options)
        globals()[(name if name else 'last') + '_server'] = server
        server.open_storages()
        server.clear_socket()
        server.create_server()
        logger.debug('SERVER CREATED')
        qout.put(server.server.acceptor.addr)
        logger.debug('ADDRESS SENT')
        thread = threading.Thread(
            target=server.server.loop, kwargs=dict(timeout=.2),
            name=(None if name is None else name + '-server'),
            )
        thread.daemon = True
        thread.start()
        os.remove(config)

        try:
            qin.get(timeout=timeout)  # wait for shutdown
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
                     show_config=False):
    """Start a ZEO server in a separate process.

    Takes two positional arguments a string containing the storage conf
    and a ZEOConfig object.

    Returns the ZEO address, the test server address, the pid, and the path
    to the config file.
    """

    if not storage_conf:
        storage_conf = '<filestorage>\npath %s\n</filestorage>' % path

    if blob_dir:
        storage_conf = (f'<blobstorage>\nblob-dir {blob_dir}\n'
                        f'{storage_conf}\n</blobstorage>')

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
    fd, tmpfile = tempfile.mkstemp(".conf", prefix='ZEO_forker',
                                   dir=os.getcwd())
    with os.fdopen(fd, 'w') as fp:
        fp.write(zeo_conf)

    if threaded:
        from queue import Queue
        from threading import Thread
    else:
        # Experimental to see whether server logging problems under MacOS
        # have to do with its default `spawn` method
        # from multiprocessing import Process as Thread
        process_type = os.environ.get("ZEO_PROCESS_TYPE", "")
        from multiprocessing import context
        Thread = getattr(context, process_type.capitalize() + "Process")
        Queue = ThreadlessQueue

    logger.info("using thread type %r", Thread)
    qin = Queue()
    qout = Queue()
    thread = Thread(
        target=runner,
        args=[tmpfile, qin, qout, 999 if suicide else None],
        kwargs=dict(debug=debug, name=name, protocol=protocol, keep=keep),
        name=(None if name is None else name + '-server-runner'),
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


def shutdown_zeo_server(stop):
    stop()


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


class ThreadlessQueue:

    def __init__(self):
        self.cin, self.cout = multiprocessing.Pipe(False)

    def put(self, v):
        self.cout.send(v)

    def get(self, timeout=None):
        if self.cin.poll(timeout):
            return self.cin.recv()
        else:
            raise Empty()
