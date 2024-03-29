##############################################################################
#
# Copyright (c) 2001, 2002, 2003 Zope Foundation and Contributors.
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
"""Start the ZEO storage server.

Usage: %s [-C URL] [-a ADDRESS] [-f FILENAME] [-h]

Options:
-C/--configure URL -- configuration file or URL
-a/--address ADDRESS -- server address of the form PORT, HOST:PORT, or PATH
                        (a PATH must contain at least one "/")
-f/--filename FILENAME -- filename for FileStorage
-t/--timeout TIMEOUT -- transaction timeout in seconds (default no timeout)
-h/--help -- print this usage message and exit
--pid-file PATH -- relative path to output file containing this process's pid;
                   default $(INSTANCE_HOME)/var/ZEO.pid but only if envar
                   INSTANCE_HOME is defined

Unless -C is specified, -a and -f are required.
"""

# The code here is designed to be reused by other, similar servers.

import logging
import os
import signal
import socket
import sys

import ZConfig.datatypes
from zdaemon.zdoptions import ZDOptions


logger = logging.getLogger('ZEO.runzeo')
_pid = str(os.getpid())


def log(msg, level=logging.INFO, exc_info=False):
    """Internal: generic logging function."""
    message = f'({_pid}) {msg}'
    logger.log(level, message, exc_info=exc_info)


def parse_binding_address(arg):
    # Caution:  Not part of the official ZConfig API.
    obj = ZConfig.datatypes.SocketBindingAddress(arg)
    return obj.family, obj.address


def windows_shutdown_handler():
    # Called by the signal mechanism on Windows to perform shutdown.
    import asyncore
    asyncore.close_all()


class ZEOOptionsMixin:

    storages = None

    def handle_address(self, arg):
        self.family, self.address = parse_binding_address(arg)

    def handle_filename(self, arg):
        from ZODB.config import FileStorage  # That's a FileStorage *opener*!

        class FSConfig:
            def __init__(self, name, path):
                self._name = name
                self.path = path
                self.stop = None

            def getSectionName(self):
                return self._name

        if not self.storages:
            self.storages = []
        name = str(1 + len(self.storages))
        conf = FileStorage(FSConfig(name, arg))
        self.storages.append(conf)

    testing_exit_immediately = False

    def handle_test(self, *args):
        self.testing_exit_immediately = True

    def add_zeo_options(self):
        self.add(None, None, None, "test", self.handle_test)
        self.add(None, None, "a:", "address=", self.handle_address)
        self.add(None, None, "f:", "filename=", self.handle_filename)
        self.add("family", "zeo.address.family")
        self.add("address", "zeo.address.address",
                 required="no server address specified; use -a or -C")
        self.add("read_only", "zeo.read_only", default=0)
        self.add("client_conflict_resolution",
                 "zeo.client_conflict_resolution",
                 default=0)
        self.add("msgpack", "zeo.msgpack", default=0)
        self.add("invalidation_queue_size", "zeo.invalidation_queue_size",
                 default=100)
        self.add("invalidation_age", "zeo.invalidation_age")
        self.add("transaction_timeout", "zeo.transaction_timeout",
                 "t:", "timeout=", float)
        self.add('pid_file', 'zeo.pid_filename',
                 None, 'pid-file=')
        self.add("ssl", "zeo.ssl")


class ZEOOptions(ZDOptions, ZEOOptionsMixin):

    __doc__ = __doc__

    logsectionname = "eventlog"
    schemadir = os.path.dirname(__file__)

    def __init__(self):
        ZDOptions.__init__(self)
        self.add_zeo_options()
        self.add("storages", "storages",
                 required="no storages specified; use -f or -C")

    def realize(self, *a, **k):
        ZDOptions.realize(self, *a, **k)
        nunnamed = [s for s in self.storages if s.name is None]
        if nunnamed:
            if len(nunnamed) > 1:
                return self.usage("No more than one storage may be unnamed.")
            if [s for s in self.storages if s.name == '1']:
                return self.usage(
                    "Can't have an unnamed storage and a storage named 1.")
            for s in self.storages:
                if s.name is None:
                    s.name = '1'
                    break


class ZEOServer:

    def __init__(self, options):
        self.options = options
        self.server = None

    def main(self):
        self.setup_default_logging()
        self.check_socket()
        self.clear_socket()
        self.make_pidfile()
        try:
            self.open_storages()
            self.setup_signals()
            self.create_server()
            self.loop_forever()
        finally:
            self.close_server()
            self.clear_socket()
            self.remove_pidfile()

    def setup_default_logging(self):
        if self.options.config_logger is not None:
            return
        # No log file is configured; default to stderr.
        root = logging.getLogger()
        root.setLevel(logging.INFO)
        fmt = logging.Formatter(
                "------\n%(asctime)s %(levelname)s %(name)s %(message)s",
                "%Y-%m-%dT%H:%M:%S")
        handler = logging.StreamHandler()
        handler.setFormatter(fmt)
        root.addHandler(handler)

    def check_socket(self):
        if isinstance(self.options.address, tuple) and \
           self.options.address[1] is None:
            self.options.address = self.options.address[0], 0
            return

        if self.can_connect(self.options.family, self.options.address):
            self.options.usage("address %s already in use" %
                               repr(self.options.address))

    def can_connect(self, family, address):
        s = socket.socket(family, socket.SOCK_STREAM)
        try:
            s.connect(address)
        except OSError:
            return 0
        else:
            s.close()
            return 1

    def clear_socket(self):
        if isinstance(self.options.address, str):
            try:
                os.unlink(self.options.address)
            except OSError:
                pass

    def open_storages(self):
        self.storages = {}
        for opener in self.options.storages:
            log("opening storage %r using %s"
                % (opener.name, opener.__class__.__name__))
            self.storages[opener.name] = opener.open()

    def setup_signals(self):
        """Set up signal handlers.

        The signal handler for SIGFOO is a method handle_sigfoo().
        If no handler method is defined for a signal, the signal
        action is not changed from its initial value.  The handler
        method is called without additional arguments.
        """
        if os.name != "posix":
            if os.name == "nt":
                self.setup_win32_signals()
            return
        if hasattr(signal, 'SIGXFSZ'):
            signal.signal(signal.SIGXFSZ, signal.SIG_IGN)  # Special case
        init_signames()
        for sig, name in signames.items():
            method = getattr(self, "handle_" + name.lower(), None)
            if method is not None:
                def wrapper(sig_dummy, frame_dummy, method=method):
                    method()
                signal.signal(sig, wrapper)

    def setup_win32_signals(self):
        # Borrow the Zope Signals package win32 support, if available.
        # Signals does a check/log for the availability of pywin32.
        try:
            import Signals.Signals
        except ImportError:
            logger.debug("Signals package not found. "
                         "Windows-specific signal handler "
                         "will *not* be installed.")
            return
        SignalHandler = Signals.Signals.SignalHandler
        if SignalHandler is not None:  # may be None if no pywin32.
            SignalHandler.registerHandler(signal.SIGTERM,
                                          windows_shutdown_handler)
            SignalHandler.registerHandler(signal.SIGINT,
                                          windows_shutdown_handler)
            SIGUSR2 = 12  # not in signal module on Windows.
            SignalHandler.registerHandler(SIGUSR2, self.handle_sigusr2)

    def create_server(self):
        self.server = create_server(self.storages, self.options)

    def loop_forever(self):
        if self.options.testing_exit_immediately:
            print("testing exit immediately")
            sys.stdout.flush()  # ensure truthful output order
        else:
            self.server.loop()

    def close_server(self):
        if self.server is not None:
            self.server.close()
            if getattr(self.options, "testing_exit_immediately", False):
                acceptor = self.server.acceptor
                if hasattr(acceptor, "event_loop"):
                    # usually, this happens automatically - but not for testing
                    acceptor.event_loop.close()

    def handle_sigterm(self):
        log("terminated by SIGTERM")
        sys.exit(0)

    def handle_sigint(self):
        log("terminated by SIGINT")
        sys.exit(0)

    def handle_sighup(self):
        log("restarted by SIGHUP")
        sys.exit(1)

    def handle_sigusr2(self):
        # log rotation signal - do the same as Zope 2.7/2.8...
        if self.options.config_logger is None or \
           os.name not in ("posix", "nt"):
            log("received SIGUSR2, but it was not handled!",
                level=logging.WARNING)
            return

        loggers = [self.options.config_logger]

        if os.name == "posix":
            for logger in loggers:
                logger.reopen()
            log("Log files reopened successfully", level=logging.INFO)
        else:  # nt - same rotation code as in Zope's Signals/Signals.py
            for logger in loggers:
                for factory in logger.handler_factories:
                    handler = factory()
                    if hasattr(handler, 'rotate') and callable(handler.rotate):
                        handler.rotate()
            log("Log files rotation complete", level=logging.INFO)

    def _get_pidfile(self):
        pidfile = self.options.pid_file
        # 'pidfile' is marked as not required.
        if not pidfile:
            # Try to find a reasonable location if the pidfile is not
            # set. If we are running in a Zope environment, we can
            # safely assume INSTANCE_HOME.
            instance_home = os.environ.get("INSTANCE_HOME")
            if not instance_home:
                # If all our attempts failed, just log a message and
                # proceed.
                logger.debug("'pidfile' option not set, and 'INSTANCE_HOME' "
                             "environment variable could not be found. "
                             "Cannot guess pidfile location.")
                return
            self.options.pid_file = os.path.join(instance_home,
                                                 "var", "ZEO.pid")

    def make_pidfile(self):
        if not self.options.read_only:
            self._get_pidfile()
            pidfile = self.options.pid_file
            if pidfile is None:
                return
            pid = os.getpid()
            try:
                if os.path.exists(pidfile):
                    os.unlink(pidfile)
                f = open(pidfile, 'w')
                print(pid, file=f)
                f.close()
                log("created PID file '%s'" % pidfile)
            except OSError:
                logger.error("PID file '%s' cannot be opened" % pidfile)

    def remove_pidfile(self):
        if not self.options.read_only:
            pidfile = self.options.pid_file
            if pidfile is None:
                return
            try:
                if os.path.exists(pidfile):
                    os.unlink(pidfile)
                    log("removed PID file '%s'" % pidfile)
            except OSError:
                logger.error("PID file '%s' could not be removed" % pidfile)


def create_server(storages, options):
    from .StorageServer import StorageServer
    return StorageServer(
        options.address,
        storages,
        read_only=options.read_only,
        client_conflict_resolution=options.client_conflict_resolution,
        msgpack=(options.msgpack if isinstance(options.msgpack, bool)
                 else os.environ.get('ZEO_MSGPACK')),
        invalidation_queue_size=options.invalidation_queue_size,
        invalidation_age=options.invalidation_age,
        transaction_timeout=options.transaction_timeout,
        ssl=options.ssl)


# Signal names

signames = None


def signame(sig):
    """Return a symbolic name for a signal.

    Return "signal NNN" if there is no corresponding SIG name in the
    signal module.
    """

    if signames is None:
        init_signames()
    return signames.get(sig) or "signal %d" % sig


def init_signames():
    global signames
    signames = {}
    for name, sig in signal.__dict__.items():
        k_startswith = getattr(name, "startswith", None)
        if k_startswith is None:
            continue
        if k_startswith("SIG") and not k_startswith("SIG_"):
            signames[sig] = name


# Main program

def main(args=None):
    options = ZEOOptions()
    options.realize(args)
    s = ZEOServer(options)
    s.main()


def run(args):
    options = ZEOOptions()
    options.realize(args)
    s = ZEOServer(options)
    s.run()


if __name__ == "__main__":
    main()
