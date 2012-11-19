===
ZEO
===

ZEO provides a client-server storage implementation for ZODB.

Usage
=====

ZEO is a client-server system for sharing a single storage among many
clients.  When you use ZEO, the storage is opened in the ZEO server
process.  Client programs connect to this process using a ZEO
ClientStorage.  ZEO provides a consistent view of the database to all
clients.  The ZEO client and server communicate using a custom RPC
protocol layered on top of TCP.

There are several configuration options that affect the behavior of a
ZEO server.  This section describes how a few of these features
working.  Subsequent sections describe how to configure every option.

Client cache
~~~~~~~~~~~~

Each ZEO client keeps an on-disk cache of recently used objects to
avoid fetching those objects from the server each time they are
requested.  It is usually faster to read the objects from disk than it
is to fetch them over the network.  The cache can also provide
read-only copies of objects during server outages.

The cache may be persistent or transient. If the cache is persistent,
then the cache files are retained for use after process restarts. A
non-persistent cache uses temporary files that are removed when the
client storage is closed.

The client cache size is configured when the ClientStorage is created.
The default size is 20MB, but the right size depends entirely on the
particular database.  Setting the cache size too small can hurt
performance, but in most cases making it too big just wastes disk
space.  The document "Client cache tracing" describes how to collect a
cache trace that can be used to determine a good cache size.

ZEO uses invalidations for cache consistency.  Every time an object is
modified, the server sends a message to each client informing it of
the change.  The client will discard the object from its cache when it
receives an invalidation.  These invalidations are often batched.

Each time a client connects to a server, it must verify that its cache
contents are still valid.  (It did not receive any invalidation
messages while it was disconnected.)  There are several mechanisms
used to perform cache verification.  In the worst case, the client
sends the server a list of all objects in its cache along with their
timestamps; the server sends back an invalidation message for each
stale object.  The cost of verification is one drawback to making the
cache too large.

Note that every time a client crashes or disconnects, it must verify
its cache.  Every time a server crashes, all of its clients must
verify their caches.

The cache verification process is optimized in two ways to eliminate
costs when restarting clients and servers.  Each client keeps the
timestamp of the last invalidation message it has seen.  When it
connects to the server, it checks to see if any invalidation messages
were sent after that timestamp.  If not, then the cache is up-to-date
and no further verification occurs.  The other optimization is the
invalidation queue, described below.

Invalidation queue
~~~~~~~~~~~~~~~~~~

The ZEO server keeps a queue of recent invalidation messages in
memory.  When a client connects to the server, it sends the timestamp
of the most recent invalidation message it has received.  If that
message is still in the invalidation queue, then the server sends the
client all the missing invalidations.  This is often cheaper than
perform full cache verification.

The default size of the invalidation queue is 100.  If the
invalidation queue is larger, it will be more likely that a client
that reconnects will be able to verify its cache using the queue.  On
the other hand, a large queue uses more memory on the server to store
the message.  Invalidation messages tend to be small, perhaps a few
hundred bytes each on average; it depends on the number of objects
modified by a transaction.

Transaction timeouts
~~~~~~~~~~~~~~~~~~~~

A ZEO server can be configured to timeout a transaction if it takes
too long to complete.  Only a single transaction can commit at a time;
so if one transaction takes too long, all other clients will be
delayed waiting for it.  In the extreme, a client can hang during the
commit process.  If the client hangs, the server will be unable to
commit other transactions until it restarts.  A well-behaved client
will not hang, but the server can be configured with a transaction
timeout to guard against bugs that cause a client to hang.

If any transaction exceeds the timeout threshold, the client's
connection to the server will be closed and the transaction aborted.
Once the transaction is aborted, the server can start processing other
client's requests.  Most transactions should take very little time to
commit.  The timer begins for a transaction after all the data has
been sent to the server.  At this point, the cost of commit should be
dominated by the cost of writing data to disk; it should be unusual
for a commit to take longer than 1 second.  A transaction timeout of
30 seconds should tolerate heavy load and slow communications between
client and server, while guarding against hung servers.

When a transaction times out, the client can be left in an awkward
position.  If the timeout occurs during the second phase of the two
phase commit, the client will log a panic message.  This should only
cause problems if the client transaction involved multiple storages.
If it did, it is possible that some storages committed the client
changes and others did not.

Connection management
~~~~~~~~~~~~~~~~~~~~~

A ZEO client manages its connection to the ZEO server.  If it loses
the connection, it attempts to reconnect.  While
it is disconnected, it can satisfy some reads by using its cache.

The client can be configured to wait for a connection when it is created
or to return immediately and provide data from its persistent cache.
It usually simplifies programming to have the client wait for a
connection on startup.

When the client is disconnected, it polls periodically to see if the
server is available.  The rate at which it polls is configurable.

The client can be configured with multiple server addresses.  In this
case, it assumes that each server has identical content and will use
any server that is available.  It is possible to configure the client
to accept a read-only connection to one of these servers if no
read-write connection is available.  If it has a read-only connection,
it will continue to poll for a read-write connection.  This feature
supports the Zope Replication Services product,
http://www.zope.com/Products/ZopeProducts/ZRS.  In general, it could
be used to with a system that arranges to provide hot backups of
servers in the case of failure.

If a single address resolves to multiple IPv4 or IPv6 addresses,
the client will connect to an arbitrary of these addresses.

Authentication
~~~~~~~~~~~~~~

ZEO supports optional authentication of client and server using a
password scheme similar to HTTP digest authentication (RFC 2069).  It
is a simple challenge-response protocol that does not send passwords
in the clear, but does not offer strong security.  The RFC discusses
many of the limitations of this kind of protocol.  Note that this
feature provides authentication only.  It does not provide encryption
or confidentiality.

The challenge-response also produces a session key that is used to
generate message authentication codes for each ZEO message.  This
should prevent session hijacking.

Guard the password database as if it contained plaintext passwords.
It stores the hash of a username and password.  This does not expose
the plaintext password, but it is sensitive nonetheless.  An attacker
with the hash can impersonate the real user.  This is a limitation of
the simple digest scheme.

The authentication framework allows third-party developers to provide
new authentication modules.

Installing software
-------------------

ZEO is installed like other Python packages using pip, easy_install,
buildout, etc.

Configuring server
------------------

The script ``runzeo`` runs the ZEO server.  The server can be
configured using command-line arguments or a config file.  This
document only describes the config file.  Run runzeo.py
-h to see the list of command-line arguments.

The ``runzeo`` script imports the ZEO package.  ZEO must either be
installed in Python's site-packages directory or be in a directory on
PYTHONPATH.

The configuration file specifies the underlying storage the server
uses, the address it binds, and a few other optional parameters.
An example is::

    <zeo>
      address zeo.example.com:8090
    </zeo>

    <filestorage>
      path /var/tmp/Data.fs
    </filestorage>

    <eventlog>
      <logfile>
        path /var/tmp/zeo.log
        format %(asctime)s %(message)s
      </logfile>
    </eventlog>

This file configures a server to use a FileStorage from
``/var/tmp/Data.fs``.  The server listens on port 8090 of
zeo.example.com.  The ZEO server writes its log file to
/var/tmp/zeo.log and uses a custom format for each line.  Assuming the
example configuration it stored in zeo.config, you can run a server by
typing::

    python runzeo -C zeo.config

A configuration file consists of a <zeo> section and a storage
section, where the storage section can use any of the valid ZODB
storage types.  It may also contain an eventlog configuration.  See
the document "Configuring a ZODB database" for more information about
configuring storages and eventlogs.

The zeo section must list the address.  All the other keys are
optional.

address
        The address at which the server should listen.  This can be in
        the form 'host:port' to signify a TCP/IP connection or a
        pathname string to signify a Unix domain socket connection (at
        least one '/' is required).  A hostname may be a DNS name or a
        dotted IP address.  If the hostname is omitted, the platform's
        default behavior is used when binding the listening socket (''
        is passed to socket.bind() as the hostname portion of the
        address).

read-only
        Flag indicating whether the server should operate in read-only
        mode.  Defaults to false.  Note that even if the server is
        operating in writable mode, individual storages may still be
        read-only.  But if the server is in read-only mode, no write
        operations are allowed, even if the storages are writable.  Note
        that pack() is considered a read-only operation.

invalidation-queue-size
        The storage server keeps a queue of the objects modified by the
        last N transactions, where N == invalidation_queue_size.  This
        queue is used to speed client cache verification when a client
        disconnects for a short period of time.

transaction-timeout
        The maximum amount of time to wait for a transaction to commit
        after acquiring the storage lock, specified in seconds.  If the
        transaction takes too long, the client connection will be closed
        and the transaction aborted.

authentication-protocol
        The name of the protocol used for authentication.  The
        only protocol provided with ZEO is "digest," but extensions
        may provide other protocols.

authentication-database
        The path of the database containing authentication credentials.

authentication-realm
        The authentication realm of the server.  Some authentication
        schemes use a realm to identify the logic set of usernames
        that are accepted by this server.

Configuring clients
-------------------

The ZEO client can also be configured using ZConfig.  The ZODB.config
module provides several function for opening a storage based on its
configuration.

- ZODB.config.storageFromString()
- ZODB.config.storageFromFile()
- ZODB.config.storageFromURL()

The ZEO client configuration requires the server address be
specified.  Everything else is optional.  An example configuration is::

    <zeoclient>
      server zeo.example.com:8090
    </zeoclient>

The other configuration options are listed below.

cache-size
        The maximum size of the client cache, in bytes.

name
        The storage name.  If unspecified, the address of the server
        will be used as the name.

client
        Enables persistent cache files.  The string passed here is
        used to construct the cache filenames.  If it is not
        specified, the client creates a temporary cache that will
        only be used by the current object.

var
        The directory where persistent cache files are stored.  By
        default cache files, if they are persistent, are stored in 
        the current directory.

min-disconnect-poll
        The minimum delay in seconds between attempts to connect to
        the server, in seconds.  Defaults to 5 seconds.

max-disconnect-poll
        The maximum delay in seconds between attempts to connect to
        the server, in seconds.  Defaults to 300 seconds.

wait
        A boolean indicating whether the constructor should wait
        for the client to connect to the server and verify the cache
        before returning.  The default is true.

read-only
        A flag indicating whether this should be a read-only storage,
        defaulting to false (i.e. writing is allowed by default).

read-only-fallback
        A flag indicating whether a read-only remote storage should be
        acceptable as a fallback when no writable storages are
        available.  Defaults to false.  At most one of read_only and
        read_only_fallback should be true.

realm
        The authentication realm of the server.  Some authentication
        schemes use a realm to identify the logic set of usernames
        that are accepted by this server.

A ZEO client can also be created by calling the ClientStorage
constructor explicitly.  For example::

    from ZEO.ClientStorage import ClientStorage
    storage = ClientStorage(("zeo.example.com", 8090))

Running the ZEO server as a daemon
----------------------------------

In an operational setting, you will want to run the ZEO server a
daemon process that is restarted when it dies.  The zdaemon package
provides two tools for running daemons: zdrun.py and zdctl.py. You can
find zdaemon and it's documentation at
http://pypi.python.org/pypi/zdaemon.

Rotating log files
~~~~~~~~~~~~~~~~~~

ZEO will re-initialize its logging subsystem when it receives a
SIGUSR2 signal.  If you are using the standard event logger, you
should first rename the log file and then send the signal to the
server.  The server will continue writing to the renamed log file
until it receives the signal.  After it receives the signal, the
server will create a new file with the old name and write to it.

Tools
-----

There are a few scripts that may help running a ZEO server.  The
zeopack script connects to a server and packs the storage.  It can
be run as a cron job.  The zeopasswd.py script
manages a ZEO servers password database.

Diagnosing problems
-------------------

If an exception occurs on the server, the server will log a traceback
and send an exception to the client.  The traceback on the client will
show a ZEO protocol library as the source of the error.  If you need
to diagnose the problem, you will have to look in the server log for
the rest of the traceback.

Compatibility
=============

ZEO 4.0.0 requires Python 2.6 or later.

Note --
   When using ZEO and upgrading from Python 2.4, you need to upgrade
   clients and servers at the same time, or upgrade clients first and
   then servers.  Clients running Python 2.5 or 2.6 will work with
   servers running Python 2.4.  Clients running Python 2.4 won't work
   properly with servers running Python 2.5 or later due to changes in
   the way Python implements exceptions.

For a long time ZEO has been distributes with ZODB.  ZEO 4 is
is now maintained as a separate project.

ZEO clients from ZODB 3.2 on can talk to ZEO 4.0 servers.
ZEO 4.0 clients  talk to ZODB 3.8, 3.9, and 3.10 and ZEO 4.0 servers.

Note --
   ZEO 4.0 servers don't support undo for clients older than ZODB 3.10.

Testing for Developers
======================

The ZEO checkouts are `buildouts <http://www.python.org/pypi/zc.buildout>`_.
When working from a ZODB checkout, first run the bootstrap.py script
to initialize the buildout:

    % python bootstrap.py

and then use the buildout script to build ZODB and gather the dependencies:

    % bin/buildout

This creates a test script:

    % bin/test -v

This command will run all the tests, printing a single dot for each
test.  When it finishes, it will print a test summary.  The exact
number of tests can vary depending on platform and available
third-party libraries.::

    Ran 1182 tests in 241.269s

    OK

The test script has many more options.  Use the ``-h`` or ``--help``
options to see a file list of options.  The default test suite omits
several tests that depend on third-party software or that take a long
time to run.  To run all the available tests use the ``--all`` option.
Running all the tests takes much longer.::

    Ran 1561 tests in 1461.557s

    OK

More information
================

For more information on ZEO, see http://zodb.org

There is a Mailman mailing list in place to discuss all issues related
to ZODB, including ZEO.  You can send questions to

    zodb-dev@zope.org

or subscribe at

    http://lists.zope.org/mailman/listinfo/zodb-dev

and view its archives at

    http://lists.zope.org/pipermail/zodb-dev

Note that Zope mailing lists have a subscriber-only posting policy.

Bugs and Patches
================

Bug reports and patches should be added to the Launchpad:

    https://launchpad.net/zodb


..
   Local Variables:
   mode: indented-text
   indent-tabs-mode: nil
   sentence-end-double-space: t
   fill-column: 70
   End:
