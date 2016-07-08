==========================
ZEO HOWTO
==========================

.. contents::

Introduction
============

ZEO (Zope Enterprise Objects) is a client-server system for sharing a
single storage among many clients. When you use ZEO, a lower-level
storage, typically a file storage, is opened in the ZEO server
process.  Client programs connect to this process using a ZEO
ClientStorage.  ZEO provides a consistent view of the database to all
clients.  The ZEO client and server communicate using a custom
protocol layered on top of TCP.

There are several features that affect the behavior of
ZEO.  This section describes how a few of these features
work.  Subsequent sections describe how to configure every option.

Client cache
------------

Each ZEO client keeps an on-disk cache of recently used data records
to avoid fetching those records from the server each time they are
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
space.

ZEO uses invalidations for cache consistency.  Every time an object is
modified, the server sends a message to each client informing it of
the change.  The client will discard the object from its cache when it
receives an invalidation. (It's actually a little more complicated,
but we won't get into that here.)

Each time a client connects to a server, it must verify that its cache
contents are still valid.  (It did not receive any invalidation
messages while it was disconnected.)  This involves asking the server
to replay invalidations it missed. If it's been disconnected too long,
it discards its cache.


Invalidation queue
------------------

The ZEO server keeps a queue of recent invalidation messages in
memory.  When a client connects to the server, it sends the timestamp
of the most recent invalidation message it has received.  If that
message is still in the invalidation queue, then the server sends the
client all the missing invalidations.

The default size of the invalidation queue is 100.  If the
invalidation queue is larger, it will be more likely that a client
that reconnects will be able to verify its cache using the queue.  On
the other hand, a large queue uses more memory on the server to store
the message.  Invalidation messages tend to be small, perhaps a few
hundred bytes each on average; it depends on the number of objects
modified by a transaction.

You can also provide an invalidation age when configuring the
server. In this case, if the invalidation queue is too small, but a
client has been disconnected for a time interval that is less than the
invalidation age, then invalidations are replayed by iterating over
the lower-level storage on the server.  If the age is too high, and
clients are disconneced for a long time, then this can put a lot of
load on the server.

Transaction timeouts
--------------------

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
---------------------

A ZEO client manages its connection to the ZEO server.  If it loses
the connection, it attempts to reconnect.  While
it is disconnected, it can satisfy some reads by using its cache.

The client can be configured with multiple server addresses.  In this
case, it assumes that each server has identical content and will use
any server that is available.  It is possible to configure the client
to accept a read-only connection to one of these servers if no
read-write connection is available.  If it has a read-only connection,
it will continue to poll for a read-write connection.

If a single address resolves to multiple IPv4 or IPv6 addresses,
the client will connect to an arbitrary of these addresses.

SSL
---

ZEO supports the use of SSL connections between servers and clients,
including certificate authentication.  We're still understanding use
cases for this, so details of operation may change.

Installing software
===================

ZEO is installed like any other Python package using pip, buildout, or
pther Python packaging tools.

Running the server
==================

Typically, the ZEO server is run using the ``runzeo`` script that's
installed as part of a ZEO installation.  The ``runzeo`` script
accepts command line options, the most important of which is the
``-C`` (``--configuration``) option.  ZEO servers are best configured
via configuration files.  The ``runzeo`` script also accepts some
command-line arguments for ad-hoc configurations, but there's an
easier way to run an ad-hoc server described below.  For more on
configuraing a ZEO server see `Server configuration`_ below.

Server quick-start/ad-hoc operation
-----------------------------------

You can quickly start a ZEO server from a Python prompt::

  import ZEO
  address, stop = ZEO.server()

This runs a ZEO server on a dynamic address and using an in-memory
storage.

We can then create a ZEO client connection using the address
returned::

  connection = ZEO.connection(addr)

This is a ZODB connection for a database opened on a client storage
instance created on the fly.  This is a shorthand for::

  db = ZEO.DB(addr)
  connection = db.open()

Which is a short-hand for::

  client_storage = ZEO.client(addr)

  import ZODB
  db = ZODB.db(client_storage)
  connection = db.open()

If you exit the Python process, the storage exits as well, as it's run
in an in-process thread.  It will leave behind it's configuration and
log file.  This provides a handy way to get a configuration example.

You shut down the server more cleanly by calling the stop function
returned by the ``ZEO.server`` function.  This will remove the
temporary configuration file.

To have data stored persistently, you can specify a file-storage path
name using a ``path`` parameter.  If you want blob support, you can
specify a blob-file directory using the ``blob_dir`` directory.

You can also supply a port to listen on, full storage configuration
and ZEO server configuration options to the ``ZEO.server``
function. See it's documentation string for more information.

Server configuration
--------------------

The script runzeo.py runs the ZEO server.  The server can be
configured using command-line arguments or a config file.  This
document only describes the config file.  Run runzeo.py
-h to see the list of command-line arguments.

The configuration file specifies the underlying storage the server
uses, the address it binds to, and a few other optional parameters.
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

The format is similar to the Apache configuration format.  Individual
settings have a name, 1 or more spaces and a value, as in::

  address zeo.example.com:8090

Settings are grouped into hierarchical sections.

The example above configures a server to use a file storage from
``/var/tmp/Data.fs``.  The server listens on port ``8090`` of
``zeo.example.com``.  The ZEO server writes its log file to
``/var/tmp/zeo.log`` and uses a custom format for each line.  Assuming the
example configuration it stored in ``zeo.config``, you can run a server by
typing::

    runzeo -C zeo.config

A configuration file consists of a <zeo> section and a storage
section, where the storage section can use any of the valid ZODB
storage types.  It may also contain an eventlog configuration.  See
ZODB documentation for information on configuring storages. See
`Configuring event logs <logs.rst>`_ for information on configuring
server logs.

An **easy way to get started** with configuration is to run an add-hoc
server and copy the generated configuration.

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
        queue is used to support client cache verification when a client
        disconnects for a short period of time.

invalidation-age
        The maximum age of a client for which quick-verification
        invalidations will be provided by iterating over the served
        storage. This option should only be used if the served storage
        supports efficient iteration from a starting point near the
        end of the transaction history (e.g. end of file).

transaction-timeout
        The maximum amount of time, in seconds, to wait for a
        transaction to commit after acquiring the storage lock,
        specified in seconds.  If the transaction takes too long, the
        client connection will be closed and the transaction aborted.

        This defaults to 30 seconds.

client-conflict-resolution
        Flag indicating that clients should perform conflict
        resolution. This option defaults to false.

Server SSL configuration
~~~~~~~~~~~~~~~~~~~~~~~~

A server can optionally support SSL.  Do do so, include a `ssl`
subsection of the ZEO section, as in::

    <zeo>
      address zeo.example.com:8090
      <ssl>
        certificate server_certificate.pem
        key server_certificate_key.pem
      </ssl>
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

The ``ssl`` section has settings:

certificate
  The path to an SSL certificate file for the server. (required)

key
  The path to the SSL key file for the server certificate (if not
  included in certificate file).

password-function
  The dotted name if an importable function that, when imported, returns
  the password needed to unlock the key (if the key requires a password.)

authenticate
  The path to a file or directory containing client certificates
  to authenticate.  ((See the ``cafile`` and ``capath``
  parameters in the Python documentation for
  ``ssl.SSLContext.load_verify_locations``.)

  If this setting is used. then certificate authentication is
  used to authenticate clients.  A client must be configured
  with one of the certificates supplied using this setting.

  This option assumes that you're using self-signed certificates.

Running the ZEO server as a daemon
----------------------------------

In an operational setting, you will want to run the ZEO server a
daemon process that is restarted when it dies.  The zdaemon package
provides two tools for running daemons: zdrun.py and zdctl.py. You can
find zdaemon and it's documentation at
http://pypi.python.org/pypi/zdaemon.

Note that ``runzeo`` makes no attempt to implemnt a well behaved
daemon. It expects that functionality to be provided by a wrapper like
zdaemon or supervisord.

Rotating log files
------------------

``runzeo`` will re-initialize its logging subsystem when it receives a
SIGUSR2 signal.  If you are using the standard event logger, you
should first rename the log file and then send the signal to the
server.  The server will continue writing to the renamed log file
until it receives the signal.  After it receives the signal, the
server will create a new file with the old name and write to it.

ZEO Clients
===========

To use a ZEO server, you need to connect to it using a ZEO client
storage.  You create client storages either using a Python API or
using a ZODB storage configuration in a ZODB storage configuration
section.

Python API for creating a ZEO client storage
--------------------------------------------

To create a client storage from Python, use the ``ZEO.client``
function::

    import ZEO
    client = ZEO.client(8200)

In the example above, we created a client that connected to a storage
listening on port 8200 on local host.  The first argument is an
address, or list of addresses to connect to.  There are many additinal
options, decumented below that should be given as keyword arguments.

Addresses can be:

- A host/port tuple

- An integer, which implies that the host is '127.0.0.1'

- A unix domain socket file name.

Options:

cache_size
   The cache size in bytes. This defaults to a 20MB.

cache
   The ZEO cache to be used.  This can be a file name, which will
   cause a persisetnt standard persistent ZEO cache to be used and
   stored in the given name.  This can also be an object that
   implements ``ZEO.interfaces.ICache``.

   If not specified, then a non-persistent cache will be used.

blob_dir
   The name of a directory to hold/cache blob data downloaded from the
   server.  This must be provided if blobs are to be used.  (Of
   course, the server storage must be configured to use blobs as
   well.)

shared_blob_dir
   A client can use a network files system (or a local directory if
   the server runs on the same machine) to share a blob directory with
   the server.  This allows downloading of blobs (except via a
   distributed file system) to be avoided.

blob_cache_size
   The size of the blob cache in bytes.  IF unset, then blobs will
   accumulate. If set, then blobs are removed when the total size
   exceeds this amount.  Blobs accessed least recently are removed
   first.

blob_cache_size_check
   The total size of data to be downloaded to trigger blob cache size
   reduction. The defaukt is 10 (percent).  This controls how often to
   remove blobs from the cache.

ssl
   An ``ssl.SSLContext`` object used to make SSL connections.

ssl_server_hostname
   Host name to use for SSL host name checks.

   If using SSL and if host name checking is enabled in the given SSL
   context then use this as the value to check.  If an address is a
   host/port pair, then this defaults to the host in the address.

read_only
   Set to true for a read-only connection.

   If false (the default), then request a read/write connection.

   This option is ignored if ``read_only_fallback`` is set to a true value.

read_only_fallback
   Set to true, then prefer a read/write connection, but be willing to
   use a read-only connection.  This defaults to a false value.

   If ``read_only_fallback`` is set, then ``read_only`` is ignored.

wait_timeout
   How long to wait for an initial connection, defaulting to 30
   seconds.  If an initial connection can't be made within this time
   limit, then creation of the client storage will fail with a
   ``ZEO.Exceptions.ClientDisconnected`` exception.

   After the initial connection, if the client is disconnected:

   - In-flight server requests will fail with a
     ``ZEO.Exceptions.ClientDisconnected`` exception.

   - New requests will block for up to ``wait_timeout`` waiting for a
     connection to be established before failing with a
     ``ZEO.Exceptions.ClientDisconnected`` exception.

client_label
   A short string to display in *server* logs for an event relating to
   this client. This can be helpful when debugging.

disconnect_poll
   The delay in seconds between attempts to connect to the
   server, in seconds.  Defaults to 1 second.

Configuration strings/files
---------------------------

ZODB databases and storages can be configured using configuration
files, or strings (extracted from configuration files).  They use the
same syntax as the server configuration files described above, but
with different sections and options.

An application that used ZODB might configure it's database using a
string like::

  <zodb>
     cache-size-bytes 1000MB

     <filestorage>
       path /var/lib/Data.fs
     </filestorage>
  </zodb>

In this example, we configured a ZODB database with a object cache
size of 1GB.  Inside the database, we configured a file storage.  The
``filestorage`` section provided file-storage parameters.  We saw a
similar section in the storage-server configuration example in `Server
configuration`_.

To configure a client storage, you use a ``clientstorage`` section,
but first you have to import it's definition, because ZEO isn't built
into ZODB.  Here's an example::

  <zodb>
     cache-size-bytes 1000MB

     %import ZEO

     <clientstorage>
       server 8200
     </clientstorage>
  </zodb>

In this example, we defined a client storage that connected to a
server on port 8200.

The following settings are supported:

cache-size
   The cache size in bytes, KB or MB. This defaults to a 20MB.
   Optional ``KB`` or ``MB`` suffixes can (and usually are) used to
   specify units other than bytes.

cache-path
   The file path of a persistent cache file

blob-dir
   The name of a directory to hold/cache blob data downloaded from the
   server.  This must be provided if blobs are to be used.  (Of
   course, the server storage must be configured to use blobs as
   well.)

shared-blob-dir
   A client can use a network files system (or a local directory if
   the server runs on the same machine) to share a blob directory with
   the server.  This allows downloading of blobs (except via a
   distributed file system) to be avoided.

blob-cache-size
   The size of the blob cache in bytes.  IF unset, then blobs will
   accumulate. If set, then blobs are removed when the total size
   exceeds this amount.  Blobs accessed least recently are removed
   first.

blob-cache-size-check
   The total size of data to be downloaded to trigger blob cache size
   reduction. The defaukt is 10 (percent).  This controls how often to
   remove blobs from the cache.

read-only
   Set to true for a read-only connection.

   If false (the default), then request a read/write connection.

   This option is ignored if ``read_only_fallback`` is set to a true value.

read-only-fallback
   Set to true, then prefer a read/write connection, but be willing to
   use a read-only connection.  This defaults to a false value.

   If ``read_only_fallback`` is set, then ``read_only`` is ignored.

wait_timeout
   How long to wait for an initial connection, defaulting to 30
   seconds.  If an initial connection can't be made within this time
   limit, then creation of the client storage will fail with a
   ``ZEO.Exceptions.ClientDisconnected`` exception.

   After the initial connection, if the client is disconnected:

   - In-flight server requests will fail with a
     ``ZEO.Exceptions.ClientDisconnected`` exception.

   - New requests will block for up to ``wait_timeout`` waiting for a
     connection to be established before failing with a
     ``ZEO.Exceptions.ClientDisconnected`` exception.

client_label
   A short string to display in *server* logs for an event relating to
   this client. This can be helpful when debugging.

disconnect_poll
   The delay in seconds between attempts to connect to the
   server, in seconds.  Defaults to 1 second.

Client SSL configuration
~~~~~~~~~~~~~~~~~~~~~~~~

An ``ssl`` subsection can be used to enable and configure SSL, as in::

  %import ZEO

  <clientstorage>
    server zeo.example.com8200
    <ssl>
    </ssl>
  </clientstorage>

In the example above, SSL is enabled in it's simplest form:

- The cient expects the server to have a signed certificate, which the
  client validates.

- The server server host name ``zeo.example.com`` is checked against
  the server's certificate.

A number of settings can be provided to configure SSL:

certificate
  The path to an SSL certificate file for the client.  This is
  needed to allow the server to authenticate the client.

key
  The path to the SSL key file for the client certificate (if not
  included in the certificate file).

password-function
  A dotted name if an importable function that, when imported, returns
  the password needed to unlock the key (if the key requires a password.)

authenticate
  The path to a file or directory containing server certificates
  to authenticate.  ((See the ``cafile`` and ``capath``
  parameters in the Python documentation for
  ``ssl.SSLContext.load_verify_locations``.)

  If this setting is used. then certificate authentication is
  used to authenticate the server.  The server must be configuted
  with one of the certificates supplied using this setting.

check-hostname
  This is a boolean setting that defaults to true. Verify the
  host name in the server certificate is as expected.

server-hostname
  The expected server host name.  This defaults to the host name
  used in the server address.  This option must be used when
  ``check-hostname`` is true and when a server address has no host
  name (localhost, or unix domain socket) or when there is more
  than one seerver and server hostnames differ.

  Using this setting implies a true value for the ``check-hostname`` setting.
