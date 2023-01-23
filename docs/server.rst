==================
Running the server
==================

Typically, the ZEO server is run using the ``runzeo`` script that's
installed as part of a ZEO installation.  The ``runzeo`` script
accepts command line options, the most important of which is the
``-C`` (``--configure``) option.  ZEO servers are best configured
via configuration files.  The ``runzeo`` script also accepts some
command-line arguments for ad-hoc configurations, but there's an
easier way to run an ad-hoc server described below.  For more on
configuring a ZEO server see `Server configuration`_ below.

Server quick-start/ad-hoc operation
===================================

You can quickly start a ZEO server from a Python prompt::

  import ZEO
  address, stop = ZEO.server()

This runs a ZEO server on a dynamic address and using an in-memory
storage.

We can then create a ZEO client connection using the address
returned::

  connection = ZEO.connection(address)

This is a ZODB connection for a database opened on a client storage
instance created on the fly.  This is a shorthand for::

  db = ZEO.DB(address)
  connection = db.open()

Which is a short-hand for::

  client_storage = ZEO.client(address)

  import ZODB
  db = ZODB.db(client_storage)
  connection = db.open()

If you exit the Python process, the storage exits as well, as it's run
in an in-process thread.

You shut down the server more cleanly by calling the stop function
returned by the ``ZEO.server`` function.

To have data stored persistently, you can specify a file-storage path
name using a ``path`` parameter.  If you want blob support, you can
specify a blob-file directory using the ``blob_dir`` directory.

You can also supply a port to listen on, full storage configuration
and ZEO server configuration options to the ``ZEO.server``
function. See it's documentation string for more information.

Server configuration
====================

The script ``runzeo`` runs the ZEO server.  The server can be
configured using command-line arguments or a configuration file.  This
document only describes the configuration file.  Run ``runzeo``
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

A configuration file consists of a ``<zeo>`` section and a storage
section, where the storage section can use any of the valid ZODB
storage types.  It may also contain an event log configuration.  See
`ZODB documentation <http://www.zodb.org>`_ for information on
configuring storages.

The ``zeo`` section must list the address.  All the other keys are
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

msgpack
        Use `msgpack <http://msgpack.org/index.html>`_ to serialize
        and de-serialize ZEO protocol messages.

        An advantage of using msgpack for ZEO communication is that
        it's a tiny bit faster and a ZEO server can support Python 2
        or Python 3 clients (but not both).

        msgpack can also be enabled by setting the ``ZEO_MSGPACK``
        environment to a non-empty string.

Server SSL configuration
------------------------

A server can optionally support SSL.  To do so, include a `ssl`
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
  to authenticate.  (See the ``cafile`` and ``capath``
  parameters in the Python documentation for
  ``ssl.SSLContext.load_verify_locations``.)

  If this setting is used. then certificate authentication is
  used to authenticate clients.  A client must be configured
  with one of the certificates supplied using this setting.

  This option assumes that you're using self-signed certificates.

Running the ZEO server as a daemon
==================================

In an operational setting, you will want to run the ZEO server as a
daemon process that is restarted when it dies.  ``runzeo`` makes no
attempt to implement a well behaved daemon. It expects that
functionality to be provided by a wrapper like `zdaemon
<https://pypi.org/project/zdaemon/>`_ or `supervisord
<http://supervisord.org/>`_.

Rotating log files
==================

``runzeo`` will re-initialize its logging subsystem when it receives a
SIGUSR2 signal.  If you are using the standard event logger, you
should first rename the log file and then send the signal to the
server.  The server will continue writing to the renamed log file
until it receives the signal.  After it receives the signal, the
server will create a new file with the old name and write to it.


