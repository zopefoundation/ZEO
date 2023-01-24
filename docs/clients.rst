===========
ZEO Clients
===========

To use a ZEO server, you need to connect to it using a ZEO client
storage.  You create client storages either using a Python API or
using a ZODB storage configuration in a ZODB storage configuration
section.

Python API for creating a ZEO client storage
============================================

To create a client storage from Python, use the ``ZEO.client``
function::

    import ZEO
    client = ZEO.client(8200)

In the example above, we created a client that connected to a storage
listening on port 8200 on local host.  The first argument is an
address, or list of addresses to connect to.  There are many additional
options, documented below that should be given as keyword arguments.

Addresses can be:

- A host/port tuple

- An integer, which implies that the host is '127.0.0.1'

- A unix domain socket file name.

Options:

cache_size
   The cache size in bytes. This defaults to a 20MB.

cache
   The ZEO cache to be used.  This can be a file name, which will
   cause a persistent standard persistent ZEO cache to be used and
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
   reduction. The default is 10 (percent).  This controls how often to
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

server_sync
   Flag, false by default, indicating whether the ``sync`` method
   should make a server request.  The ``sync`` method is called at the
   start of explicitly begin transactions.  Making a server requests assures
   that any invalidations outstanding at the beginning of a
   transaction are processed.

   Setting this to True is important when application activity is
   spread over multiple ZEO clients. The classic example of this is
   when a web browser makes a request to an application server (ZEO
   client) that makes a change and then makes a request to another
   application server that depends on the change.

   Setting this to True makes transactions a little slower because of
   the added server round trip.  For transactions that don't otherwise
   need to access the storage server, the impact can be significant.

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
===========================

ZODB databases and storages can be configured using configuration
files, or strings (extracted from configuration files).  They use the
same syntax as the server configuration files described above, but
with different sections and options.

An application that used ZODB might configure it's database using a
string like::

  <zodb>
     cache-size 1000MB

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
     cache-size 1000MB

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
   reduction. The default is 10 (percent).  This controls how often to
   remove blobs from the cache.

read-only
   Set to true for a read-only connection.

   If false (the default), then request a read/write connection.

   This option is ignored if ``read_only_fallback`` is set to a true value.

read-only-fallback
   Set to true, then prefer a read/write connection, but be willing to
   use a read-only connection.  This defaults to a false value.

   If ``read_only_fallback`` is set, then ``read_only`` is ignored.

server-sync
   Sets the ``server_sync`` option described above.

wait-timeout
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

client-label
   A short string to display in *server* logs for an event relating to
   this client. This can be helpful when debugging.


Client SSL configuration
------------------------

An ``ssl`` subsection can be used to enable and configure SSL, as in::

  %import ZEO

  <clientstorage>
    server zeo.example.com8200
    <ssl>
    </ssl>
  </clientstorage>

In the example above, SSL is enabled in it's simplest form:

- The client expects the server to have a signed certificate, which the
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
  to authenticate.  (See the ``cafile`` and ``capath``
  parameters in the Python documentation for
  ``ssl.SSLContext.load_verify_locations``.)

  If this setting is used then certificate authentication is
  used to authenticate the server.  The server must be configured
  with one of the certificates supplied using this setting.

check-hostname
  This is a boolean setting that defaults to true. Verify the
  host name in the server certificate is as expected.

server-hostname
  The expected server host name.  This defaults to the host name
  used in the server address.  This option must be used when
  ``check-hostname`` is true and when a server address has no host
  name (localhost, or unix domain socket) or when there is more
  than one server and server hostnames differ.

  Using this setting implies a true value for the ``check-hostname`` setting.
