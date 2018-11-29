Changelog
=========

5.2.1 (unreleased)
------------------

- Add support for Python 3.7.


5.2.0 (2018-03-28)
------------------

- Fixed: The quickstart/ad-hoc/play ZEO server relied on test
  dependencies. See `issue 105
  <https://github.com/zopefoundation/ZEO/issues/105>`_.

- Disallow passing strings as addresses to ClientStorage under Windows
  because string addresses are used for unix-domain sockets, which
  aren't supported on Windows. See `issue 107
  <https://github.com/zopefoundation/ZEO/issues/107>`_.

- Renamed all ``async`` attributes to ``async_`` for compatibility
  with Python 3.7. See `issue 104
  <https://github.com/zopefoundation/ZEO/issues/104>`_.

- Fixed to work with some changes made in ZODB 5.4.0.

  Client-side updates are incuded for ZODB 5.4.0 or databases that
  already had ``zodbpickle.binary`` OIDs. See `issue 113
  <https://github.com/zopefoundation/ZEO/issues/113>`_.

- ZEO now uses pickle protocol 3 for both Python 2 and Python 3.
  (Previously protocol 1 was used for Python 2.) This matches the
  change in ZODB 5.4.0.

5.1.2 (2018-03-27)
------------------

- Fix: ZEO didn't work with a change in ZODB 5.4.0.

  (Allow ``zodbpickle.binary`` to be used in RPC requests, which is
  necessary for compatibility with ZODB 5.4.0 on Python 2. See `issue
  107 <https://github.com/zopefoundation/ZEO/issues/107>`_.)

5.1.1 (2017-12-18)
------------------

- All classes are new-style classes on Python 2 (they were already
  new-style on Python 3). This improves performance on PyPy. See
  `issue 86 <https://github.com/zopefoundation/ZEO/pull/86>`_.

- Fixed removing UNIX socket files under Python 2 with ZConfig 3.2.0.
  See `issue 90 <https://github.com/zopefoundation/ZEO/issues/90>`_.

5.1.0 (2017-04-03)
------------------

- Added support for serializing ZEO messages using `msgpack
  <http://msgpack.org/index.html>`_ rather than pickle.  This helps
  pave the way to supporting `byteserver
  <https://github.com/jimfulton/byteserver>`_, but it also allows ZEO
  servers to support Python 2 or 3 clients (but not both at the same
  time) and may provide a small performance improvement.

- Possibly fixed the deprecated and untested zeoctl script.

- Removed zeopasswd, which no longer makes sense given that ZEO
  authentication was removed, in favor of SSL.

5.0.4 (2016-11-18)
------------------

- Fixed: ZEO needed changes to work with recent transaction changes.

  ZEO now works with the latest versions of ZODB and transaction

5.0.3 (2016-11-18)
------------------

- Temporarily require non-quite-current versions of ZODB and
  transaction until we can sort out some recent breakage.

5.0.2 (2016-11-02)
------------------

- Provide much better performance on Python 2.

- Provide better error messages when pip tries to install ZEO on an
  unsupported Python version. See `issue 75
  <https://github.com/zopefoundation/ZEO/issues/75>`_.

5.0.1 (2016-09-06)
------------------

Packaging-related doc fix

5.0.0 (2016-09-06)
------------------

This is a major ZEO revision, which replaces the ZEO network protocol
implementation.

New features:

- SSL support

- Optional client-side conflict resolution.

- Lots of mostly internal clean ups.

- ``ClientStorage``server-sync`` configuration option and
  ``server_sync`` constructor argument to force a server round trip at
  the beginning of transactions to wait for any outstanding
  invalidations at the start of the transaction to be delivered.

- Client disconnect errors are now transient errors.  When
  applications retry jobs that raise transient errors, jobs (e.g. web
  requests) with disconnect errors will be retried. Together with
  blocking synchronous ZEO server calls for a limited time while
  disconnected, this change should allow brief disconnections due to
  server restart to avoid generating client-visible errors (e.g. 500
  web responses).

- ClientStorage prefetch method to prefetch oids.

  When oids are prefetched, requests are made at once, but the caller
  doesn't block waiting for the results.  Rather, then the caller
  later tries to fetch data for one of the object ids, it's either
  delivered right away from the ZEO cache, if the prefetch for the
  object id has completed, or the caller blocks until the inflight
  prefetch completes. (No new request is made.)

Dropped features:

- The ZEO authentication protocol.

  This will be replaced by new authentication mechanims leveraging SSL.

- The ZEO monitor server.

- Full cache verification.

- Client suppprt for servers older than ZODB 3.9

- Server support for clients older than ZEO 4.2.0

5.0.0b0 (2016-08-18)
--------------------

- Added a ``ClientStorage`` ``server-sync`` configuration option and
  ``server_sync`` constructor argument to force a server round trip at
  the beginning of transactions to wait for any outstanding
  invalidations at the start of the transaction to be delivered.

- When creating an ad hoc server, a log file isn't created by
  default. You must pass a ``log`` option specifying a log file name.

- The ZEO server register method now returns the storage last
  transaction, allowing the client to avoid an extra round trip during
  cache verification.

- Client disconnect errors are now transient errors.  When
  applications retry jobs that raise transient errors, jobs (e.g. web
  requests) with disconnect errors will be retried. Together with
  blocking synchronous ZEO server calls for a limited time while
  disconnected, this change should allow brief disconnections due to
  server restart to avoid generating client-visible errors (e.g. 500
  web responses).

- Fixed bugs in using the ZEO 5 client with ZEO 4 servers.

5.0.0a2 (2016-07-30)
--------------------

- Added the ability to pass credentials when creating client storages.

  This is experimental in that passing credentials will cause
  connections to an ordinary ZEO server to fail, but it facilitates
  experimentation with custom ZEO servers. Doing this with custom ZEO
  clients would have been awkward due to the many levels of
  composition involved.

  In the future, we expect to support server security plugins that
  consume credentials for authentication (typically over SSL).

  Note that credentials are opaque to ZEO. They can be any object with
  a true value.  The client mearly passes them to the server, which
  will someday pass them to a plugin.

5.0.0a1 (2016-07-21)
--------------------

- Added a ClientStorage prefetch method to prefetch oids.

  When oids are prefetched, requests are made at once, but the caller
  doesn't block waiting for the results.  Rather, then the caller
  later tries to fetch data for one of the object ids, it's either
  delivered right away from the ZEO cache, if the prefetch for the
  object id has completed, or the caller blocks until the inflight
  prefetch completes. (No new request is made.)

- Fixed: SSL clients of servers with signed certs didn't load default
  certs and were unable to connect.

5.0.0a0 (2016-07-08)
--------------------

This is a major ZEO revision, which replaces the ZEO network protocol
implementation.

New features:

- SSL support

- Optional client-side conflict resolution.

- Lots of mostly internal clean ups.

Dropped features:

- The ZEO authentication protocol.

  This will be replaced by new authentication mechanims leveraging SSL.

- The ZEO monitor server.

- Full cache verification.

- Client suppprt for servers older than ZODB 3.9

- Server support for clients older than ZEO 4.2.0

4.2.0 (2016-06-15)
------------------

- Changed loadBefore to operate more like load behaved, especially
  with regard to the load lock.  This allowes ZEO to work with the
  upcoming ZODB 5, which used loadbefore rather than load.

  Reimplemented load using loadBefore, thus testing loadBefore
  extensively via existing tests.

- Other changes to work with ZODB 5 (as well as ZODB 4)

- Fixed: the ZEO cache loadBefore method failed to utilize current data.

- Drop support for Python 2.6 and 3.2.

- Fix AttributeError: 'ZEOServer' object has no attribute 'server' when
  StorageServer creation fails.

4.2.0b1 (2015-06-05)
--------------------

- Add support for PyPy.

4.1.0 (2015-01-06)
------------------

- Add support for Python 3.4.

- Added a new ``ruok`` client protocol for getting server status on
  the ZEO port without creating a full-blown client connection and
  without logging in the server log.

- Log errors on server side even if using multi threaded delay.

4.0.0 (2013-08-18)
------------------

- Avoid reading excess random bytes when setting up an auth_digest session.

- Optimize socket address enumeration in ZEO client (avoid non-TCP types).

- Improve Travis CI testing support.

- Assign names to all threads for better runtime debugging.

- Fix "assignment to keyword" error under Py3k in 'ZEO.scripts.zeoqueue'.

4.0.0b1 (2013-05-20)
--------------------

- Depend on ZODB >= 4.0.0b2

- Add support for Python 3.2 / 3.3.

4.0.0a1 (2012-11-19)
--------------------

First (in a long time) separate ZEO release.

Since ZODB 3.10.5:

- Storage servers now emit Serving and Closed events so subscribers
  can discover addresses when dynamic port assignment (bind to port 0)
  is used. This could, for example, be used to update address
  information in a ZooKeeper database.

- Client storages have a method, new_addr, that can be used to change
  the server address(es). This can be used, for example, to update a
  dynamically determined server address from information in a
  ZooKeeper database.
