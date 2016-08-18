Changelog
=========

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
