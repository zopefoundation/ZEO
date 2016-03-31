Changelog
=========

4.2.0 (unreleased)
------------------

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
