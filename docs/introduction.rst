============
Introduction
============

There are several features that affect the behavior of
ZEO.  This section describes how a few of these features
work.  Subsequent sections describe how to configure every option.

Client cache
============

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
==================

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
clients are disconnected for a long time, then this can put a lot of
load on the server.

Transaction timeouts
====================

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
=====================

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
===

ZEO supports the use of SSL connections between servers and clients,
including certificate authentication.  We're still understanding use
cases for this, so details of operation may change.
