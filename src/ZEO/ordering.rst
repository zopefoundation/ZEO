==============================
Response ordering requirements
==============================

ZEO servers are logically concurrent because they serve multiple
clients in multiple threads.  Because of this, we have to make sure
that information about object histories remain consistent.

An object history is a sequence of object revisions. Each revision has
a tid, which is essentially a time stamp.

We load objects using either ``load``, which returns the current
object. or loadBefore, which returns the object before a specific time/tid.

When we cache revisions, we record the tid and the next/end tid, which
may be None. The end tid is important for choosing a revision for
loadBefore, as well as for determining whether a cached value is
current, for load.

Because the client and server are multi-threaded, the client may see
data out of order.  Let's consider some scenarios.  In these
scenarios, we'll consider a single object with revisions t1, t2, etc.
We consider loading pretty generically, as bath load and loadBefore
are similar in that they may have data about current revisions.

Scenarios
=========

S1
  Client sees load results before earlier invalidations

  - server commits t1

  - server commits t2

  - client makes load request, server loads t2

  - client gets load result for t2

  - client gets invalidation for t1, client should ignore

  - client gets invalidation for t2, client should ignore

  This scenario could occur because the commits are for a different
  client, and a hypothetical; server doesn't block loads while
  committing. This situation is pretty easy to deal with, as we just
  ignore invalidations for earlier revisions.

  Note that invalidations will never come out of order from the server.

S2
  Client sees load results before finish results (for another client thread)

  - Client commits, server commits t1

  - Client commits, server commits t2

  - Client makes load request, server reads t2.

  - Client receives t2 in load result.

  - Client receives t1 in tpc_finish result, doesn't invalidate anything

  - Client receives t2 in tpc_finish result, doesn't invalidate anything

  This scenario is equivalent to S1.

S3
  Client sees invalidations before load results.

  - Client loads, storage reads t1.

  - server commits t2

  - Client receives invalidation for t2.

  - Client receives load result for t1.

  This scenario is worrisome because the data that needs to be
  invalidated isn't present when the invalidation arrives.

S4
  Client sees commit results before load results.

  - Client loads, storage reads t1.

  - Client commits, storage commits t2.

  - Client receives t2 in tpc_finish result.

  - Client receives load result for t1.

  This scenario is equivalent to S3.

Implementation notes
===================

First, it's worth noting that the server sends data to the client in
correct order with respect to loads and invalidations (or tpc_finish
results). This is a consequence of the fact that invalidations are
sent in a callback called when the storage lock is held, blocking
loadd while committing, and the fact that client requests, for a
particular client, are handled by a single thread on the server.

Invalidations are sent from different threads that clients.  Outgoing
data is queued, however, using Python lists, which are protected by
the GIL.  This means that the serialization provided though storage
locks is preserved by the way that server outputs are queued.


ZEO 4
-----

In ZEO 4, invalidations and loads are handled by separate
threads. This means that even though data arive in order, they may not
be processed in order,

S1
  The existing servers mitigate this by blocking loads while
  committing. On the client, this is still a potential issue because loads
  and invalidations are handled by separate threads.

  The client cache is conservative because it always forgets current data in
  memory when it sees an invalidation data for an object.

  The client gets this scenario wrong, in an edge case, because it
  checks for invalidations matching the current tid, but not
  invalidations before the current tid.  If the thread handling
  invalidations was slow enough for this scenario to occur, then the
  cache would end up with an end tid < a starting tid. This is
  probably very unlikely.

S2
  The existing clients prevent this by serializing commits with each
  other (only one at a time on the client) and with loads.

S3
  Existing storages serialize commits (and thus sending of
  invalidations) and loads. As with scenario S1, threading on the
  client can cause load results and invalidations to be processed out
  of order.  To mitigate this, the client uses a load lock to track
  when loads are invalidated while in flight and doesn't save to the
  cache when they are.  This is bad on multiple levels. It serializes
  loads even when there are multiple threads.  It may prevent writing
  to the cache unnecessarily, if the invalidation is for a revision
  before the one that was loaded.

S4
  As with S2, clients mitigate this by preventing simultaneous loads
  and commits.

ZEO 5
-----

In ZEO(/ZODB) 5, we want to get more concurrency, both on the client,
and on the server.  On the client, cache invalidations and loads are
done by the same thread, which makes things a bit simpler. This let's
us get rid of the client load lock and prevents the scenarios above
with existing servers and storages.

On the client, we'd like to stop serializing loads and commits.  We'd
like commits (tpc_finish calls) to in flight with loads (and with
other commits).  In the current protocol, tpc_finish, load and
loadBefore are all synchronous calls that are handled by a single
thread on the server, so these calls end up being serialized on the
server.

If we ever allowed multiple threads to service client requests, then
we'd need to consider scenario S4, but this isn't an issue now (or for
the foreseeable future).

The main server opportunity is allowing commits for separate oids to
happen concurrently. This wouldn't effect the invalidation/load
ordering though, assuming we continued to block loading an oid while
it was being committed in tpc_finish.

We could also allow loads to proceed while invalidations are being
queued for an object. Queuing invalidations is pretty fast though. It's
not clear that this would be much of a win.  This probably isn't worth
fooling with for now. If we did want to relax this, we could, on the
client, track invalidations for outstanding load requests and adjust
how we wrote data to the cache accordingly.  Again, we won't bother in
the short term.

So, for now, we can rely on the server sending clients
properly-ordered loads and invalidations.  Also, because invalidations
and loads will be performed by a single thread on the client, we can
count on the ordering being preserved on the client.

