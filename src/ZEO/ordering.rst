==============================
Response ordering requirements
==============================

ZEO servers are logically concurrent because they server multiple
clients in multiple threads.  Because of this, we have to make sure
that information about object histories remain consistent.

An object history is a sequence of object revisions. Each revision has
a tid, which is essentially a time stamp.

We load objects using either load, which retirns the current
object. or loadBefore, which returns the object before a specic time/tid.

When we cache revisions, we record the tid and the next/end tid, which
may be None. The end tid is important for choosing a revision for
loadBefore, as well as for determining whether a cached value is
current, for load.

Because the client and server are multi-threaded, the client may see
data out of order.  Let's consider some scenarios.  In these
scenarious, we'll consider a single object with revisions t1, t2, etc.
We consider loading pretty generically, as bith load and loadBefore
are similar in that they may have data about current revisions.

Scenarios
=========

S1
  Client sees load results before later invalidations

  - server commits t1

  - server commits t2

  - client makes load request, server loads t2

  - client gets load result for t2

  - client gets invalidation for t1, client should ignore

  - client gets invalidation for t2, client should ignore

  This scenario could occur because the commits are for a different
  client, and the server doesn't block loads while committing. This
  situation is pretty easy to deal with, as we just ignore
  invalidations for earlier revisions.

  Note that invalidations will never come out of order from the server.

S2
  Client sees load results before finish results (for another client thread)

  - Client commits, server commits t1

  - Client commits, server commits t2

  - Client makes load request, server reads t2.

  - Client receives t2 in load result.

  - Client recieves t1 in tpc_finish result, doesn't invalidate anything

  - Client recieves t2 in tpc_finish result, doesn't invalidate anything

  This scenario is equivalent to S1.

S3
  Client sees invalidations before load results.

  - Client loads, storage reads t1.

  - server commits t2

  - Client recieves invalidation for t2.

  - Client recieves load result for t1.

  This scenario id worrisome because the data that needs to be
  invalidated isn't present when the invalidation arrives.

S4
  Client sees commit resulrs before load results.

  - Client loads, storage reads t1.

  - Client commits, storage commits t2.

  - Client recieves t2 in tpc_finish result.

  - Client recieves load result for t1.

  This scenario is equivalent to S3.

Implemenation notes
===================

ZEO 4
-----

S1
  The existing servers mitigates this by blocking loads while
  committing.  Because this is easy to deal with, maybe in the future
  the server will allow loads to be done before invalidations are
  sent. On the client, this is still a potential issue because loads
  and invalidations are handled by separate threads.

  The client is conservative because it always forgets current data in
  memory when it sees an invalidation data for an object.

  The client gets the scenario wrong because it checks for
  invalidations matching the current tid, but not invalidations before
  the current tid.  If the thread handling invalidations was slow
  enough for this scenario to occur, then the cache would end up with
  an end tid < a starting tid. This is probably very unlikely.

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

Note that on the server, invalidations are sent from different threads
that clients.  Outgoing data is queued, however, using Python lists,
which are protected by the GIL.  This means that the serialization
provided though storage locks is preserved by the way that server
outputs are queued.

ZEO 5
-----

In ZEO(/ZODB) 5, we want to get more conccurency, both on the client,
and on the server.  On the client, cache invalidations and loads are
done by the same thread, which makes things a bit simpler. This let's
us get rid of the client load lock and prevents the scenarios above
with existing servers and storages.

Going forward, however, we'd like to allow greater server
concurrency.  The main opportunity here is allowing commits for
separate oids to happen concurrently. This wouldn't effect the
invalidation/load ordering though, assuming we blocked loading an oid
while it was being committed in tpc_finish.

We could also allow loads to proceed while invalidations are being
queued for an object. Queuing invalidations is pretty fast though. It's
not clear that this would be much of a win.  This probaby isn't worth
fooling with for now. If we did want to relax this, we could, on the
client, track invalidations for outstanding load requests and adjust
how we wrote data to the cache accordingly.  Again, we won't bother in
the short term.

So, for now, we can rely on the server sending clients
properly-ordered loads and invalidations.  Also, because invalidations
and loads will be performed by a single thread on the client, we can
count on the ordering being preserved on the client.

