==============================
Response ordering requirements
==============================

ZEO servers are logically concurrent because they serve multiple
clients in multiple threads.  Because of this, we have to make sure
that information about object histories remain consistent.

An object history is a sequence of object revisions. Each revision has
a tid, which is essentially a time stamp.

We load objects using either ``load``, which returns the current
object. or ``loadBefore``, which returns the object before a specific time/tid.

When we cache revisions, we record the tid and the next/end tid, which
may be None. The end tid is important for choosing a revision for
``loadBefore``, as well as for determining whether a cached value is
current, for ``load``.

Because the client and server are multi-threaded, the client may see
data out of order.  Let's consider some scenarios.  In these
scenarios

Scenarios
=========

When considering ordering scenarioes, we'll consider 2 different
client behaviors, traditional (T) and loadBefore (B).

The *traditional* behaviors is that used in ZODB 4.  It uses the storage
``load(oid)`` method to load objects if it hasn't seen an invalidation
for the object.  If it has seen an invalidation, it uses
``loadBefore(oid, START)``, where ``START`` is the transaction time of
the first invalidation it's seen.  If it hasn't seen an invalidation
*for an object*, it uses ``load(oid)`` and then checks again for an
invalidation. If it sees an invalidation, then it retries using
``loadBefore``.  This approach **assumes that invalidations for a tid
are returned before loads for a tid**.

The *loadBefore* behavior, used in ZODB5, always determines
transaction start time, ``START`` at the beginning of a transaction by
calling the storage's ``sync`` method and then querying the storage's
``lastTransaction`` method (and adding 1). It loads objects
exclusively using ``loadBefore(oid, START)``.

Scenario 1, Invalidations seen after loads for transaction
----------------------------------------------------------

This scenario could occur because the commits are for a different
client, and a hypothetical; server doesn't block loads while
committing, or sends invalidations in a way that might delay them (but
not send them out of order).

T1

  - client starts a transaction

  - client load(O1) gets O1-T1

  - client load(O2)

  - Server commits O2-T2

  - Server loads (O2-T2)

  - Client gets O2-T2, updates the client cache, and completes load

  - Client sees invalidation for O2-T2.  If the
    client is smart, it doesn't update the cache.

    The transaction now has inconsistent data, because it should have
    loaded whatever O2 was before T2.  Because the invalidation came
    in after O2 was loaded, the load was unaffected.

 B1

  - client starts a transaction. Sets START to T1+1

  - client loadBefore(O1, T1+1) gets O1-T1, T1, None

  - client loadBefore(O2, T1+1)

  - Server commits O2-T2

  - Server loadBefore(O2, T1+1) -> O2-T0-T2

    (assuming that the revision of O2 before T2 was T0)

  - Client gets O2-T0-T2, updates cache.

  - Client sees invalidation for O2-T2. No update to the cache is
    necessary.

  In this scenario, loadBefore prevents reading incorrect data.

A variation on this scenario is that client sees invalidations
tpc_finish in another thread after loads for the same transaction.

Scenario 2, Client sees invalidations for later transaction before load result
------------------------------------------------------------------------------

T2

  - client starts a transaction

  - client load(O1) gets O1-T1

  - client load(O2)

  - Server loads (O2-T0)

  - Server commits O2-T2

  - Client sees invalidation for O2-T2.  O2 isn't in the cache, so
    nothing to do.

  - Client gets O2-T0, updates the client cache, and completes load

    The cache is now incorrect. It has O2-T0-None, meaning it thinks
    O2-T0 is current.

    The transaction is OK, because it got a consistent value for O2.

B2

  - client starts a transaction. Sets START to T1+1

  - client loadBefore(O1, T1+1) gets O1-T1, T1, None

  - client loadBefore(O2, T1+1)

  - Server loadBefore(O2, T1+1) -> O2-T0-None

  - Server commits O2-T2

  - Client sees invalidation for O2-T2.  O2 isn't in the cache, so
    nothing to do.

  - Client gets O2-T0-None, and completes load

    ZEO 4 doesn't cache loadBefore results with no ending transaction.

    Assume ZEO 5 updates the client cache.

    For ZEO 5, the cache is now incorrect. It has O2-T0-None, meaning
    it thinks O2-T0 is current.

    The transaction is OK, because it got a consistent value for O2.

  In this case, ``loadBefore`` didn't prevent an invalid cache value.

Scenario 3, client sees invalidation after lastTransaction result
------------------------------------------------------------------

(This doesn't effect the traditional behavior.)

B3

  - The client cache has a last tid of T1.

  - ZODB calls sync() then calls lastTransaction.  Is so configured,
    ZEO calls lastTransaction on the server. This is mainly to make a
    round trip to get in-flight invalidations. We don't necessarily
    need to use the value. In fact, in protocol 5, we could just add a
    sync method that just makes a round trip, but does nothing else.

  - Server commits O1-T2, O2-T2.

  - Server reads and returns T2. (It doesn't mater what it returns

  - client sets START to T1+1, because lastTransaction is based on
    what's in the cache, which is based on invalidations.

  - Client loadBefore(O1, T2+1), finds O1-T1-None in cache and uses
    it.

  - Client gets invalidation for O1-T2. Updates cache to O1-T1-T2.

  - Client loadBefore(O2, T1+1), gets O2-T1-None

  This is OK, as long as the client doesn't do anything with the
  lastTransaction result in ``sync``.

Implementation notes
====================

ZEO 4
-----

The ZEO 4 server sends data to the client in correct order with
respect to loads and invalidations (or tpc_finish results). This is a
consequence of the fact that invalidations are sent in a callback
called when the storage lock is held, blocking loads while committing,
and, fact that client requests, for a particular client, are
handled by a single thread on the server, and that all output for a
client goes through a thread-safe queue.

Invalidations are sent from different threads than clients. Outgoing
data is queued, however, using Python lists, which are protected by
the GIL.  This means that the serialization provided though storage
locks is preserved by the way that server outputs are queued.  **The
queueing mechanism is in part a consequence of the way asyncore, used
by ZEO4, works.**

In ZEO 4 clients, invalidations and loads are handled by separate
threads. This means that even though data arive in order, they may not
be processed in order,

T1
  The existing servers mitigate this by blocking loads while
  committing. On the client, this is still a potential issue because loads
  and invalidations are handled by separate threads, however, locks are
  used on the client to assure that invalidations are processed before
  blocked loads complete.

T2
  Existing storage servers serialize commits (and thus sending of
  invalidations) and loads. As with scenario T1, threading on the
  client can cause load results and invalidations to be processed out
  of order.  To mitigate this, the client uses a load lock to track
  when loads are invalidated while in flight and doesn't save to the
  cache when they are.  This is bad on multiple levels. It serializes
  loads even when there are multiple threads.  It may prevent writing
  to the cache unnecessarily, if the invalidation is for a revision
  before the one that was loaded.

B2
  Here, we avoid incorrect returned values and incorrect cache at the
  cost of caching nothing.

  ZEO 4.2.0 addressed this by using the same locking strategy for
  ``loadBefore`` that was used for ``load``, thus mitigating B2 the
  same way it mitigates T2.

ZEO 5
-----

In ZEO(/ZODB) 5, we want to get more concurrency, both on the client,
and on the server.  On the client, cache invalidations and loads are
done by the same thread, which makes things a bit simpler. This let's
us get rid of the client load lock and prevents the scenarios above
with existing servers.

On the client, we'd like to stop serializing loads and commits.  We'd
like commits (tpc_finish calls) to be in flight with loads (and with
other commits).  In the current protocol, tpc_finish, load and
loadBefore are all synchronous calls that are handled by a single
thread on the server, so these calls end up being serialized on the
server anyway.

The server-side hndling of invalidations is a bit tricker in ZEO 5
because there isn't a thread-safe queue of outgoing messages in ZEO 5
as there was in ZEO 4.  The natural approach in ZEO 5 would be to use
asyncio's ``call_soon_threadsafe`` to send invalidations in a client's
thread.  This could easily cause invalidations to be sent after loads.
As shown above, this isn't a problem for ZODB 5, at least assuming
that invalidations arrive in order.  This would be a problem for
ZODB 4.  For this reason, we require ZODB 5 for ZEO 5.

Note that this approach can't cause invalidations to be sent early,
because they could only be sent by the thread that's busy loading, so
scenario 2 wouldn't happen.

B2
  Because the server send invalidations by calling
  ``call_soon_threadsafe``, it's impoossible for invalidations to be
  send while a load request is being handled.

The main server opportunity is allowing commits for separate oids to
happen concurrently. This wouldn't effect the invalidation/load
ordering though.

It would be nice not to block loads while making tpc_finish calls, but
storages do this anyway now, so there's nothing to be done about it
now.  Storage locking requirements aren't well specified, and probably
should be rethought in light of ZODB5/loadBefore.
