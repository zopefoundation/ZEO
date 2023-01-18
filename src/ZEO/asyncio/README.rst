================================
asyncio-based networking for ZEO
================================

This package provides the networking interface for ZEO. It provides a
somewhat RPC-like API.

Notes
=====

Sending data immediately: asyncio vs asyncore
--------------------------------------------

The previous ZEO networking implementation used the ``asyncore`` library.
When writing with asyncore, writes were done only from the event loop.
This meant that when sending data, code would have to "wake up" the
event loop, typically after adding data to some sort of output buffer.

Asyncio takes an entirely different and saner approach.  When an
application wants to send data, it writes to a transport.  All
interactions with a transport (in a correct application) are from the
same thread, which is also the thread running any event loop.
Transports are always either idle or sending data.  When idle, the
transport writes to the outout socket immediately. If not all data
isn't sent, then it buffers it and becomes sending.  If a transport is
sending, then we know that the socket isn't ready for more data, so
``write`` can just buffer the data. There's no point in waking up the
event loop, because the socket will do so when it's ready for more
data.

An exception to the paragraph above occurs when operations cross
threads, as occures for most client operations and when a transaction
commits on the server and results have to be sent to other clients. In
these cases, a call_soon_threadsafe method is used which queues an
operation and has to wake up an event loop to process it.

Server threading
----------------

ZEO server implementation always uses single networking thread that serves all
clients. In other words the server is single-threaded.

Historically ZEO switched to a multi-threaded implementation several years ago
because it was found to improve performance for large databases using
magnetic disks. Because client threads are always working on behalf of
a single client, there's not really an issue with making blocking
calls, such as executing slow I/O operations.

Initially, the asyncio-based implementation used a multi-threaded
server.  A simple thread accepted connections and handed accepted
sockets to ``create_connection``. This became a problem when SSL was
added because ``create_connection`` sets up SSL conections as client
connections, and doesn't provide an option to create server
connections.

In response, Jim created an ``asyncio.Server``-based implementation.
This required using a single thread.  This was a pretty trivial
change, however, it led to the tests becoming unstable to the point
that it was impossible to run all tests without some failing.  One
test was broken due to a ``asyncio.Server`` `bug
<http://bugs.python.org/issue27386>`_.  It's unclear whether the test
instability is due to ``asyncio.Server`` problems or due to latent
test (or ZEO) bugs, but even after beating the tests mostly into
submission, tests failures are more likely when using
``asyncio.Server``.  Beatings will continue.

While fighting test failures using ``asyncio.Server``, the
multi-threaded implementation was updated to use a monkey patch to
allow it to create SSL server connections.  Aside from the real risk of a
monkey patch, this works very well.

Both implementations seemed to perform about the same.

Over the time single-threaded server mode became the default, and, given that
multi-threaded implementation did not provide any speed advantages, it was
eventually deprecated and scheduled for removal to ease maintenance burden.

Finally, the multi-threaded server mode was removed, when it was found that this
mode had concurrency bugs that lead to data corruptions. See `issue 209
<https://github.com/zopefoundation/ZEO/issues/209>` for details.
