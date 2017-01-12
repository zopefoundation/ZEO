from .._compat import PY3

if PY3:
    import asyncio
else:
    import trollius as asyncio

from zope.testing import setupstack
from concurrent.futures import Future
import mock
from ZODB.POSException import ReadOnlyError
from ZODB.utils import maxtid

import collections
import logging
import struct
import unittest

from ..Exceptions import ClientDisconnected, ProtocolError

from .testing import Loop
from .client import ClientRunner, Fallback
from .server import new_connection, best_protocol_version
from .marshal import encoder, decoder

class Base(object):

    enc = b'Z'
    seq_type = list

    def setUp(self):
        super(Base, self).setUp()
        self.encode = encoder(self.enc)
        self.decode = decoder(self.enc)

    def unsized(self, data, unpickle=False):
        result = []
        while data:
            size, message = data[:2]
            data = data[2:]
            self.assertEqual(struct.unpack(">I", size)[0], len(message))
            if unpickle:
                message = self.decode(message)
            result.append(message)

        if len(result) == 1:
            result = result[0]
        return result

    def parse(self, data):
        return self.unsized(data, True)

    target = None
    def send(self, method, *args, **kw):
        target = kw.pop('target', self.target)
        called = kw.pop('called', True)
        no_output = kw.pop('no_output', True)
        self.assertFalse(kw)

        self.loop.protocol.data_received(
            sized(self.encode(0, True, method, args)))
        if target is not None:
            target = getattr(target, method)
            if called:
                target.assert_called_with(*args)
                target.reset_mock()
            else:
                self.assertFalse(target.called)
        if no_output:
            self.assertFalse(self.loop.transport.pop())

    def pop(self, count=None, parse=True):
        return self.unsized(self.loop.transport.pop(count), parse)

class ClientTests(Base, setupstack.TestCase, ClientRunner):

    maxDiff = None

    def tearDown(self):
        self.client.close()
        super(ClientTests, self)

    def start(self,
              addrs=(('127.0.0.1', 8200), ), loop_addrs=None,
              read_only=False,
              finish_start=False,
              ):
        # To create a client, we need to specify an address, a client
        # object and a cache.

        wrapper = mock.Mock()
        self.target = wrapper
        cache = MemoryCache()
        self.set_options(addrs, wrapper, cache, 'TEST', read_only, timeout=1)

        # We can also provide an event loop.  We'll use a testing loop
        # so we don't have to actually make any network connection.
        loop = Loop(addrs if loop_addrs is None else loop_addrs)
        self.setup_delegation(loop)
        self.assertFalse(wrapper.notify_disconnected.called)
        protocol = loop.protocol
        transport = loop.transport

        if finish_start:
            protocol.data_received(sized(self.enc + b'3101'))
            self.assertEqual(self.pop(2, False), self.enc + b'3101')
            self.respond(1, None)
            self.respond(2, 'a'*8)
            self.pop(4)
            self.assertEqual(self.pop(), (3, False, 'get_info', ()))
            self.respond(3, dict(length=42))

        return (wrapper, cache, self.loop, self.client, protocol, transport)

    def respond(self, message_id, result, async=False):
        self.loop.protocol.data_received(
            sized(self.encode(message_id, async, '.reply', result)))

    def wait_for_result(self, future, timeout):
        if future.done() and future.exception() is not None:
            raise future.exception()
        return future

    def testClientBasics(self):

        # Here, we'll go through the basic usage of the asyncio ZEO
        # network client.  The client is responsible for the core
        # functionality of a ZEO client storage.  The client storage
        # is largely just a wrapper around the asyncio client.

        wrapper, cache, loop, client, protocol, transport = self.start()
        self.assertFalse(wrapper.notify_disconnected.called)

        # The client isn't connected until the server sends it some data.
        self.assertFalse(client.connected.done() or transport.data)

        # The server sends the client it's protocol. In this case,
        # it's a very high one.  The client will send it's highest that
        # it can use.
        protocol.data_received(sized(self.enc + b'99999'))

        # The client sends back a handshake, and registers the
        # storage, and requests the last transaction.
        self.assertEqual(self.pop(2, False), self.enc + b'5')
        self.assertEqual(self.pop(), (1, False, 'register', ('TEST', False)))

        # The client isn't connected until it initializes it's cache:
        self.assertFalse(client.connected.done() or transport.data)

        # If we try to make calls while the client is *initially*
        # connecting, we get an error. This is because some dufus
        # decided to create a client storage without waiting for it to
        # connect.
        self.assertRaises(ClientDisconnected, self.call, 'foo', 1, 2)

        # When the client is reconnecting, it's ready flag is set to False and
        # it queues calls:
        client.ready = False
        f1 = self.call('foo', 1, 2)
        self.assertFalse(f1.done())

        # If we try to make an async call, we get an immediate error:
        self.assertRaises(ClientDisconnected, self.async, 'bar', 3, 4)

        # The wrapper object (ClientStorage) hasn't been notified:
        self.assertFalse(wrapper.notify_connected.called)

        # Let's respond to the register call:
        self.respond(1, None)

        # The client requests the last transaction:
        self.assertEqual(self.pop(), (2, False, 'lastTransaction', ()))

        # We respond
        self.respond(2, 'a'*8)

        # After verification, the client requests info:
        self.assertEqual(self.pop(), (3, False, 'get_info', ()))
        self.respond(3, dict(length=42))

        # Now we're connected, the cache was initialized, and the
        # queued message has been sent:
        self.assert_(client.connected.done())
        self.assertEqual(cache.getLastTid(), 'a'*8)
        self.assertEqual(self.pop(), (4, False, 'foo', (1, 2)))

        # The wrapper object (ClientStorage) has been notified:
        wrapper.notify_connected.assert_called_with(client, {'length': 42})

        self.respond(4, 42)
        self.assertEqual(f1.result(), 42)

        # Now we can make async calls:
        f2 = self.async('bar', 3, 4)
        self.assert_(f2.done() and f2.exception() is None)
        self.assertEqual(self.pop(), (0, True, 'bar', (3, 4)))

        # Loading objects gets special handling to leverage the cache.
        loaded = self.load_before(b'1'*8, maxtid)

        # The data wasn't in the cache, so we made a server call:
        self.assertEqual(self.pop(), ((b'1'*8, maxtid), False, 'loadBefore', (b'1'*8, maxtid)))
        # Note load_before uses the oid as the message id.
        self.respond((b'1'*8, maxtid), (b'data', b'a'*8, None))
        self.assertEqual(loaded.result(), (b'data', b'a'*8, None))

        # If we make another request, it will be satisfied from the cache:
        loaded = self.load_before(b'1'*8, maxtid)
        self.assertEqual(loaded.result(), (b'data', b'a'*8, None))
        self.assertFalse(transport.data)

        # Let's send an invalidation:
        self.send('invalidateTransaction', b'b'*8, self.seq_type([b'1'*8]))

        # Now, if we try to load current again, we'll make a server request.
        loaded = self.load_before(b'1'*8, maxtid)

        # Note that if we make another request for the same object,
        # the requests will be collapsed:
        loaded2 = self.load_before(b'1'*8, maxtid)

        self.assertEqual(self.pop(), ((b'1'*8, maxtid), False, 'loadBefore', (b'1'*8, maxtid)))
        self.respond((b'1'*8, maxtid), (b'data2', b'b'*8, None))
        self.assertEqual(loaded.result(), (b'data2', b'b'*8, None))
        self.assertEqual(loaded2.result(), (b'data2', b'b'*8, None))

        # Loading non-current data may also be satisfied from cache
        loaded = self.load_before(b'1'*8, b'b'*8)
        self.assertEqual(loaded.result(), (b'data', b'a'*8, b'b'*8))
        self.assertFalse(transport.data)
        loaded = self.load_before(b'1'*8, b'c'*8)
        self.assertEqual(loaded.result(), (b'data2', b'b'*8, None))
        self.assertFalse(transport.data)
        loaded = self.load_before(b'1'*8, b'_'*8)

        self.assertEqual(self.pop(), ((b'1'*8, b'_'*8), False, 'loadBefore', (b'1'*8, b'_'*8)))
        self.respond((b'1'*8, b'_'*8), (b'data0', b'^'*8, b'_'*8))
        self.assertEqual(loaded.result(), (b'data0', b'^'*8, b'_'*8))

        # When committing transactions, we need to update the cache
        # with committed data.  To do this, we pass a (oid, data, resolved)
        # iteratable to tpc_finish_threadsafe.

        tids = []
        def finished_cb(tid):
            tids.append(tid)

        committed = self.tpc_finish(
            b'd'*8,
            [(b'2'*8, 'committed 2', False),
             (b'1'*8, 'committed 3', True),
             (b'4'*8, 'committed 4', False),
             ],
            finished_cb)
        self.assertFalse(committed.done() or
                         cache.load(b'2'*8) or
                         cache.load(b'4'*8))
        self.assertEqual(cache.load(b'1'*8), (b'data2', b'b'*8))
        self.assertEqual(self.pop(),
                         (5, False, 'tpc_finish', (b'd'*8,)))
        self.respond(5, b'e'*8)
        self.assertEqual(committed.result(), b'e'*8)
        self.assertEqual(cache.load(b'1'*8), None)
        self.assertEqual(cache.load(b'2'*8), ('committed 2', b'e'*8))
        self.assertEqual(cache.load(b'4'*8), ('committed 4', b'e'*8))
        self.assertEqual(tids.pop(), b'e'*8)

        # If the protocol is disconnected, it will reconnect and will
        # resolve outstanding requests with exceptions:
        loaded = self.load_before(b'1'*8, maxtid)
        f1 = self.call('foo', 1, 2)
        self.assertFalse(loaded.done() or f1.done())
        self.assertEqual(
            self.pop(),
            [((b'11111111', b'\x7f\xff\xff\xff\xff\xff\xff\xff'),
              False, 'loadBefore', (b'1'*8, maxtid)),
             (6, False, 'foo', (1, 2))],
            )
        exc = TypeError(43)

        self.assertFalse(wrapper.notify_disconnected.called)
        wrapper.notify_connected.reset_mock()
        protocol.connection_lost(exc)
        wrapper.notify_disconnected.assert_called_with()

        self.assertTrue(isinstance(loaded.exception(), ClientDisconnected))
        self.assertEqual(loaded.exception().args, (exc,))
        self.assertTrue(isinstance(f1.exception(), ClientDisconnected))
        self.assertEqual(f1.exception().args, (exc,))

        # Because we reconnected, a new protocol and transport were created:
        self.assert_(protocol is not loop.protocol)
        self.assert_(transport is not loop.transport)
        protocol = loop.protocol
        transport = loop.transport

        # and we have a new incomplete connect future:
        self.assertFalse(client.connected.done() or transport.data)

        # This time we'll send a lower protocol version.  The client
        # will send it back, because it's lower than the client's
        # protocol:
        protocol.data_received(sized(self.enc + b'310'))
        self.assertEqual(self.unsized(transport.pop(2)), self.enc + b'310')
        self.assertEqual(self.pop(), (1, False, 'register', ('TEST', False)))
        self.assertFalse(wrapper.notify_connected.called)

        # If the register response is a tid, then the client won't
        # request lastTransaction
        self.respond(1, b'e'*8)
        self.assertEqual(self.pop(), (2, False, 'get_info', ()))
        self.respond(2, dict(length=42))

        # Because the server tid matches the cache tid, we're done connecting
        wrapper.notify_connected.assert_called_with(client, {'length': 42})
        self.assert_(client.connected.done() and not transport.data)
        self.assertEqual(cache.getLastTid(), b'e'*8)

        # Because we were able to update the cache, we didn't have to
        # invalidate the database cache:
        self.assertFalse(wrapper.invalidateTransaction.called)

        # The close method closes the connection and cache:
        client.close()
        self.assert_(transport.closed and cache.closed)

        # The client doesn't reconnect
        self.assertEqual(loop.protocol, protocol)
        self.assertEqual(loop.transport, transport)

    def test_cache_behind(self):
        wrapper, cache, loop, client, protocol, transport = self.start()

        cache.setLastTid(b'a'*8)
        cache.store(b'4'*8, b'a'*8, None, '4 data')
        cache.store(b'2'*8, b'a'*8, None, '2 data')

        self.assertFalse(client.connected.done() or transport.data)
        protocol.data_received(sized(self.enc + b'3101'))
        self.assertEqual(self.unsized(transport.pop(2)), self.enc + b'3101')
        self.respond(1, None)
        self.respond(2, b'e'*8)
        self.pop(4)

        # We have to verify the cache, so we're not done connecting:
        self.assertFalse(client.connected.done())
        self.assertEqual(self.pop(), (3, False, 'getInvalidations', (b'a'*8, )))
        self.respond(3, (b'e'*8, [b'4'*8]))

        self.assertEqual(self.pop(), (4, False, 'get_info', ()))
        self.respond(4, dict(length=42))

        # Now that verification is done, we're done connecting
        self.assert_(client.connected.done() and not transport.data)
        self.assertEqual(cache.getLastTid(), b'e'*8)

        # And the cache has been updated:
        self.assertEqual(cache.load(b'2'*8),
                         ('2 data', b'a'*8)) # unchanged
        self.assertEqual(cache.load(b'4'*8), None)

        # Because we were able to update the cache, we didn't have to
        # invalidate the database cache:
        self.assertFalse(wrapper.invalidateCache.called)

    def test_cache_way_behind(self):
        wrapper, cache, loop, client, protocol, transport = self.start()

        cache.setLastTid(b'a'*8)
        cache.store(b'4'*8, b'a'*8, None, '4 data')
        self.assertTrue(cache)

        self.assertFalse(client.connected.done() or transport.data)
        protocol.data_received(sized(self.enc + b'3101'))
        self.assertEqual(self.unsized(transport.pop(2)), self.enc + b'3101')
        self.respond(1, None)
        self.respond(2, b'e'*8)
        self.pop(4)

        # We have to verify the cache, so we're not done connecting:
        self.assertFalse(client.connected.done())
        self.assertEqual(self.pop(), (3, False, 'getInvalidations', (b'a'*8, )))

        # We respond None, indicating that we're too far out of date:
        self.respond(3, None)

        self.assertEqual(self.pop(), (4, False, 'get_info', ()))
        self.respond(4, dict(length=42))

        # Now that verification is done, we're done connecting
        self.assert_(client.connected.done() and not transport.data)
        self.assertEqual(cache.getLastTid(), b'e'*8)

        # But the cache is now empty and we invalidated the database cache
        self.assertFalse(cache)
        wrapper.invalidateCache.assert_called_with()

    def test_multiple_addresses(self):
        # We can pass multiple addresses to client constructor
        addrs = [('1.2.3.4', 8200), ('2.2.3.4', 8200)]
        wrapper, cache, loop, client, protocol, transport = self.start(
            addrs, ())

        # We haven't connected yet
        self.assert_(protocol is None and transport is None)

        # There are 2 connection attempts outstanding:
        self.assertEqual(sorted(loop.connecting), addrs)

        # We cause the first one to fail:
        loop.fail_connecting(addrs[0])
        self.assertEqual(sorted(loop.connecting), addrs[1:])

        # The failed connection is attempted in the future:
        delay, func, args, _ = loop.later.pop(0)
        self.assert_(1 <= delay <= 2)
        func(*args)
        self.assertEqual(sorted(loop.connecting), addrs)

        # Let's connect the second address
        loop.connect_connecting(addrs[1])
        self.assertEqual(sorted(loop.connecting), addrs[:1])
        protocol = loop.protocol
        transport = loop.transport
        protocol.data_received(sized(self.enc + b'3101'))
        self.assertEqual(self.unsized(transport.pop(2)), self.enc + b'3101')
        self.respond(1, None)

        # Now, when the first connection fails, it won't be retried,
        # because we're already connected.
        # (first in later is heartbeat)
        self.assertEqual(sorted(loop.later[1:]), [])
        loop.fail_connecting(addrs[0])
        self.assertEqual(sorted(loop.connecting), [])
        self.assertEqual(sorted(loop.later[1:]), [])

    def test_bad_server_tid(self):
        # If in verification we get a server_tid behing the cache's, make sure
        # we retry the connection later.
        wrapper, cache, loop, client, protocol, transport = self.start()
        cache.store(b'4'*8, b'a'*8, None, '4 data')
        cache.setLastTid('b'*8)
        protocol.data_received(sized(self.enc + b'3101'))
        self.assertEqual(self.unsized(transport.pop(2)), self.enc + b'3101')
        self.respond(1, None)
        self.respond(2, 'a'*8)
        self.pop()
        self.assertFalse(client.connected.done() or transport.data)
        delay, func, args, _ = loop.later.pop(1) # first in later is heartbeat
        self.assert_(8 < delay < 10)
        self.assertEqual(len(loop.later), 1) # first in later is heartbeat
        func(*args) # connect again
        self.assertFalse(protocol is loop.protocol)
        self.assertFalse(transport is loop.transport)
        protocol = loop.protocol
        transport = loop.transport
        protocol.data_received(sized(self.enc + b'3101'))
        self.assertEqual(self.unsized(transport.pop(2)), self.enc + b'3101')
        self.respond(1, None)
        self.respond(2, 'b'*8)
        self.pop(4)
        self.assertEqual(self.pop(), (3, False, 'get_info', ()))
        self.respond(3, dict(length=42))
        self.assert_(client.connected.done() and not transport.data)
        self.assert_(client.ready)

    def test_readonly_fallback(self):
        addrs = [('1.2.3.4', 8200), ('2.2.3.4', 8200)]
        wrapper, cache, loop, client, protocol, transport = self.start(
            addrs, (), read_only=Fallback)

        self.assertTrue(self.is_read_only())

        # We'll treat the first address as read-only and we'll let it connect:
        loop.connect_connecting(addrs[0])
        protocol, transport = loop.protocol, loop.transport
        protocol.data_received(sized(self.enc + b'3101'))
        self.assertEqual(self.unsized(transport.pop(2)), self.enc + b'3101')
        # We see that the client tried a writable connection:
        self.assertEqual(self.pop(),
                         (1, False, 'register', ('TEST', False)))
        # We respond with a read-only exception:
        self.respond(1, ('ZODB.POSException.ReadOnlyError', ()), True)
        self.assertTrue(self.is_read_only())

        # The client tries for a read-only connection:
        self.assertEqual(self.pop(), (2, False, 'register', ('TEST', True)))
        # We respond with successfully:
        self.respond(2, None)
        self.pop(2)
        self.respond(3, 'b'*8)
        self.assertTrue(self.is_read_only())

        # At this point, the client is ready and using the protocol,
        # and the protocol is read-only:
        self.assert_(client.ready)
        self.assertEqual(client.protocol, protocol)
        self.assertEqual(protocol.read_only, True)
        connected = client.connected

        # The client asks for info, and we respond:
        self.assertEqual(self.pop(), (4, False, 'get_info', ()))
        self.respond(4, dict(length=42))

        self.assert_(connected.done())

        # We connect the second address:
        loop.connect_connecting(addrs[1])
        loop.protocol.data_received(sized(self.enc + b'3101'))
        self.assertEqual(self.unsized(loop.transport.pop(2)), self.enc + b'3101')
        self.assertEqual(self.parse(loop.transport.pop()),
                         (1, False, 'register', ('TEST', False)))
        self.assertTrue(self.is_read_only())

        # We respond and the writable connection succeeds:
        self.respond(1, None)

        # at this point, a lastTransaction request is emitted:

        self.assertEqual(self.parse(loop.transport.pop()),
                         (2, False, 'lastTransaction', ()))
        self.assertFalse(self.is_read_only())

        # Now, the original protocol is closed, and the client is
        # no-longer ready:
        self.assertFalse(client.ready)
        self.assertFalse(client.protocol is protocol)
        self.assertEqual(client.protocol, loop.protocol)
        self.assertEqual(protocol.closed, True)
        self.assert_(client.connected is not connected)
        self.assertFalse(client.connected.done())
        protocol, transport = loop.protocol, loop.transport
        self.assertEqual(protocol.read_only, False)

        # Now, we finish verification
        self.respond(2, 'b'*8)
        self.respond(3, dict(length=42))
        self.assert_(client.ready)
        self.assert_(client.connected.done())

    def test_invalidations_while_verifying(self):
        # While we're verifying, invalidations are ignored
        wrapper, cache, loop, client, protocol, transport = self.start()
        protocol.data_received(sized(self.enc + b'3101'))
        self.assertEqual(self.unsized(transport.pop(2)), self.enc + b'3101')
        self.respond(1, None)
        self.pop(4)
        self.send('invalidateTransaction', b'b'*8, [b'1'*8], called=False)
        self.respond(2, b'a'*8)
        self.send('invalidateTransaction', b'c'*8, self.seq_type([b'1'*8]),
                  no_output=False)
        self.assertEqual(self.pop(), (3, False, 'get_info', ()))

        # We'll disconnect:
        protocol.connection_lost(Exception("lost"))
        self.assert_(protocol is not loop.protocol)
        self.assert_(transport is not loop.transport)
        protocol = loop.protocol
        transport = loop.transport

        # Similarly, invalidations aren't processed while reconnecting:

        protocol.data_received(sized(self.enc + b'3101'))
        self.assertEqual(self.unsized(transport.pop(2)), self.enc + b'3101')
        self.respond(1, None)
        self.pop(4)
        self.send('invalidateTransaction', b'd'*8, [b'1'*8], called=False)
        self.respond(2, b'c'*8)
        self.send('invalidateTransaction', b'e'*8, self.seq_type([b'1'*8]),
                  no_output=False)
        self.assertEqual(self.pop(), (3, False, 'get_info', ()))

    def test_flow_control(self):
        # When sending a lot of data (blobs), we don't want to fill up
        # memory behind a slow socket. Asycio's flow control helper
        # seems a bit complicated. We'd rather pass an iterator that's
        # consumed as we can.

        wrapper, cache, loop, client, protocol, transport = self.start(
            finish_start=True)

        # Give the transport a small capacity:
        transport.capacity = 2
        self.async('foo')
        self.async('bar')
        self.async('baz')
        self.async('splat')

        # The first 2 were sent, but the remaining were queued.
        self.assertEqual(self.pop(),
                         [(0, True, 'foo', ()), (0, True, 'bar', ())])

        # But popping them allowed sending to resume:
        self.assertEqual(self.pop(),
                         [(0, True, 'baz', ()), (0, True, 'splat', ())])

        # This is especially handy with iterators:
        self.async_iter((name, ()) for name in 'abcde')
        self.assertEqual(self.pop(), [(0, True, 'a', ()), (0, True, 'b', ())])
        self.assertEqual(self.pop(), [(0, True, 'c', ()), (0, True, 'd', ())])
        self.assertEqual(self.pop(), (0, True, 'e', ()))
        self.assertEqual(self.pop(), [])

    def test_bad_protocol(self):
        wrapper, cache, loop, client, protocol, transport = self.start()
        with mock.patch("ZEO.asyncio.client.logger.error") as error:
            self.assertFalse(error.called)
            protocol.data_received(sized(self.enc + b'200'))
            self.assert_(isinstance(error.call_args[0][1], ProtocolError))


    def test_get_peername(self):
        wrapper, cache, loop, client, protocol, transport = self.start(
            finish_start=True)
        self.assertEqual(client.get_peername(), '1.2.3.4')

    def test_call_async_from_same_thread(self):
        # There are a few (1?) cases where we call into client storage
        # where it needs to call back asyncronously. Because we're
        # calling from the same thread, we don't need to use a futurte.
        wrapper, cache, loop, client, protocol, transport = self.start(
            finish_start=True)

        client.call_async_from_same_thread('foo', 1)
        self.assertEqual(self.pop(), (0, True, 'foo', (1, )))

    def test_ClientDisconnected_on_call_timeout(self):
        wrapper, cache, loop, client, protocol, transport = self.start()
        self.wait_for_result = super(ClientTests, self).wait_for_result
        self.assertRaises(ClientDisconnected, self.call, 'foo')
        client.ready = False
        self.assertRaises(ClientDisconnected, self.call, 'foo')

    def test_errors_in_data_received(self):
        # There was a bug in ZEO.async.client.Protocol.data_recieved
        # that caused it to fail badly if errors were raised while
        # handling data.

        wrapper, cache, loop, client, protocol, transport =self.start(
            finish_start=True)

        wrapper.receiveBlobStart.side_effect = ValueError('test')

        chunk = 'x' * 99999
        try:
            loop.protocol.data_received(
                sized(
                    self.encode(0, True, 'receiveBlobStart', ('oid', 'serial'))
                    ) +
                sized(
                    self.encode(
                        0, True, 'receiveBlobChunk', ('oid', 'serial', chunk))
                    )
                )
        except ValueError:
            pass
        loop.protocol.data_received(sized(
            self.encode(0, True, 'receiveBlobStop', ('oid', 'serial'))
            ))
        wrapper.receiveBlobChunk.assert_called_with('oid', 'serial', chunk)
        wrapper.receiveBlobStop.assert_called_with('oid', 'serial')

    def test_heartbeat(self):
        # Protocols run heartbeats on a configurable (sort of)
        # heartbeat interval, which defaults to every 60 seconds.
        wrapper, cache, loop, client, protocol, transport = self.start(
            finish_start=True)

        delay, func, args, handle = loop.later.pop()
        self.assertEqual(
            (delay, func, args, handle),
            (60, protocol.heartbeat, (), protocol.heartbeat_handle),
            )
        self.assertFalse(loop.later or handle.cancelled)

        # The heartbeat function sends heartbeat data and reschedules itself.
        func()
        self.assertEqual(self.pop(), (-1, 0, '.reply', None))
        self.assertTrue(protocol.heartbeat_handle != handle)

        delay, func, args, handle = loop.later.pop()
        self.assertEqual(
            (delay, func, args, handle),
            (60, protocol.heartbeat, (), protocol.heartbeat_handle),
            )
        self.assertFalse(loop.later or handle.cancelled)

        # The heartbeat is cancelled when the protocol connection is lost:
        protocol.connection_lost(None)
        self.assertTrue(handle.cancelled)

class MsgpackClientTests(ClientTests):
    enc = b'M'
    seq_type = tuple

class MemoryCache(object):

    def __init__(self):
        # { oid -> [(start, end, data)] }
        self.data = collections.defaultdict(list)
        self.last_tid = None

    clear = __init__

    closed = False
    def close(self):
        self.closed = True

    def __len__(self):
        return len(self.data)

    def load(self, oid):
        revisions = self.data[oid]
        if revisions:
            start, end, data = revisions[-1]
            if not end:
                return data, start
        return None

    def store(self, oid, start_tid, end_tid, data):
        assert start_tid is not None
        revisions = self.data[oid]
        data = (start_tid, end_tid, data)
        if not revisions or data != revisions[-1]:
            revisions.append(data)
        revisions.sort()

    def loadBefore(self, oid, tid):
        for start, end, data in self.data[oid]:
            if start < tid and (end is None or end >= tid):
                return data, start, end

    def invalidate(self, oid, tid):
        revisions = self.data[oid]
        if revisions:
            if tid is None:
                del revisions[:]
            else:
                start, end, data = revisions[-1]
                if end is None:
                    revisions[-1] = start, tid, data

    def getLastTid(self):
        return self.last_tid

    def setLastTid(self, tid):
        self.last_tid = tid


class ServerTests(Base, setupstack.TestCase):

    # The server side of things is pretty simple compared to the
    # client, because it's the clien't job to make and keep
    # connections. Servers are pretty passive.

    def connect(self, finish=False):
        protocol = server_protocol(self.enc == b'M')
        self.loop = protocol.loop
        self.target = protocol.zeo_storage
        if finish:
            self.assertEqual(self.pop(parse=False),
                             self.enc + best_protocol_version)
            protocol.data_received(sized(self.enc + b'5'))
        return protocol

    message_id = 0
    target = None
    def call(self, meth, *args, **kw):
        if kw:
            expect = kw.pop('expect', self)
            target = kw.pop('target', self.target)
            self.assertFalse(kw)

            if target is not None:
                target = getattr(target, meth)
                if expect is not self:
                    target.return_value = expect

        self.message_id += 1
        self.loop.protocol.data_received(
            sized(self.encode(self.message_id, False, meth, args)))

        if target is not None:
            target.assert_called_once_with(*args)
            target.reset_mock()

        if expect is not self:
            self.assertEqual(self.pop(),
                             (self.message_id, False, '.reply', expect))

    def testServerBasics(self):
        # A simple listening thread accepts connections.  It creats
        # asyncio connections by calling ZEO.asyncio.new_connection:
        protocol = self.connect()
        self.assertFalse(protocol.zeo_storage.notify_connected.called)

        # The server sends it's protocol.
        self.assertEqual(self.pop(parse=False),
                         self.enc + best_protocol_version)

        # The client sends it's protocol:
        protocol.data_received(sized(self.enc + b'5'))

        self.assertEqual(protocol.protocol_version, self.enc + b'5')

        protocol.zeo_storage.notify_connected.assert_called_once_with(protocol)

        # The client registers:
        self.call('register', False, expect=None)

        # It does other things, like, send hearbeats:
        protocol.data_received(sized(b'(J\xff\xff\xff\xffK\x00U\x06.replyNt.'))

        # The client can make async calls:
        self.send('register')

        # Let's close the connection
        self.assertFalse(protocol.zeo_storage.notify_disconnected.called)
        protocol.connection_lost(None)
        protocol.zeo_storage.notify_disconnected.assert_called_once_with()

    def test_invalid_methods(self):
        protocol = self.connect(True)
        protocol.zeo_storage.notify_connected.assert_called_once_with(protocol)

        # If we try to call a methid that isn't in the protocol's
        # white list, it will disconnect:
        self.assertFalse(protocol.loop.transport.closed)
        self.call('foo', target=None)
        self.assertTrue(protocol.loop.transport.closed)

class MsgpackServerTests(ServerTests):
    enc = b'M'
    seq_type = tuple

def server_protocol(msgpack,
                    zeo_storage=None,
                    protocol_version=None,
                    addr=('1.2.3.4', '42'),
                    ):
    if zeo_storage is None:
        zeo_storage = mock.Mock()
    loop = Loop()
    sock = () # anything not None
    new_connection(loop, addr, sock, zeo_storage, msgpack)
    if protocol_version:
        loop.protocol.data_received(sized(protocol_version))
    return loop.protocol

def response(*data):
    return sized(self.encode(*data))

def sized(message):
    return struct.pack(">I", len(message)) + message

class Logging(object):

    def __init__(self, level=logging.ERROR):
        self.level = level

    def __enter__(self):
        self.handler = logging.StreamHandler()
        logging.getLogger().addHandler(self.handler)
        logging.getLogger().setLevel(self.level)

    def __exit__(self, *args):
        logging.getLogger().removeHandler(self.handler)
        logging.getLogger().setLevel(logging.NOTSET)


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ClientTests))
    suite.addTest(unittest.makeSuite(ServerTests))
    suite.addTest(unittest.makeSuite(MsgpackClientTests))
    suite.addTest(unittest.makeSuite(MsgpackServerTests))
    return suite
