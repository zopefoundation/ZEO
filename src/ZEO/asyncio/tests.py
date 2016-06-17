from zope.testing import setupstack
from concurrent.futures import Future
from unittest import mock
from ZODB.POSException import ReadOnlyError

import asyncio
import collections
import logging
import pdb
import pickle
import struct
import unittest
import ZEO.Exceptions

from .testing import Loop
from .client import ClientRunner, Fallback
from ..Exceptions import ClientDisconnected
from ..ClientStorage import m64

class AsyncTests(setupstack.TestCase, ClientRunner):

    def start(self,
              addrs=(('127.0.0.1', 8200), ), loop_addrs=None,
              read_only=False,
              finish_start=False,
              ):
        # To create a client, we need to specify an address, a client
        # object and a cache.

        wrapper = mock.Mock()
        cache = MemoryCache()
        self.set_options(addrs, wrapper, cache, 'TEST', read_only, timeout=1)

        # We can also provide an event loop.  We'll use a testing loop
        # so we don't have to actually make any network connection.
        loop = Loop(addrs if loop_addrs is None else loop_addrs)
        self.setup_delegation(loop)
        self.assertFalse(wrapper.notify_disconnected.called)
        protocol = loop.protocol
        transport = loop.transport

        def send(meth, *args):
            loop.protocol.data_received(
                sized(pickle.dumps((0, True, meth, args), 3)))

        def respond(message_id, result):
            loop.protocol.data_received(
                sized(pickle.dumps((message_id, False, '.reply', result), 3)))

        if finish_start:
            protocol.data_received(sized(b'Z3101'))
            self.assertEqual(self.unsized(transport.pop(2)), b'Z3101')
            parse = self.parse
            self.assertEqual(parse(transport.pop()),
                             [(1, False, 'register', ('TEST', False)),
                              (2, False, 'lastTransaction', ()),
                              ])
            respond(1, None)
            respond(2, 'a'*8)
            self.assertEqual(parse(transport.pop()), (3, False, 'get_info', ()))
            respond(3, dict(length=42))

        return (wrapper, cache, self.loop, self.client, protocol, transport,
                send, respond)

    def wait_for_result(self, future, timeout):
        return future

    def testBasics(self):

        # Here, we'll go through the basic usage of the asyncio ZEO
        # network client.  The client is responsible for the core
        # functionality of a ZEO client storage.  The client storage
        # is largely just a wrapper around the asyncio client.

        wrapper, cache, loop, client, protocol, transport, send, respond = (
            self.start())
        self.assertFalse(wrapper.notify_disconnected.called)

        # The client isn't connected until the server sends it some data.
        self.assertFalse(client.connected.done() or transport.data)

        # The server sends the client it's protocol. In this case,
        # it's a very high one.  The client will send it's highest that
        # it can use.
        protocol.data_received(sized(b'Z99999'))

        # The client sends back a handshake, and registers the
        # storage, and requests the last transaction.
        self.assertEqual(self.unsized(transport.pop(2)), b'Z3101')
        parse = self.parse
        self.assertEqual(parse(transport.pop()),
                         [(1, False, 'register', ('TEST', False)),
                          (2, False, 'lastTransaction', ()),
                          ])

        # Actually, the client isn't connected until it initializes it's cache:
        self.assertFalse(client.connected.done() or transport.data)

        # If we try to make calls while the client is *initially*
        # connecting, we get an error. This is because some dufus
        # decided to create a client storage without waiting for it to
        # connect.
        f1 = self.call('foo', 1, 2)
        self.assertTrue(isinstance(f1.exception(), ClientDisconnected))

        # When the client is reconnecting, it's ready flag is set to False and
        # it queues calls:
        client.ready = False
        f1 = self.call('foo', 1, 2)
        self.assertFalse(f1.done())

        # If we try to make an async call, we get an immediate error:
        f2 = self.async('bar', 3, 4)
        self.assert_(isinstance(f2.exception(), ClientDisconnected))

        # The wrapper object (ClientStorage) hasn't been notified:
        self.assertFalse(wrapper.notify_connected.called)

        # Let's respond to those first 2 calls:

        respond(1, None)
        respond(2, 'a'*8)

        # After verification, the client requests info:
        self.assertEqual(parse(transport.pop()), (3, False, 'get_info', ()))
        respond(3, dict(length=42))

        # Now we're connected, the cache was initialized, and the
        # queued message has been sent:
        self.assert_(client.connected.done())
        self.assertEqual(cache.getLastTid(), 'a'*8)
        self.assertEqual(parse(transport.pop()), (4, False, 'foo', (1, 2)))

        # The wrapper object (ClientStorage) has been notified:
        wrapper.notify_connected.assert_called_with(client, {'length': 42})

        respond(4, 42)
        self.assertEqual(f1.result(), 42)

        # Now we can make async calls:
        f2 = self.async('bar', 3, 4)
        self.assert_(f2.done() and f2.exception() is None)
        self.assertEqual(parse(transport.pop()), (0, True, 'bar', (3, 4)))

        # Loading objects gets special handling to leverage the cache.
        loaded = self.load_before(b'1'*8, m64)

        # The data wasn't in the cache, so we make a server call:
        self.assertEqual(parse(transport.pop()),
                         (5, False, 'loadBefore', (b'1'*8, m64)))
        respond(5, (b'data', b'a'*8, None))
        self.assertEqual(loaded.result(), (b'data', b'a'*8, None))

        # If we make another request, it will be satisfied from the cache:
        loaded = self.load_before(b'1'*8, m64)
        self.assertEqual(loaded.result(), (b'data', b'a'*8, None))
        self.assertFalse(transport.data)

        # Let's send an invalidation:
        send('invalidateTransaction', b'b'*8, [b'1'*8])
        wrapper.invalidateTransaction.assert_called_with(b'b'*8, [b'1'*8])
        wrapper.invalidateTransaction.reset_mock()

        # Now, if we try to load current again, we'll make a server request.
        loaded = self.load_before(b'1'*8, m64)
        self.assertEqual(parse(transport.pop()),
                         (6, False, 'loadBefore', (b'1'*8, m64)))
        respond(6, (b'data2', b'b'*8, None))
        self.assertEqual(loaded.result(), (b'data2', b'b'*8, None))

        # Loading non-current data may also be satisfied from cache
        loaded = self.load_before(b'1'*8, b'b'*8)
        self.assertEqual(loaded.result(), (b'data', b'a'*8, b'b'*8))
        self.assertFalse(transport.data)
        loaded = self.load_before(b'1'*8, b'c'*8)
        self.assertEqual(loaded.result(), (b'data2', b'b'*8, None))
        self.assertFalse(transport.data)
        loaded = self.load_before(b'1'*8, b'_'*8)

        self.assertEqual(parse(transport.pop()),
                         (7, False, 'loadBefore', (b'1'*8, b'_'*8)))
        respond(7, (b'data0', b'^'*8, b'_'*8))
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
        self.assertEqual(parse(transport.pop()),
                         (8, False, 'tpc_finish', (b'd'*8,)))
        respond(8, b'e'*8)
        self.assertEqual(committed.result(), b'e'*8)
        self.assertEqual(cache.load(b'1'*8), None)
        self.assertEqual(cache.load(b'2'*8), ('committed 2', b'e'*8))
        self.assertEqual(cache.load(b'4'*8), ('committed 4', b'e'*8))
        self.assertEqual(tids.pop(), b'e'*8)

        # If the protocol is disconnected, it will reconnect and will
        # resolve outstanding requests with exceptions:
        loaded = self.load_before(b'1'*8, m64)
        f1 = self.call('foo', 1, 2)
        self.assertFalse(loaded.done() or f1.done())
        self.assertEqual(parse(transport.pop()),
                         [(9, False, 'loadBefore', (b'1'*8, m64)),
                          (10, False, 'foo', (1, 2))],
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
        protocol.data_received(sized(b'Z310'))
        self.assertEqual(self.unsized(transport.pop(2)), b'Z310')
        self.assertEqual(parse(transport.pop()),
                         [(1, False, 'register', ('TEST', False)),
                          (2, False, 'lastTransaction', ()),
                          ])
        self.assertFalse(wrapper.notify_connected.called)
        respond(1, None)
        respond(2, b'e'*8)
        self.assertEqual(parse(transport.pop()), (3, False, 'get_info', ()))
        respond(3, dict(length=42))

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
        wrapper, cache, loop, client, protocol, transport, send, respond = (
            self.start())

        cache.setLastTid(b'a'*8)
        cache.store(b'4'*8, b'a'*8, None, '4 data')
        cache.store(b'2'*8, b'a'*8, None, '2 data')

        self.assertFalse(client.connected.done() or transport.data)
        protocol.data_received(sized(b'Z3101'))
        self.assertEqual(self.unsized(transport.pop(2)), b'Z3101')
        self.assertEqual(self.parse(transport.pop()),
                         [(1, False, 'register', ('TEST', False)),
                          (2, False, 'lastTransaction', ()),
                          ])
        respond(1, None)
        respond(2, b'e'*8)

        # We have to verify the cache, so we're not done connecting:
        self.assertFalse(client.connected.done())
        self.assertEqual(self.parse(transport.pop()),
                         (3, False, 'getInvalidations', (b'a'*8, )))
        respond(3, (b'e'*8, [b'4'*8]))

        self.assertEqual(self.parse(transport.pop()),
                         (4, False, 'get_info', ()))
        respond(4, dict(length=42))

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
        wrapper, cache, loop, client, protocol, transport, send, respond = (
            self.start())

        cache.setLastTid(b'a'*8)
        cache.store(b'4'*8, b'a'*8, None, '4 data')
        self.assertTrue(cache)

        self.assertFalse(client.connected.done() or transport.data)
        protocol.data_received(sized(b'Z3101'))
        self.assertEqual(self.unsized(transport.pop(2)), b'Z3101')
        self.assertEqual(self.parse(transport.pop()),
                         [(1, False, 'register', ('TEST', False)),
                          (2, False, 'lastTransaction', ()),
                          ])
        respond(1, None)
        respond(2, b'e'*8)

        # We have to verify the cache, so we're not done connecting:
        self.assertFalse(client.connected.done())
        self.assertEqual(self.parse(transport.pop()),
                         (3, False, 'getInvalidations', (b'a'*8, )))

        # We respond None, indicating that we're too far out of date:
        respond(3, None)

        self.assertEqual(self.parse(transport.pop()),
                         (4, False, 'get_info', ()))
        respond(4, dict(length=42))

        # Now that verification is done, we're done connecting
        self.assert_(client.connected.done() and not transport.data)
        self.assertEqual(cache.getLastTid(), b'e'*8)

        # But the cache is now empty and we invalidated the database cache
        self.assertFalse(cache)
        wrapper.invalidateCache.assert_called_with()

    def test_multiple_addresses(self):
        # We can pass multiple addresses to client constructor
        addrs = [('1.2.3.4', 8200), ('2.2.3.4', 8200)]
        wrapper, cache, loop, client, protocol, transport, send, respond = (
            self.start(addrs, ()))

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
        protocol.data_received(sized(b'Z3101'))
        self.assertEqual(self.unsized(transport.pop(2)), b'Z3101')
        respond(1, None)

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
        wrapper, cache, loop, client, protocol, transport, send, respond = (
            self.start())
        cache.store(b'4'*8, b'a'*8, None, '4 data')
        cache.setLastTid('b'*8)
        protocol.data_received(sized(b'Z3101'))
        self.assertEqual(self.unsized(transport.pop(2)), b'Z3101')
        parse = self.parse
        self.assertEqual(parse(transport.pop()),
                         [(1, False, 'register', ('TEST', False)),
                          (2, False, 'lastTransaction', ()),
                          ])
        respond(1, None)
        respond(2, 'a'*8)
        self.assertFalse(client.connected.done() or transport.data)
        delay, func, args, _ = loop.later.pop(1) # first in later is heartbeat
        self.assert_(8 < delay < 10)
        self.assertEqual(len(loop.later), 1) # first in later is heartbeat
        func(*args) # connect again
        self.assertFalse(protocol is loop.protocol)
        self.assertFalse(transport is loop.transport)
        protocol = loop.protocol
        transport = loop.transport
        protocol.data_received(sized(b'Z3101'))
        self.assertEqual(self.unsized(transport.pop(2)), b'Z3101')
        self.assertEqual(parse(transport.pop()),
                         [(1, False, 'register', ('TEST', False)),
                          (2, False, 'lastTransaction', ()),
                          ])
        respond(1, None)
        respond(2, 'b'*8)
        self.assertEqual(parse(transport.pop()), (3, False, 'get_info', ()))
        respond(3, dict(length=42))
        self.assert_(client.connected.done() and not transport.data)
        self.assert_(client.ready)

    def test_readonly_fallback(self):
        addrs = [('1.2.3.4', 8200), ('2.2.3.4', 8200)]
        wrapper, cache, loop, client, protocol, transport, send, respond = (
            self.start(addrs, (), read_only=Fallback))

        self.assertTrue(self.is_read_only())

        # We'll treat the first address as read-only and we'll let it connect:
        loop.connect_connecting(addrs[0])
        protocol, transport = loop.protocol, loop.transport
        protocol.data_received(sized(b'Z3101'))
        self.assertEqual(self.unsized(transport.pop(2)), b'Z3101')
        # We see that the client tried a writable connection:
        self.assertEqual(self.parse(transport.pop()),
                         (1, False, 'register', ('TEST', False)))
        # We respond with a read-only exception:
        respond(1, (ReadOnlyError, ReadOnlyError()))
        self.assertTrue(self.is_read_only())

        # The client tries for a read-only connection:
        self.assertEqual(self.parse(transport.pop()),
                         [(2, False, 'register', ('TEST', True)),
                          (3, False, 'lastTransaction', ()),
                          ])
        # We respond with successfully:
        respond(2, None)
        respond(3, 'b'*8)
        self.assertTrue(self.is_read_only())

        # At this point, the client is ready and using the protocol,
        # and the protocol is read-only:
        self.assert_(client.ready)
        self.assertEqual(client.protocol, protocol)
        self.assertEqual(protocol.read_only, True)
        connected = client.connected

        # The client asks for info, and we respond:
        self.assertEqual(self.parse(transport.pop()),
                         (4, False, 'get_info', ()))
        respond(4, dict(length=42))

        self.assert_(connected.done())

        # We connect the second address:
        loop.connect_connecting(addrs[1])
        loop.protocol.data_received(sized(b'Z3101'))
        self.assertEqual(self.unsized(loop.transport.pop(2)), b'Z3101')
        self.assertEqual(self.parse(loop.transport.pop()),
                         (1, False, 'register', ('TEST', False)))
        self.assertTrue(self.is_read_only())

        # We respond and the writable connection succeeds:
        respond(1, None)
        self.assertFalse(self.is_read_only())

        # at this point, a lastTransaction request is emitted:

        self.assertEqual(self.parse(loop.transport.pop()),
                         (2, False, 'lastTransaction', ()))

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
        respond(2, 'b'*8)
        respond(3, dict(length=42))
        self.assert_(client.ready)
        self.assert_(client.connected.done())

    def test_invalidations_while_verifying(self):
        # While we're verifying, invalidations are ignored
        wrapper, cache, loop, client, protocol, transport, send, respond = (
            self.start())
        protocol.data_received(sized(b'Z3101'))
        self.assertEqual(self.unsized(transport.pop(2)), b'Z3101')
        self.assertEqual(self.parse(transport.pop()),
                         [(1, False, 'register', ('TEST', False)),
                          (2, False, 'lastTransaction', ()),
                          ])
        respond(1, None)
        send('invalidateTransaction', b'b'*8, [b'1'*8])
        self.assertFalse(wrapper.invalidateTransaction.called)
        respond(2, b'a'*8)
        send('invalidateTransaction', b'c'*8, [b'1'*8])
        wrapper.invalidateTransaction.assert_called_with(b'c'*8, [b'1'*8])
        wrapper.invalidateTransaction.reset_mock()

        # We'll disconnect:
        protocol.connection_lost(Exception("lost"))
        self.assert_(protocol is not loop.protocol)
        self.assert_(transport is not loop.transport)
        protocol = loop.protocol
        transport = loop.transport

        # Similarly, invalidations aren't processed while reconnecting:

        protocol.data_received(sized(b'Z3101'))
        self.assertEqual(self.unsized(transport.pop(2)), b'Z3101')
        self.assertEqual(self.parse(transport.pop()),
                         [(1, False, 'register', ('TEST', False)),
                          (2, False, 'lastTransaction', ()),
                          ])
        respond(1, None)
        send('invalidateTransaction', b'd'*8, [b'1'*8])
        self.assertFalse(wrapper.invalidateTransaction.called)
        respond(2, b'c'*8)
        send('invalidateTransaction', b'e'*8, [b'1'*8])
        wrapper.invalidateTransaction.assert_called_with(b'e'*8, [b'1'*8])
        wrapper.invalidateTransaction.reset_mock()

    def test_flow_control(self):
        # When sending a lot of data (blobs), we don't want to fill up
        # memory behind a slow socket. Asycio's flow control helper
        # seems a bit complicated. We'd rather pass an iterator that's
        # consumed as we can.

        wrapper, cache, loop, client, protocol, transport, send, respond = (
            self.start(finish_start=True))

        # Give the transport a small capacity:
        transport.capacity = 2
        self.async('foo')
        self.async('bar')
        self.async('baz')
        self.async('splat')

        # The first 2 were sent, but the remaining were queued.
        self.assertEqual(self.parse(transport.pop()),
                         [(0, True, 'foo', ()), (0, True, 'bar', ())])

        # But popping them allowed sending to resume:
        self.assertEqual(self.parse(transport.pop()),
                         [(0, True, 'baz', ()), (0, True, 'splat', ())])

        # This is especially handy with iterators:
        self.async_iter((name, ()) for name in 'abcde')
        self.assertEqual(self.parse(transport.pop()),
                         [(0, True, 'a', ()), (0, True, 'b', ())])
        self.assertEqual(self.parse(transport.pop()),
                         [(0, True, 'c', ()), (0, True, 'd', ())])
        self.assertEqual(self.parse(transport.pop()),
                         (0, True, 'e', ()))
        self.assertEqual(self.parse(transport.pop()),
                         [])

    def test_bad_protocol(self):
        wrapper, cache, loop, client, protocol, transport, send, respond = (
            self.start())
        with mock.patch("ZEO.asyncio.client.logger.error") as error:
            self.assertFalse(error.called)
            protocol.data_received(sized(b'Z200'))
            self.assert_(isinstance(error.call_args[0][1],
                                    ZEO.Exceptions.ProtocolError))


    def test_get_peername(self):
        wrapper, cache, loop, client, protocol, transport, send, respond = (
            self.start(finish_start=True))
        self.assertEqual(client.get_peername(), '1.2.3.4')

    def test_call_async_from_same_thread(self):
        # There are a few (1?) cases where we call into client storage
        # where it needs to call back asyncronously. Because we're
        # calling from the same thread, we don't need to use a futurte.
        wrapper, cache, loop, client, protocol, transport, send, respond = (
            self.start(finish_start=True))

        client.call_async_from_same_thread('foo', 1)
        self.assertEqual(self.parse(transport.pop()), (0, True, 'foo', (1, )))

    def test_ClientDisconnected_on_call_timeout(self):
        wrapper, cache, loop, client, protocol, transport, send, respond = (
            self.start())
        self.wait_for_result = super().wait_for_result
        self.assertRaises(ClientDisconnected, self.call, 'foo')
        client.ready = False
        self.assertRaises(ClientDisconnected, self.call, 'foo')

    def test_errors_in_data_received(self):
        # There was a bug in ZEO.async.client.Protocol.data_recieved
        # that caused it to fail badly if errors were raised while
        # handling data.

        wrapper, cache, loop, client, protocol, transport, send, respond = (
            self.start(finish_start=True))

        wrapper.receiveBlobStart.side_effect = ValueError('test')

        chunk = 'x' * 99999
        try:
            loop.protocol.data_received(b''.join((
                sized(pickle.dumps(
                    (0, True, 'receiveBlobStart', ('oid', 'serial')), 3)),
                sized(pickle.dumps(
                    (0, True, 'receiveBlobChunk',
                     ('oid', 'serial', chunk)), 3)),
                )))
        except ValueError:
            pass
        loop.protocol.data_received(
            sized(pickle.dumps(
                (0, True, 'receiveBlobStop', ('oid', 'serial')), 3)),
            )
        wrapper.receiveBlobChunk.assert_called_with('oid', 'serial', chunk)
        wrapper.receiveBlobStop.assert_called_with('oid', 'serial')

    def test_heartbeat(self):
        # Protocols run heartbeats on a configurable (sort of)
        # heartbeat interval, which defaults to every 60 seconds.
        wrapper, cache, loop, client, protocol, transport, send, respond = (
            self.start(finish_start=True))

        delay, func, args, handle = loop.later.pop()
        self.assertEqual(
            (delay, func, args, handle),
            (60, protocol.heartbeat, (), protocol.heartbeat_handle),
            )
        self.assertFalse(loop.later or handle.cancelled)

        # The heartbeat function sends heartbeat data and reschedules itself.
        func()
        self.assertEqual(self.parse(transport.pop()), (-1, 0, '.reply', None))
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

    def unsized(self, data, unpickle=False):
        result = []
        while data:
            size, message, *data = data
            self.assertEqual(struct.unpack(">I", size)[0], len(message))
            if unpickle:
                message = pickle.loads(message)
            result.append(message)

        if len(result) == 1:
            result = result[0]
        return result

    def parse(self, data):
        return self.unsized(data, True)

def response(*data):
    return sized(pickle.dumps(data, 3))

def sized(message):
    return struct.pack(">I", len(message)) + message

class MemoryCache:

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
        revisions.append((start_tid, end_tid, data))
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

class Logging:

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
    suite.addTest(unittest.makeSuite(AsyncTests))
    return suite
