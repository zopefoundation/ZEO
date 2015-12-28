from zope.testing import setupstack
from concurrent.futures import Future
from unittest import mock
import asyncio
import collections
import pickle
import struct
import unittest

from .testing import Loop
from .client import ClientRunner
from ..Exceptions import ClientDisconnected

class AsyncTests(setupstack.TestCase, ClientRunner):

    addr = ('127.0.0.1', 8200)

    def start(self):
        # To create a client, we need to specify an address, a client
        # object and a cache.

        wrapper = mock.Mock()
        cache = MemoryCache()
        self.set_options(self.addr, wrapper, cache, 'TEST', False)

        # We can also provide an event loop.  We'll use a testing loop
        # so we don't have to actually make any network connection.
        self.setup_delegation(Loop())
        protocol = self.loop.protocol
        transport = self.loop.transport

        def send(meth, *args):
            protocol.data_received(
                sized(pickle.dumps((0, True, meth, args), 3)))

        def respond(message_id, result):
            protocol.data_received(
                sized(pickle.dumps((message_id, False, '.reply', result), 3)))

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

        # The client isn't connected until the server sends it some data.
        self.assertFalse(client.connected.done() or transport.data)

        # The server sends the client some data:
        protocol.data_received(sized(b'Z101'))

        # The client sends back a handshake, and registers the
        # storage, and requests the last transaction.
        self.assertEqual(self.unsized(transport.pop(2)), b'Z101')
        parse = self.parse
        self.assertEqual(parse(transport.pop()),
                         [(1, False, 'register', ('TEST', False)),
                          (2, False, 'lastTransaction', ()),
                          ])

        # Actually, the client isn't connected until it initializes it's cache:
        self.assertFalse(client.connected.done() or transport.data)

        # If we try to make calls while the client is connecting, they're queued
        f1 = self.call('foo', 1, 2)
        self.assertFalse(f1.done())

        # If we try to make an async call, we get an immediate error:
        f2 = self.callAsync('bar', 3, 4)
        self.assert_(isinstance(f2.exception(), ClientDisconnected))

        # Let's respond to those first 2 calls:

        respond(1, None)
        respond(2, 'a'*8)

        # Now we're connected, the cache was initialized, and the
        # queued message has been sent:
        self.assert_(client.connected.done())
        self.assertEqual(cache.getLastTid(), 'a'*8)
        self.assertEqual(parse(transport.pop()), (3, False, 'foo', (1, 2)))

        respond(3, 42)
        self.assertEqual(f1.result(), 42)

        # Now we can make async calls:
        f2 = self.callAsync('bar', 3, 4)
        self.assert_(f2.done() and f2.exception() is None)
        self.assertEqual(parse(transport.pop()), (0, True, 'bar', (3, 4)))

        # Loading objects gets special handling to leverage the cache.
        loaded = self.load(b'1'*8)

        # The data wasn't in the cache, so we make a server call:
        self.assertEqual(parse(transport.pop()),
                         (4, False, 'loadEx', (b'1'*8,)))
        respond(4, (b'data', b'a'*8))
        self.assertEqual(loaded.result(), (b'data', b'a'*8))

        # If we make another request, it will be satisfied from the cache:
        loaded = self.load(b'1'*8)
        self.assertEqual(loaded.result(), (b'data', b'a'*8))
        self.assertFalse(transport.data)

        # Let's send an invalidation:
        send('invalidateTransaction', b'b'*8, [b'1'*8])
        wrapper.invalidateTransaction.assert_called_with(b'b'*8, [b'1'*8])

        # Now, if we try to load current again, we'll make a server request.
        loaded = self.load(b'1'*8)
        self.assertEqual(parse(transport.pop()),
                         (5, False, 'loadEx', (b'1'*8,)))
        respond(5, (b'data2', b'b'*8))
        self.assertEqual(loaded.result(), (b'data2', b'b'*8))

        # Loading non-current data may also be satisfied from cache
        loaded = self.load_before(b'1'*8, b'b'*8)
        self.assertEqual(loaded.result(), (b'data', b'a'*8, b'b'*8))
        self.assertFalse(transport.data)
        loaded = self.load_before(b'1'*8, b'c'*8)
        self.assertEqual(loaded.result(), (b'data2', b'b'*8, None))
        self.assertFalse(transport.data)
        loaded = self.load_before(b'1'*8, b'_'*8)

        self.assertEqual(parse(transport.pop()),
                         (6, False, 'loadBefore', (b'1'*8, b'_'*8)))
        respond(6, (b'data0', b'^'*8, b'_'*8))
        self.assertEqual(loaded.result(), (b'data0', b'^'*8, b'_'*8))

        # When committing transactions, we need to update the cache
        # with committed data.  To do this, we pass a (oid, tid, data)
        # iteratable to tpc_finish_threadsafe.
        from ZODB.ConflictResolution import ResolvedSerial
        committed = self.tpc_finish(
            b'd'*8,
            [(b'2'*8, b'd'*8, 'committed 2'),
             (b'1'*8, ResolvedSerial, 'committed 3'),
             (b'4'*8, b'd'*8, 'committed 4'),
             ])
        self.assertFalse(committed.done() or
                         cache.load(b'2'*8) or
                         cache.load(b'4'*8))
        self.assertEqual(cache.load(b'1'*8), (b'data2', b'b'*8))
        self.assertEqual(parse(transport.pop()),
                         (7, False, 'tpc_finish', (b'd'*8,)))
        respond(7, None)
        self.assertEqual(committed.result(), None)
        self.assertEqual(cache.load(b'1'*8), None)
        self.assertEqual(cache.load(b'2'*8), ('committed 2', b'd'*8))
        self.assertEqual(cache.load(b'4'*8), ('committed 4', b'd'*8))

        # Is the protocol is disconnected, it will reconnect and will
        # resolve outstanding requests with exceptions:
        loaded = self.load(b'1'*8)
        f1 = self.call('foo', 1, 2)
        self.assertFalse(loaded.done() or f1.done())
        self.assertEqual(parse(transport.pop()),
                         [(8, False, 'loadEx', (b'1'*8,)),
                          (9, False, 'foo', (1, 2))],
                         )
        exc = TypeError(43)

        protocol.connection_lost(exc)

        self.assertEqual(loaded.exception(), exc)
        self.assertEqual(f1.exception(), exc)

        # Because we reconnected, a new protocol and transport were created:
        self.assert_(protocol is not loop.protocol)
        self.assert_(transport is not loop.transport)
        protocol = loop.protocol
        transport = loop.transport

        # and we have a new incomplete connect future:
        self.assertFalse(client.connected.done() or transport.data)
        protocol.data_received(sized(b'Z101'))
        self.assertEqual(self.unsized(transport.pop(2)), b'Z101')
        self.assertEqual(parse(transport.pop()),
                         [(10, False, 'register', ('TEST', False)),
                          (11, False, 'lastTransaction', ()),
                          ])
        respond(10, None)
        respond(11, b'd'*8)

        # Because the server tid matches the cache tid, we're done connecting
        self.assert_(client.connected.done() and not transport.data)
        self.assertEqual(cache.getLastTid(), b'd'*8)

        # Because we were able to update the cache, we didn't have to
        # invalidate the database cache:
        wrapper.invalidateTransaction.assert_not_called()

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
        protocol.data_received(sized(b'Z101'))
        self.assertEqual(self.unsized(transport.pop(2)), b'Z101')
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

        # Now that verification is done, we're done connecting
        self.assert_(client.connected.done() and not transport.data)
        self.assertEqual(cache.getLastTid(), b'e'*8)

        # And the cache has been updated:
        self.assertEqual(cache.load(b'2'*8),
                         ('2 data', b'a'*8)) # unchanged
        self.assertEqual(cache.load(b'4'*8), None)

        # Because we were able to update the cache, we didn't have to
        # invalidate the database cache:
        wrapper.invalidateCache.assert_not_called()

    def test_cache_way_behind(self):
        wrapper, cache, loop, client, protocol, transport, send, respond = (
            self.start())

        cache.setLastTid(b'a'*8)
        cache.store(b'4'*8, b'a'*8, None, '4 data')
        self.assertTrue(cache)

        self.assertFalse(client.connected.done() or transport.data)
        protocol.data_received(sized(b'Z101'))
        self.assertEqual(self.unsized(transport.pop(2)), b'Z101')
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

        # Now that verification is done, we're done connecting
        self.assert_(client.connected.done() and not transport.data)
        self.assertEqual(cache.getLastTid(), b'e'*8)

        # But the cache is now empty and we invalidated the database cache
        self.assertFalse(cache)
        wrapper.invalidateCache.assert_called_with()

    def test_cache_crazy(self):
        wrapper, cache, loop, client, protocol, transport, send, respond = (
            self.start())

        cache.setLastTid(b'e'*8)
        cache.store(b'4'*8, b'e'*8, None, '4 data')
        self.assertTrue(cache)

        self.assertFalse(client.connected.done() or transport.data)
        protocol.data_received(sized(b'Z101'))
        self.assertEqual(self.unsized(transport.pop(2)), b'Z101')
        self.assertEqual(self.parse(transport.pop()),
                         [(1, False, 'register', ('TEST', False)),
                          (2, False, 'lastTransaction', ()),
                          ])
        respond(1, None)
        respond(2, b'a'*8)

        # The server tid is less than the client tid, WTF? We error
        with self.assertRaisesRegex(AssertionError, 'Server behind client'):
            client.connected.result()

# todo:
# bad cache validation, make sure ZODB cache is cleared
# cache boolean value in interface

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

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(AsyncTests))
    return suite
