import logging
import unittest
from zope.testing import setupstack
from ..StorageServer import LockManager
from ..asyncio.server import Delay

class Client:

    locked = False
    connected = True
    ncalls = 0

    def __init__(self):
        self.call_queue = []
        self.log_data = []

    def call_soon_threadsafe(self, func, *args):
        self.call_queue.append((func, args))

    def call_queued(self):
        while self.call_queue:
            func, args = self.call_queue.pop(0)
            func(*args)

    def lock(self):
        self.locked = True
        self.ncalls += 1
        return 'lock' + str(self.ncalls)

    def nolock(self):
        self.ncalls += 1
        return 'nolock' + str(self.ncalls)

    def oops(self):
        raise TypeError('oops')

    def log(self, *a):
        self.log_data.append(a)

    @property
    def logged(self):
        r = self.log_data[:]
        del self.log_data[:]
        return r

class Timer:

    timed = None

    def begin(self, zs):
        self.timed = zs

    def end(self, zs):
        assert self.timed is zs
        self.timed = None

class Stats:
    lock_time = None

class LockManagerTests(setupstack.TestCase):
    """Tests of the storage-server lock manager
    """

    def setUp(self):
        super(LockManagerTests, self).setUp()
        self.__time_time = self.mock('time.time', return_value = 1468173514)
        self.__stats = Stats()
        self.__timer = Timer()
        self.__lm = LockManager('test', self.__stats, self.__timer)

    def send_reply(self, *a):
        self.__reply = a

    def send_error(self, *a):
        self.__error = a

    def tick(self):
        self.__time_time.return_value += 1

    def test_basics(self):

        c1 = Client(); c2 = Client(); c3 = Client(); c4 = Client()

        equal = self.assertEqual
        true = self.assertTrue; false = self.assertFalse
        tick = self.tick

        # we can lock non-conflicting clients at the same time.
        false(c1.locked)
        equal(self.__lm.lock(c1, [11, 12], c1.lock), 'lock1'); tick()
        true(c1.locked)
        equal(c1.logged,
              [("('test') lock: transactions waiting: 0", logging.DEBUG)])

        false(c2.locked)
        equal(self.__lm.lock(c2, [22, 21], c2.lock), 'lock1'); tick()
        true(c2.locked)
        equal(c2.logged,
              [("('test') lock: transactions waiting: 0", logging.DEBUG)])

        # But not so much for a conflicting lock:
        d3 = self.__lm.lock(c3, [21, 13], c3.lock); tick()
        d3.set_sender(1, self)
        false(c3.locked)
        equal(c3.logged,
              [("('test') queue lock: transactions waiting: 1", logging.DEBUG)])

        # When we lock objects, the stats and timer are updated to
        # reflect the object that hs been locked the most:
        equal(self.__stats.lock_time, self.__time_time.return_value - 3)
        equal(self.__timer.timed, c1)


        # When we release c1, the timer and stats will reflect c2
        self.__lm.release(c1); tick()
        false(c1.locked)
        equal(self.__stats.lock_time, self.__time_time.return_value - 1)
        equal(self.__timer.timed, c2)
        equal(c1.logged,
              [("('test') unlock: transactions waiting: 1", logging.DEBUG)])

        # C3 is still locked:
        false(c3.locked)

        # We'll lock c1 again, but it will be blocked by c3,
        # because c3 has the lock for 13.
        d1 = self.__lm.lock(c1, [11, 13], c1.lock); tick()
        d1.set_sender(2, self)
        false(c1.locked)
        equal(c1.logged,
              [("('test') queue lock: transactions waiting: 2", logging.DEBUG)])

        # Now, we'll release c2.
        self.__lm.release(c2); tick()
        false(c2.locked)
        equal(c2.logged,
              [("('test') unlock: transactions waiting: 2", logging.DEBUG)])

        # A call was queued for C3. Let's execute it:
        true(c3.call_queue); c3.call_queued(); tick()

        # Note that nothing was queued for c1
        false(c1.call_queue)
        false(c1.logged)

        # c3 is now locked:
        true(c3.locked)
        equal(c3.logged,
              [("('test') lock: transactions waiting: 1", logging.DEBUG)])
        equal(self.__stats.lock_time, self.__time_time.return_value - 1)
        equal(self.__timer.timed, c3)

        # C3's delay was completed:
        equal(self.__reply, (1, 'lock1'))

        # Now, we'll release c3
        self.__lm.release(c3); tick()
        false(c3.locked)
        equal(c3.logged,
              [("('test') unlock: transactions waiting: 1", logging.DEBUG)])


        # A call was queued for C1. Let's execute it:
        true(c1.call_queue); c1.call_queued(); tick()

        # c1 is now locked:
        true(c1.locked)
        equal(c1.logged,
              [("('test') lock: transactions waiting: 0", logging.DEBUG)])
        equal(self.__stats.lock_time, self.__time_time.return_value - 1)
        equal(self.__timer.timed, c1)

        # C1's delay was completed:
        equal(self.__reply, (2, 'lock2'))

    def test_multiple_waiting(self):
        c1 = Client(); c2 = Client(); c3 = Client(); c4 = Client()
        equal = self.assertEqual
        true = self.assertTrue; false = self.assertFalse

        self.__lm.lock(c1, [1], c1.lock); true(c1.locked)
        d2 = self.__lm.lock(c2, [1], c2.lock); false(c2.locked);
        d2.set_sender(2, self)
        equal(c2.logged,
              [("('test') queue lock: transactions waiting: 1", logging.DEBUG)])
        d3 = self.__lm.lock(c3, [1], c3.lock); false(c3.locked);
        d3.set_sender(3, self)
        equal(c3.logged,
              [("('test') queue lock: transactions waiting: 2", logging.DEBUG)])

        c = Client()
        self.__lm.lock(c, [2], c.lock)
        d4 = self.__lm.lock(c4, [2], c4.lock); false(c4.locked)
        d4.set_sender(4, self)
        equal(c4.logged,
              [("('test') queue lock: transactions waiting: 3", logging.DEBUG)])

        for i in range(6):
            c = Client()
            d = self.__lm.lock(c, [2], c.locked)
            equal(c.logged,
                  [("('test') queue lock: transactions waiting: {}".format(i+4),
                    logging.WARNING)])

        c = Client()
        d = self.__lm.lock(c, [2], c.locked)
        equal(c.logged,
              [("('test') queue lock: transactions waiting: 10",
                logging.CRITICAL)])

        self.__lm.release(c1)
        false(c1.call_queue)
        true(c2.call_queue) # retry
        true(c3.call_queue) # retry
        false(c4.call_queue)
        false(c.call_queue)

        c2.call_queued(); true(c2.locked) # first one to try gets the lock
        c3.call_queued(); false(c3.locked)
        false(c2.call_queue)
        false(c3.call_queue)

        self.__lm.release(c2); false(c2.locked)
        false(c2.call_queue)
        true(c3.call_queue)
        c3.call_queued(); true(c3.locked)

    def test_multiple_tries_to_get_lock(self):
        true = self.assertTrue; false = self.assertFalse

        clients = []
        for i in range(3):
            c = Client()
            clients.append(c)
            self.__lm.lock(c, [i], c.lock)

        client = Client()
        d = self.__lm.lock(client, [0, 1, 2], client.lock)
        false(client.locked or client.call_queue)

        # Releasing the first 2 won't be enough:
        for i in range(2):
            self.__lm.release(clients[i])
            true(client.call_queue); client.call_queued()
            false(client.locked)

        # But the 3rd time will be the charm:
        self.__lm.release(clients[2])
        true(client.call_queue); client.call_queued()
        true(client.locked)

    def test_nolock(self):
        c1 = Client(); c2 = Client()
        self.__lm.lock(c1, [0], c2.nolock)
        self.assertFalse(c1.locked)
        self.__lm.lock(c2, [0], c2.lock)
        self.assertTrue(c2.locked)

    def test_errors(self):
        c1 = Client(); c2 = Client()

        # Failure in lock callback no delay:

        with self.assertRaisesRegexp(TypeError, 'oops'):
            self.__lm.lock(c1, [0], c1.oops)

        self.assertFalse(c1.locked)

        # Delayed error"
        self.__lm.lock(c1, [0], c1.lock)
        d = self.__lm.lock(c2, [0], c2.oops)
        d.set_sender(42, self)
        self.__lm.release(c1)
        c2.call_queued()

        self.assertEqual(self.__error[0], 42)
        with self.assertRaisesRegexp(TypeError, 'oops'):
            raise self.__error[1]

def test_suite():
    return unittest.makeSuite(LockManagerTests)
