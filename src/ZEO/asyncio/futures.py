"""Optimized variants of ``asyncio``'s ``Future`` and ``Task``.

``asyncio`` schedules callbacks to be executed in the next
loop run. This increases the number of loop runs necessary to
obtain the result of a ZEO server request and adds significant
latency (+ 27% in some benchmarks).
This module defines variants which run callbacks immediately.
"""

from asyncio import CancelledError, InvalidStateError, get_event_loop
from threading import Event
from time import sleep


# ``Future`` states -- inlined below for speed
PENDING = 0
RESULT = 1
EXCEPTION = 2
CANCELLED = 3


class Future:
    """ Minimal mostly ``asyncio`` compatible future.

    In contrast to an ``asyncio`` future,
    callbacks are called immediately, not scheduled;
    their context is ignored.
    """
    __slots__ = ("_loop", "state", "_result", "callbacks",
                 "_asyncio_future_blocking")

    def __init__(self, loop=None):
        self._loop = loop if loop is not None else get_event_loop()
        self.state = 0  # PENDING
        self._result = None
        self.callbacks = []
        self._asyncio_future_blocking = False

    def get_loop(self):
        return self._loop

    def cancel(self, msg=None):
        """cancel the future if not done.

        Return ``True``, if really cancelled.

        *msg* is ignored.
        """
        if self.state:
            return False
        self.state = 3  # CANCELLED
        self._result = CancelledError()
        self.call_callbacks()
        return True

    def cancelled(self):
        return self.state == 3  # CANCELLED

    def done(self):
        return self.state

    def result(self):
        if self.state == 0:  # PENDING
            raise InvalidStateError("not done")
        elif self.state == 1:  # RESULT
            return self._result
        else:
            raise self._result

    def exception(self):
        if self.state == 0:  # PENDING
            raise InvalidStateError("not done")
        elif self.state == 1:  # RESULT
            return None
        else:
            return self._result

    def add_done_callback(self, cb, context=None):
        if not self.state or self.callbacks:
            self.callbacks.append(cb)
        else:
            cb(self)

    def remove_done_callback(self, cb):
        if self.state and self.callbacks:
            raise NotImplementedError("cannot remove callbacks when done")
        flt = [c for c in self.callbacks if c != cb]
        rv = len(self.callbacks) - len(flt)
        if rv:
            self.callbacks[:] = flt
        return rv

    def call_callbacks(self):
        for cb in self.callbacks:  # allows ``callbacks`` to grow
            cb(self)
        del self.callbacks[:]

    def set_result(self, result):
        if self.state:
            raise InvalidStateError("already done")
        self.state = 1  # RESULT
        self._result = result
        self.call_callbacks()

    def set_exception(self, exc):
        if self.state:
            raise InvalidStateError("already done")
        if isinstance(exc, type):
            exc = exc()
        self.state = 2  # EXCEPTION
        self._result = exc
        self.call_callbacks()

    def __await__(self):
        if not self.state:
            self._asyncio_future_blocking = True
            yield self
        return self.result()

    __iter__ = __await__

    def __str__(self):
        cls = self.__class__
        info = [cls.__module__ + "." + cls.__name__,
                ("PENDING", "RESULT", "EXCEPTION", "CANCELLED")[self.state],
                self._result,
                self.callbacks]
        return " ".join(str(x) for x in info)


class ConcurrentFuture(Future):
    """A future threads can wait on."""
    __slots__ = "completed",

    def __init__(self, loop=False):
        Future.__init__(self, loop=loop)
        self.completed = Event()

        @self.add_done_callback
        def complete(self):
            self.completed.set()
            switch_thread()

    def result(self, timeout=None):
        self.completed.wait(timeout)
        return Future.result(self)


def switch_thread():
    sleep(1e-6)


class CoroutineExecutor:
    """Execute a coroutine on behalf of a task.

    No context support.

    No ``cancel`` support (for the moment).
    """
    slots = "coro", "task", "awaiting"

    def __init__(self, task, coro):
        self.task = task  # likely creates a reference cycle
        self.coro = coro

    def step(self):
        self.awaiting = None
        try:
            result = self.coro.send(None)
        except BaseException as e:
            # we are done
            task = self.task
            self.task = None  # break reference cycle
            if isinstance(e, StopIteration):
                task.set_result(e.value)
            elif isinstance(e, CancelledError):
                task._cancel()
            else:
                task.set_exception(e)
                if isinstance(e, (KeyboardInterrupt, SystemExit)):
                    raise
        else:
            result._asyncio_future_blocking = False
            self.awaiting = result

            @result.add_done_callback
            def wakeup(unused, step=self.step):
                step()

    def cancel(self):
        raise NotImplementedError


class AsyncTask(Future):
    """Simplified ``asyncio.Task``.

    Steps are not scheduled but executed immediately.
    """
    __slots__ = "executor",

    def __init__(self, coro, loop=None):
        Future.__init__(self, loop=loop)
        self.executor = CoroutineExecutor(self, coro)  # reference cycle
        self.executor.step()

    def cancel(self, msg=None):
        """external cancel request."""
        return self.executor.cancel()

    def _cancel(self):
        """internal cancel request."""
        return Future.cancel(self)


class ConcurrentTask(ConcurrentFuture):
    """Task reporting to ``ConcurrentFuture``.

    Steps are not scheduled but executed immediately.
    """
    __slots__ = "executor",

    def __init__(self, coro, loop):
        ConcurrentFuture.__init__(self, loop=loop)
        self.executor = CoroutineExecutor(self, coro)  # reference cycle
        self._loop.call_soon_threadsafe(self.executor.step)
        # try to activate the IO thread as soon as possible
        switch_thread()

    def cancel(self, msg=None):
        """external cancel request."""
        return self.executor.cancel()

    def _cancel(self):
        """internal cancel request."""
        return ConcurrentFuture.cancel(self)


# use C implementation if available
try:
    from ._futures import Future, ConcurrentFuture  # noqa: F401, F811
    from ._futures import AsyncTask, ConcurrentTask  # noqa: F401, F811
    from ._futures import switch_thread  # noqa: F401, F811
except ImportError:
    pass

run_coroutine_threadsafe = ConcurrentTask
