"""Optimized variants of ``asyncio``'s ``Future`` and ``Task``.

``asyncio`` schedules callbacks to be executed in the next
loop run. This increases the number of loop runs necessary to
obtain the result of a ZEO server request and adds significant
latency (+ 27% in some benchmarks).
This module defines variants which run callbacks immediately.
"""

from asyncio import CancelledError, InvalidStateError, get_event_loop
from asyncio.base_tasks import _task_repr_info
from concurrent.futures import Future as ConcurrentFuture


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
    __slots__ = ("loop", "state", "_result", "callbacks",
                 "_asyncio_future_blocking")

    def __init__(self, loop=None):
        self.loop = loop if loop is not None else get_event_loop()
        self.state = 0  # PENDING
        self._result = None
        self.callbacks = []
        self._asyncio_future_blocking = False
        
    def get_loop(self):
        return self.loop

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
            assert getattr(result, "_asyncio_future_blocking", None)
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
        super().__init__(loop=loop)
        self.executor = CoroutineExecutor(self, coro)  # reference cycle
        self.executor.step()

    def cancel(self, msg=None):
        return self.executor.cancel()

    def _cancel(self):
        return super().cancel()


class ConcurrentTask(ConcurrentFuture):
    """Task reporting to ``ConcurrentFuture``.

    Steps are not scheduled but executed immediately.
    """

    def __init__(self, coro, loop):
        super().__init__()
        self.executor = CoroutineExecutor(self, coro)  # reference cycle
        loop.call_soon_threadsafe(self.executor.step)

    def cancel(self, msg=None):
        return self.executor.cancel()

    def _cancel(self):
        return super().cancel()


run_coroutine_threadsafe = ConcurrentTask
