"""Optimized variants of ``asyncio``'s ``Future`` and ``Task``.

``asyncio`` schedules callbacks to be executed in the next
loop run. This increases the number of loop runs necessary to
obtain the result of a ZEO server request and adds significant
latency (+ 27% in some benchmarks).
This module defines variants which run callbacks immediately
and ignore a context.

The tasks defined by this module build on those futures
and in addition do not implement a task context. If you do not
know the coroutine run by the task, it is safer to use
a standard ``asyncio.Task``.
"""

import asyncio
from asyncio import CancelledError
from asyncio import InvalidStateError
from asyncio import get_event_loop
from threading import Event
from time import sleep


# ``Future`` states -- inlined below for speed
PENDING = 0
RESULT = 1
EXCEPTION = 2
CANCELLED = 3


class Future:
    """Minimal mostly ``asyncio`` compatible future.

    In contrast to an ``asyncio`` future,
    callbacks are called immediately, not scheduled;
    their context is ignored.
    """
    __slots__ = ("_loop", "state", "_result", "callbacks",
                 "_asyncio_future_blocking", "_result_retrieved")

    def __init__(self, loop=None):
        self._loop = loop if loop is not None else get_event_loop()
        self.state = 0  # PENDING
        self._result = None
        self.callbacks = []
        self._asyncio_future_blocking = False
        self._result_retrieved = False

    def get_loop(self):
        return self._loop

    def cancel(self, msg=None):
        """cancel the future if not done.

        Return ``True``, if really cancelled.
        """
        if self.state:
            return False
        self.state = 3  # CANCELLED
        self._result = CancelledError()  if msg is None  else \
                       CancelledError(msg)
        self.call_callbacks()
        return True

    def cancelled(self):
        return self.state == 3  # CANCELLED

    def done(self):
        return self.state

    def result(self):
        if self.state == 0:  # PENDING
            raise InvalidStateError("not done")
        self._result_retrieved = True
        if self.state == 1:  # RESULT
            return self._result
        else:
            raise self._result

    def exception(self):
        if self.state == 0:  # PENDING
            raise InvalidStateError("not done")
        self._result_retrieved = True
        if self.state == 1:  # RESULT
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
            try:
                cb(self)
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self._loop.call_exception_handler({
                        'message': f'Exception in callback {cb}',
                        'exception': exc,
                    })

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

    # py3 accesses ._exception directly
    @property
    def _exception(self):
        if self.state != 2:  # EXCEPTION
            return None
        return self._result

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

    def __del__(self):
        if self.state == 2  and  not self._result_retrieved:  # EXCEPTION
            self._loop.call_exception_handler({
                'message': "%s exception was never retrieved" % self.__class__.__name__,
                'exception': self._result,
                'future': self,
            })


class ConcurrentFuture(Future):
    """A future threads can wait on.

    Note: this differs from concurrent.future.Future - hereby ConcurrentFuture
    is generally _not_ concurrent - only .result() is allowed to be called from
    different threads and provides semantic similar to concurrent.future.Future.
    """
    __slots__ = "completed",

    def __init__(self, loop=False):
        Future.__init__(self, loop=loop)
        self.completed = Event()

        @self.add_done_callback
        def complete(self):
            self.completed.set()
            switch_thread()

    def result(self, timeout=None):
        """result waits till the future is done and returns its result.

        If the future isn't done in specified time TimeoutError(*) is raised.

        (*) NOTE: it is asyncio.TimeoutError, not concurrent.futures.TimeoutError,
            which is raised for uniformity.
        """
        if not self.completed.wait(timeout):
            raise asyncio.TimeoutError()
        return Future.result(self)


def switch_thread():
    sleep(1e-6)


class CoroutineExecutor:
    """Execute a coroutine on behalf of a task.

    No context support.
    """
    __slots__ = "coro", "task", "awaiting", "cancel_requested", "cancel_msg"

    def __init__(self, task, coro):
        self.task = task  # likely creates a reference cycle
        self.coro = coro
        self.awaiting = None
        self.cancel_requested = False
        self.cancel_msg = None

    def step(self):
        await_result = None  # with what to wakeup suspended await
        await_resexc = False # is it exception?
        awaiting = self.awaiting
        if awaiting is not None:
            self.awaiting = None
            assert awaiting.done()
            try:
                await_result = awaiting.result()
            except BaseException as e:
                await_result = e
                await_resexc = True
        if self.cancel_requested:
            await_result = CancelledError()  if self.cancel_msg is None  else \
                           CancelledError(self.cancel_msg)
            await_resexc = True
            self.cancel_requested = False
            self.cancel_msg = None
        try:
            if not await_resexc:
                result = self.coro.send(await_result)
            else:
                result = self.coro.throw(await_result)
        except BaseException as e:
            # we are done
            task = self.task
            self.task = None  # break reference cycle
            if isinstance(e, StopIteration):
                task.set_result(e.value)
            elif isinstance(e, CancelledError):
                if len(e.args) == 0:
                    msg = getattr(awaiting, '_cancel_message', None)  # see _cancel_future
                elif len(e.args) == 1:
                    msg = e.args[0]
                else:
                    msg = e.args
                task._cancel(msg)
            else:
                task.set_exception(e)
                if isinstance(e, (KeyboardInterrupt, SystemExit)):
                    raise
        else:
            await_next = None
            # yielded Future - wait on it
            blocking = getattr(result, '_asyncio_future_blocking', None)
            if blocking is not None:
                result._asyncio_future_blocking = False
                await_next = result

            # bad await
            else:
                await_next = Future(self.task.get_loop())
                await_next.set_exception(
                        RuntimeError("Task got bad await: {!r}".format(result)))

            if self.cancel_requested:
                _cancel_future(await_next, self.cancel_msg)
                self.cancel_requested = False
                self.cancel_msg = None

            self.awaiting = await_next

            @await_next.add_done_callback
            def wakeup(unused, step=self.step):
                step()

            awaiting = None
            await_next = None


    def cancel(self, msg):
        """request cancellation of the coroutine.

        It is safe to call cancel only from the same thread where coroutine is executed.
        """
        awaiting = self.awaiting
        if awaiting is not None:
            if _cancel_future(awaiting, msg):
                return True
        self.cancel_msg = msg
        self.cancel_requested = True
        task = self.task
        if task is None or task.done():
            return False
        return True

# _cancel_future cancels future fut with message msg.
# if fut does not support cancelling with message, the message is saved in fut._cancel_message .
def _cancel_future(fut, msg):
    try:
        return fut.cancel(msg)
    except TypeError:
        # on py3 < 3.9 Future.cancel does not accept msg
        _ = fut.cancel()
        fut._cancel_message = msg
        return _


class AsyncTask(Future):
    """Simplified ``asyncio.Task``.

    Steps are not scheduled but executed immediately; the context is ignored.
    """
    __slots__ = "executor",

    def __init__(self, coro, loop=None):
        Future.__init__(self, loop=loop)
        self.executor = CoroutineExecutor(self, coro)  # reference cycle
        self.executor.step()

    def cancel(self, msg=None):
        """external cancel request."""
        return self.executor.cancel(msg)

    def _cancel(self, msg):
        """internal cancel request."""
        return Future.cancel(self, msg)


class ConcurrentTask(ConcurrentFuture):
    """Task reporting to ``ConcurrentFuture``.

    Steps are not scheduled but executed immediately; the context is ignored.
    Cancel can be used only from IO thread.
    """
    __slots__ = "executor",

    def __init__(self, coro, loop):
        ConcurrentFuture.__init__(self, loop=loop)
        self.executor = CoroutineExecutor(self, coro)  # reference cycle
        self._loop.call_soon_threadsafe(self.executor.step)

    def cancel(self, msg=None):
        """external cancel request.

        cancel requests cancellation of the task.
        it is safe to call cancel only from IO thread.
        """
        return self.executor.cancel(msg)

    def _cancel(self, msg):
        """internal cancel request."""
        return ConcurrentFuture.cancel(self, msg)


# use C implementation if available
try:
    from ._futures import AsyncTask  # noqa: F401, F811
    from ._futures import ConcurrentFuture
    from ._futures import ConcurrentTask
    from ._futures import Future
    from ._futures import switch_thread  # noqa: F401, F811
except ImportError:
    pass

run_coroutine_threadsafe = ConcurrentTask
