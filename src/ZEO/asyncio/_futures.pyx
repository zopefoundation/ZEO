# cython: language_level=3

from asyncio import CancelledError, InvalidStateError, get_event_loop
from threading import Event
from time import sleep


cdef enum State:
    PENDING = 0
    RESULT = 1
    EXCEPTION = 2
    CANCELLED = 3


cdef class Future:
    """Minimal mostly ``asyncio`` compatible future.

    In contrast to an ``asyncio`` future,
    callbacks are called immediately, not scheduled;
    their context is ignored.
    """
    cdef public object _asyncio_future_blocking

    cdef public object _loop
    cdef State state
    cdef object _result
    cdef list callbacks

    def __init__(self, loop=None):
        self._asyncio_future_blocking = False
        self._loop = loop if loop is not None else get_event_loop()
        self.state = PENDING
        self._result = None
        self.callbacks = []

    def get_loop(self):
        return self._loop

    cpdef cancel(self, msg=None):
        """cancel the future if not done.

        Return ``True``, if really cancelled.

        *msg* is ignored.
        """
        if self.state:
            return False
        self.state = CANCELLED
        self._result = CancelledError()
        self.call_callbacks()
        return True

    def cancelled(self):
        return self.state == CANCELLED

    def done(self):
        return self.state

    cpdef result(self):
        if self.state == PENDING:
            raise InvalidStateError("not done")
        elif self.state == RESULT:
            return self._result
        else:
            raise self._result

    cpdef exception(self):
        if self.state == PENDING:
            raise InvalidStateError("not done")
        elif self.state == RESULT:
            return None
        else:
            return self._result

    cpdef add_done_callback(self, cb, context=None):
        if not self.state or self.callbacks:
            self.callbacks.append(cb)
        else:
            cb(self)

    cpdef remove_done_callback(self, cb):
        if self.state and self.callbacks:
            raise NotImplementedError("cannot remove callbacks when done")
        flt = [c for c in self.callbacks if c != cb]
        cdef int rv = len(self.callbacks) - len(flt)
        if rv:
            self.callbacks[:] = flt
        return rv

    cdef call_callbacks(self):
        for cb in self.callbacks:  # allows ``callbacks`` to grow
            try:
                cb(self)
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self._loop.call_exception_handler({
                        'message': 'Exception in callback %s' % (cb,),
                        'exception': exc,
                    })

        del self.callbacks[:]

    cpdef set_result(self, result):
        if self.state:
            raise InvalidStateError("already done")
        self.state = RESULT
        self._result = result
        self.call_callbacks()

    cpdef set_exception(self, exc):
        if self.state:
            raise InvalidStateError("already done")
        if isinstance(exc, type):
            exc = exc()
        self.state = EXCEPTION
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


cdef class ConcurrentFuture(Future):
    """A future threads can wait on."""
    cdef object completed

    def __init__(self, loop=False):
        Future.__init__(self, loop=loop)
        self.completed = Event()
        self.add_done_callback(self._complete)

    cpdef _complete(self, unused):
        self.completed.set()

    cpdef result(self, timeout=None):
        self.completed.wait(timeout)
        return Future.result(self)


cdef class CoroutineExecutor:
    """Execute a coroutine on behalf of a task.

    No context support.

    No ``cancel`` support (for the moment).
    """
    cdef object coro  # executed coroutine
    cdef object task   # associated task
    cdef object awaiting  # future we are waiting for

    def __init__(self, task, coro):
        """execute *coro* on behalf of *task*."""
        self.task = task  # likely introduces a reference cycle
        self.coro = coro

    cpdef step(self):
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
            assert getattr(result, '_asyncio_future_blocking', None)
            result._asyncio_future_blocking = False
            self.awaiting = result
            result.add_done_callback(self.wakeup)

    cpdef wakeup(self, unused):
        self.step()

    def cancel(self):
        raise NotImplementedError


cdef class AsyncTask(Future):
    """Simplified ``asyncio.Task``.

    Steps are not scheduled but executed immediately.
    """
    cdef object executor

    def __init__(self, coro, loop=None):
        Future.__init__(self, loop=loop)
        self.executor = CoroutineExecutor(self, coro)  # reference cycle
        self.executor.step()

    def cancel(self, msg=None):
        return self.executor.cancel()

    def _cancel(self):
        return Future.cancel(self)


cdef class ConcurrentTask(ConcurrentFuture):
    """Task reporting to ``ConcurrentFuture``.

    Steps are not scheduled but executed immediately.
    """
    cdef object executor

    def __init__(self, coro, loop):
        ConcurrentFuture.__init__(self, loop=loop)
        self.executor = CoroutineExecutor(self, coro)  # reference cycle
        loop.call_soon_threadsafe(self.executor.step)

    def cancel(self, msg=None):
        return self.executor.cancel()

    def _cancel(self):
        return ConcurrentFuture.cancel(self)


run_coroutine_threadsafe = ConcurrentTask
