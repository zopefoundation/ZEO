# cython: language_level=3
"""``cython`` implementation of ``futures.py``.

Please see its docstring for details.
"""

import asyncio


cdef object CancelledError = asyncio.CancelledError
cdef object InvalidStateError = asyncio.InvalidStateError
cdef object get_event_loop = asyncio.get_event_loop
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
    cdef bint _result_retrieved

    def __init__(self, loop=None):
        self._asyncio_future_blocking = False
        self._loop = loop if loop is not None else get_event_loop()
        self.state = PENDING
        self._result = None
        self.callbacks = []
        self._result_retrieved = False

    def get_loop(self):
        return self._loop

    cpdef cancel(self, msg=None):
        """cancel the future if not done.

        Return ``True``, if really cancelled.
        """
        if self.state:
            return False
        self.state = CANCELLED
        self._result = CancelledError()  if msg is None  else \
                       CancelledError(msg)
        self.call_callbacks()
        return True

    def cancelled(self):
        return self.state == CANCELLED

    def done(self):
        return self.state

    cpdef result(self):
        if self.state == PENDING:
            raise InvalidStateError("not done")
        self._result_retrieved = True
        if self.state == RESULT:
            return self._result
        else:
            raise self._result

    cpdef exception(self):
        if self.state == PENDING:
            raise InvalidStateError("not done")
        self._result_retrieved = True
        if self.state == RESULT:
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

    # py3 accesses ._exception directly
    @property
    def _exception(self):
        if self.state != EXCEPTION:
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
        if self.state == EXCEPTION  and  not self._result_retrieved:
            self._loop.call_exception_handler({
                'message': "%s exception was never retrieved" % self.__class__.__name__,
                'exception': self._result,
                'future': self,
            })


cdef class ConcurrentFuture(Future):
    """A future threads can wait on.

    Note: this differs from concurrent.future.Future - hereby ConcurrentFuture
    is generally _not_ concurrent - only .result() is allowed to be called from
    different threads and provides semantic similar to concurrent.future.Future.
    """
    cdef object completed

    def __init__(self, loop=False):
        Future.__init__(self, loop=loop)
        self.completed = Event()
        self.add_done_callback(self._complete)

    cpdef _complete(self, unused):
        self.completed.set()
        switch_thread()

    cpdef result(self, timeout=None):
        """result waits till the future is done and returns its result.

        If the future isn't done in specified time TimeoutError(*) is raised.

        (*) NOTE: it is asyncio.TimeoutError, not concurrent.futures.TimeoutError,
            which is raised for uniformity.
        """
        if not self.completed.wait(timeout):
            raise asyncio.TimeoutError()
        return Future.result(self)


IF UNAME_SYSNAME == "Windows":
    cpdef switch_thread():
        sleep(1e-6)
ELSE:
    cdef extern from "<sys/select.h>" nogil:
        ctypedef struct fd_set
        cdef struct timeval:
           long tv_sec
           unsigned long tv_usec
        int select(int no, fd_set *rd, fd_set *wr, fd_set *ex,
                   timeval *to) nogil
    cpdef switch_thread():
        cdef timeval timeout
        timeout.tv_sec = 0; timeout.tv_usec = 1
        with nogil:
            select(0, NULL, NULL, NULL, &timeout)


cdef class CoroutineExecutor:
    """Execute a coroutine on behalf of a task.

    No context support.
    """
    cdef object coro  # executed coroutine
    cdef object task   # associated task
    cdef object awaiting  # future we are waiting for
    cdef bint   cancel_requested  # request to be canceled from .cancel
    cdef object cancel_msg  # ^^^ cancellation message

    def __init__(self, task, coro):
        """execute *coro* on behalf of *task*."""
        self.task = task  # likely introduces a reference cycle
        self.coro = coro
        self.awaiting = None
        self.cancel_requested = False
        self.cancel_msg = None

    cpdef step(self):
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
                        RuntimeError("Task got bad await: %r" % (result,)))

            if self.cancel_requested:
                _cancel_future(await_next, self.cancel_msg)
                self.cancel_requested = False
                self.cancel_msg = None

            self.awaiting = await_next
            await_next.add_done_callback(self.wakeup)

            awaiting = None
            await_next = None

    cpdef wakeup(self, unused):
        self.step()

    cpdef cancel(self, msg):
        """cancel requests cancellation of the coroutine.

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
cdef _cancel_future(fut, msg):
    try:
        return fut.cancel(msg)
    except TypeError:
        # on py3 < 3.9 Future.cancel does not accept msg
        _ = fut.cancel()
        fut._cancel_message = msg
        return _


cdef class AsyncTask(Future):
    """Simplified ``asyncio.Task``.

    Steps are not scheduled but executed immediately; the context is ignored.
    """
    cdef CoroutineExecutor executor

    def __init__(self, coro, loop=None):
        Future.__init__(self, loop=loop)
        self.executor = CoroutineExecutor(self, coro)  # reference cycle
        self.executor.step()

    cpdef cancel(self, msg=None):
        """external cancel request."""
        return self.executor.cancel(msg)

    def _cancel(self, msg):
        """internal cancel request."""
        return Future.cancel(self, msg)


cdef class ConcurrentTask(ConcurrentFuture):
    """Task reporting to ``ConcurrentFuture``.

    Steps are not scheduled but executed immediately; the context is ignored.
    Cancel can be used only from IO thread.
    """
    cdef CoroutineExecutor executor

    def __init__(self, coro, loop):
        ConcurrentFuture.__init__(self, loop=loop)
        self.executor = CoroutineExecutor(self, coro)  # reference cycle
        loop.call_soon_threadsafe(self._start)

    cpdef _start(self):
        self.executor.step()

    cpdef cancel(self, msg=None):
        """external cancel request.

        cancel requests cancellation of the task.
        it is safe to call cancel only from IO thread.
        """
        return self.executor.cancel(msg)

    def _cancel(self, msg):
        """internal cancel request."""
        return ConcurrentFuture.cancel(self, msg)


# @coroutine - only py implementtion


run_coroutine_threadsafe = ConcurrentTask
