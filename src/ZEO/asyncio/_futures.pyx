# cython: language_level=3

from .compat import asyncio
cdef object CancelledError = asyncio.CancelledError
cdef object InvalidStateError = asyncio.InvalidStateError
cdef object get_event_loop = asyncio.get_event_loop
import inspect
from threading import Event, Lock
from ZEO._compat import get_ident

from cpython cimport PY_MAJOR_VERSION


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
        exc_stop = None
        for cb in self.callbacks:  # allows ``callbacks`` to grow
            try:
                cb(self)
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                if PY_MAJOR_VERSION < 3:
                    # trollius stops the loop by raising _StopError
                    # we delay loop stopping till after all callbacks are invoked
                    # this goes in line with py3 behaviour
                    if isinstance(exc, asyncio.base_events._StopError):
                        exc_stop = exc
                        continue

                self._loop.call_exception_handler({
                        'message': 'Exception in callback %s' % (cb,),
                        'exception': exc,
                    })

        del self.callbacks[:]
        if exc_stop is not None:
            raise exc_stop

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

    # trollius and py3 < py3.7 access ._exception directly
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

    if PY_MAJOR_VERSION >= 3:  # py3-only because cyclic garbage with __del__ is not collected on py2
        def __del__(self):
            if self.state == EXCEPTION  and  not self._result_retrieved:
                self._loop.call_exception_handler({
                    'message': "%s exception was never retrieved" % self.__class__.__name__,
                    'exception': self._result,
                    'future': self,
                })

# py3: asyncio.isfuture checks ._asyncio_future_blocking
# py2: trollius does isinstace(_FUTURE_CLASSES)
# -> register our Future so that it is recognized as such by trollius
if PY_MAJOR_VERSION < 3:
    _ = asyncio.futures._FUTURE_CLASSES
    if not isinstance(_, tuple):
        _ = (_,)
    _ += (Future,)
    asyncio.futures._FUTURE_CLASSES = _
    del _


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


cpdef switch_thread():
    with nogil:
        pass


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
            if isinstance(e, (StopIteration, _GenReturn)):
                if PY_MAJOR_VERSION < 3:
                    v = getattr(e, 'value', None)  # e.g. no .value on plain return
                    if hasattr(e, 'raised'):  # coroutines implemented inside trollius raise Return
                        e.raised = True       # which checks it has been caught and complains if not
                else:
                    v = e.value
                task.set_result(v)
            elif isinstance(e, CancelledError):
                if len(e.args) == 0:
                    msg = getattr(awaiting, '_xasyncio_cancel_msg', None)  # see _cancel_future
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
            elif PY_MAJOR_VERSION < 3 and isinstance(result, asyncio.Future):
                await_next = result  # trollius predates ._asyncio_future_blocking

            # `yield coro` - handle as if it was `yield from coro`
            elif _iscoroutine(result):
                # NOTE - always AsyncTask even if we are originally under ConcurrentTask
                await_next = AsyncTask(result, self.task.get_loop())

            else:
                # object with __await__ - e.g. @cython.iterable_coroutine used by uvloop
                risawaitable = True
                try:
                    rawait = result.__await__()
                except AttributeError:
                    risawaitable = False
                else:
                    # cython.iterable_coroutine returns `coroutine_wrapper` that mimics
                    # iterator/generator but does not inherit from types.GeneratorType .
                    await_next = AsyncTask(rawait, self.task.get_loop())

                if not risawaitable:
                    # bare yield
                    if result is None:
                        await_next = Future(self.task.get_loop())
                        await_next.set_result(None)

                    # bad yield
                    else:
                        await_next = Future(self.task.get_loop())
                        await_next.set_exception(
                                RuntimeError("Task got bad yield: %r" % (result,)))

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
# if fut does not support cancelling with message, the message is saved in fut._xasyncio_cancel_msg .
cdef _cancel_future(fut, msg):
    try:
        return fut.cancel(msg)
    except TypeError:
        # on trollius and py3 < 3.9 Future.cancel does not accept msg
        _ = fut.cancel()
        fut._xasyncio_cancel_msg = msg
        return _


cdef class AsyncTask(Future):
    """Simplified ``asyncio.Task``.

    Steps are not scheduled but executed immediately.
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

    Steps are not scheduled but executed immediately.
    Cancel can be used from any thread.
    """
    cdef CoroutineExecutor executor
    cdef long loop_thread_id

    def __init__(self, coro, loop):
        ConcurrentFuture.__init__(self, loop=loop)
        self.executor = CoroutineExecutor(self, coro)  # reference cycle
        self.loop_thread_id = -1
        loop.call_soon_threadsafe(self._start)

    cpdef _start(self):
        # asyncio.Loop has ._thread_id, but uvloop does not expose it
        self.loop_thread_id = get_ident()  # with gil
        self.executor.step()

    cpdef cancel(self, msg=None):
        """external cancel request.

        cancel requests cancellation of the task.
        it is safe to call cancel from any thread.
        """
        # invoke CoroutineExecutor.cancel on the loop thread and wait for its result.
        # but run it directly to avoid deadlock if we are already on the loop thread.
        if get_ident() == self.loop_thread_id:  # with gil
            return self.executor.cancel(msg)
        self._cancel_via_loopthread(msg)

    def _cancel_via_loopthread(self, msg):  # cpdef does not allow to use closures
        sema = Lock()
        sema.acquire()
        res = [None]
        def _():
            try:
                x = self.executor.cancel(msg)
            except BaseException as e:
                x = e
                if PY_MAJOR_VERSION < 3: # trollius stops the loop by raising _StopError
                    if isinstance(e, asyncio.base_events._StopError):
                        x = True
                        raise
            finally:
                res[0] = x
                sema.release()
        self._loop.call_soon_threadsafe(_)
        sema.acquire() # wait for the call to complete
        r = res[0]
        if isinstance(r, BaseException):
            raise r
        return r


    def _cancel(self, msg):
        """internal cancel request."""
        return ConcurrentFuture.cancel(self, msg)


# @coroutine - only py implementtion

cdef _iscoroutine(obj):
    """_iscoroutine checks whether obj is coroutine object."""
    if inspect.isgenerator(obj) or asyncio.iscoroutine(obj):
        return True
    else:
        return False

cpdef return_(x):
    """return_(x) should be used instead of ``return x`` in coroutine functions.

    It exists to support Python2 where ``return x`` is rejected inside generators.
    """
    # py3:     disallows to explicitly raise StopIteration from inside generator
    # py2/py3: StopIteration inherits from Exception (not from BaseException)
    # -> use our own exception type, that mimics StopIteration, but that can be
    #    raised from inside generator and that is not caught by `except Exception`.
    e = _GenReturn(x)
    e.value = x
    raise e

class _GenReturn(BaseException):  # note: base != Exception to prevent catching
    __slots__ = "value"           # returns inside `except Exception` in the same function


run_coroutine_threadsafe = ConcurrentTask
