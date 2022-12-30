# cython: language_level=3

from .compat import asyncio
cdef object CancelledError = asyncio.CancelledError
cdef object InvalidStateError = asyncio.InvalidStateError
cdef object get_event_loop = asyncio.get_event_loop
import inspect
from threading import Event
from time import sleep

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

    # trollius accesses ._exception directly
    if PY_MAJOR_VERSION < 3:
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
        self.awaiting = None

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
                task._cancel()
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

            self.awaiting = await_next
            await_next.add_done_callback(self.wakeup)

            awaiting = None
            await_next = None

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
