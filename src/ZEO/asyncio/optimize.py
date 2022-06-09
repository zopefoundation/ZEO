"""Optimized variants of ``asyncio``'s ``Future`` and ``Task``.

``asyncio`` schedules callbacks to be executed in the next
loop run. This increases the number of loop runs necessary to
obtain the result of a ZEO server request and adds significant
latency (+ 27% in some benchmarks).
This module defines variants which run callbacks immediately.
"""

from asyncio import CancelledError
from asyncio.base_tasks import _task_repr_info
from asyncio.futures import _PyFuture as PyFuture
from concurrent.futures import Future as ConcurrentFuture


class Future(PyFuture):
    """a ``Future`` calling (rather than scheduling) callbacks.

    Context provided for callbacks is ignored.
    """
    def __schedule_callbacks(self):
        # make this empty to avoid the scheduling
        return

    _schedule_callbacks = __schedule_callbacks  # for older versions

    def _call_callbacks(self):
        """replacement for ``__schedule_callbacks``, calling directly."""
        for callback in self._callbacks:  # allows new callbacks
            callback(self)
        self._callbacks[:] = []

    def cancel(self, msg=None):
        if super().cancel():  # older versions do support ``msg``
            self._call_callbacks()
            return True
        return False

    def set_exception(self, exception):
        super().set_exception(exception)
        self._call_callbacks()

    def set_result(self, result):
        super().set_result(result)
        self._call_callbacks()

    def add_done_callback(self, fn, *, context=None):
        """Add a callback to be run when the future becomes done.

        The callback is called with a single argument - the future object. If
        the future is already done when this is called, the callback
        is called immediately.

        ATT: ``context`` is ignored
        """
        if not self.done() or self._callbacks:
            self._callbacks.append(fn)
        else:
            fn(self)

    def remove_done_callback(self, fn):
        """Remove all instances of a callback from the "call when done" list.

        Returns the number of callbacks removed.
        """
        if self.done() and self._callbacks:
            raise NotImplementedError("cannot remove callbacks when done")
        filtered_callbacks = [f for f in self._callbacks if f != fn]
        removed_count = len(self._callbacks) - len(filtered_callbacks)
        if removed_count:
            self._callbacks[:] = filtered_callbacks
        return removed_count


class CoroutineExecutor:
    """Mixin to provide simplified ``task`` essentials.

    No context support.

    No ``cancel`` support (for the moment).
    """

    # to be defined by derived classes
    # ASYNC = None

    def __init__(self, coro, loop=None):
        self._loop = loop
        self._must_cancel = False
        self._fut_waiter = None
        self._coro = coro
        if self.ASYNC:
            super().__init__(loop=loop)
            self._step()
        else:
            super().__init__()
            self._loop.call_soon_threadsafe(self._step)

    _repr_info = _task_repr_info

    def get_name(self):
        return "ZEO optimized task"

    def cancel(self, msg=None):
        raise NotImplementedError

    def _step(self, exc=None):
        """run coroutine until next ``await`` or completion."""
        assert not self.done()
        coro = self._coro
        self._fut_waiter = None
        try:
            if exc is None:
                result = coro.send(None)
            else:
                result = coro.throw(exc)
        except StopIteration as exc:
            super().set_result(exc.value)
        except CancelledError as exc:
            exc.__traceback__ = None  # avoid cyclic garbage
            super().cancel()  # I.e., Future.cancel(self).
        except (KeyboardInterrupt, SystemExit) as exc:
            super().set_exception(exc)
            raise
        except BaseException as exc:
            super().set_exception(exc)
        else:
            blocking = getattr(result, '_asyncio_future_blocking', None)
            assert blocking
            result._asyncio_future_blocking = False
            self._fut_waiter = result
            if self._must_cancel:
                if self._fut_waiter.cancel():
                    self._must_cancel = False

            @result.add_done_callback
            def wakeup(unused, step=self._step):
                step()
        finally:
            self = None  # Needed to break cycles when an exception occurs.


class AsyncTask(CoroutineExecutor, Future):
    """simplified ``asyncio.Task``

    Steps are not scheduled but executed immediately.
    """
    ASYNC = True


PyAsyncTask = AsyncTask


class ConcurrentTask(CoroutineExecutor, ConcurrentFuture):
    """Concurrent task"""
    ASYNC = False

    # Note: might need to redefine ``_repr_info``


PyConcurrentTask = ConcurrentTask

run_coroutine_threadsafe = ConcurrentTask
py_run_coroutine_threadsafe = run_coroutine_threadsafe

try:
    from ._opt import Future, AsyncTask, ConcurrentTask,\
         run_coroutine_threadsafe
except ImportError:
    pass
