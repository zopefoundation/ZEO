"""Optimized variants of ``asyncio``'s ``Future``.

``asyncio`` schedules callbacks to be executed in the next
loop run. This increases the number of loop runs necessary to
obtain the result of a ZEO server request and adds significant
latency (+ 27% in some benchmarks).
This module defines variant which run callbacks immediately.
"""

from .compat import asyncio
import functools
import six
CancelledError = asyncio.CancelledError
InvalidStateError = asyncio.InvalidStateError
get_event_loop = asyncio.get_event_loop


# ``Future`` states -- inlined below for speed
PENDING = 0
RESULT = 1
EXCEPTION = 2
CANCELLED = 3


class Future(object):
    """Minimal mostly ``asyncio`` compatible future.

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

    if six.PY3:
        # return from generator raises SyntaxError on py2
        exec('''if 1:
        def __await__(self):
            if not self.state:
                self._asyncio_future_blocking = True
                yield self
            return self.result()
        ''')
    else:
        def __await__(self):
            if not self.state:
                self._asyncio_future_blocking = True
                yield self
            raise asyncio.Return(self.result())

    __iter__ = __await__

    def __str__(self):
        cls = self.__class__
        info = [cls.__module__ + "." + cls.__name__,
                ("PENDING", "RESULT", "EXCEPTION", "CANCELLED")[self.state],
                self._result,
                self.callbacks]
        return " ".join(str(x) for x in info)


def future_generator(func):
    """Decorates a generator that generates futures
    """

    @functools.wraps(func)
    def call_generator(*args, **kw):
        gen = func(*args, **kw)
        try:
            f = next(gen)
        except StopIteration:
            gen.close()
        else:
            def store(gen, future):
                @future.add_done_callback
                def _(future):
                    try:
                        try:
                            result = future.result()
                        except Exception as exc:
                            f = gen.throw(exc)
                        else:
                            f = gen.send(result)
                    except StopIteration:
                        gen.close()
                    else:
                        store(gen, f)

            store(gen, f)

    return call_generator
