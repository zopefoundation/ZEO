"""Optimized variants of ``asyncio``'s ``Future``.

``asyncio`` schedules callbacks to be executed in the next
loop run. This increases the number of loop runs necessary to
obtain the result of a ZEO server request and adds significant
latency (+ 27% in some benchmarks).
This module defines variant which run callbacks immediately.
"""

import functools


_missing = object()


class Fut(object):
    """Lightweight future that calls it's callbacks immediately ...

    rather than soon.
    """

    def __init__(self):
        self.cbv = []

    def add_done_callback(self, cb):
        self.cbv.append(cb)

    _result = _missing
    exc = None

    def set_exception(self, exc):
        self.exc = exc
        for cb in self.cbv:
            cb(self)

    def set_result(self, result):
        self._result = result
        for cb in self.cbv:
            cb(self)

    def result(self):
        if self.exc:
            raise self.exc
        else:
            return self._result

    def done(self):
        return (self._result is not _missing) or (self.exc is not None)


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
