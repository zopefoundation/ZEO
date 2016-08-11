from .._compat import PY3
if PY3:
    import asyncio
    try:
        from uvloop import new_event_loop
    except ImportError:
        from asyncio import new_event_loop
else:
    import trollius as asyncio
    from trollius import new_event_loop
