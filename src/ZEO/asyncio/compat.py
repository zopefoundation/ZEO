from .._compat import PY3
if PY3:
    import asyncio
    try:
        from uvloop import new_event_loop
    except ImportError:
        from asyncio import new_event_loop
else:
    import trollius as asyncio  # NOQA: F401 unused import
    from trollius import new_event_loop  # NOQA: F401 unused import
