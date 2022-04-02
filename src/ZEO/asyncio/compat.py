try:
    from uvloop import new_event_loop
except ImportError:
    from asyncio import new_event_loop
