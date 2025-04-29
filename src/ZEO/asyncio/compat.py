try:
    from uvloop import new_event_loop
except ModuleNotFoundError:
    from asyncio import new_event_loop
