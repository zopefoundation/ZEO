"""Python 2 implementation for ``connect_coroutine``."""

from .compat import asyncio

@asyncio.coroutine
def connect_coroutine(cr, self, logger, random):
    while not self.closed:
        try:
            yield cr()
            return
        except asyncio.CancelledError:
            logger.info("Connection to %r cancelled", self.addr)
            raise
        except Exception as exc:
            logger.info("Connection to %r failed, %s",
                        self.addr, exc)
        yield asyncio.sleep(self.connect_poll + random())
        logger.info("retry connecting %r", self.addr)

