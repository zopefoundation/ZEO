"""Python 3 implementation for ``connect_coroutine``."""

from .compat import asyncio

async def connect_coroutine(cr, self, logger, random):
    while not self.closed:
        try:
            return await cr()
        except asyncio.CancelledError:
            logger.info("Connection to %r cancelled", self.addr)
            raise
        except Exception as exc:
            logger.info("Connection to %r failed, %s",
                        self.addr, exc)
        await asyncio.sleep(self.connect_poll + random())
        logger.info("retry connecting %r", self.addr)

