import time, asyncio
from typing import Callable, Any, Awaitable, Type, TypeVar, ParamSpec
from functools import wraps
from math import ceil
import logging


BACKOFF_NUM_SECONDS = 1

P = ParamSpec('P')
R = TypeVar('R')

## Allows concurrent calls to an async function, but if a predefined exception happens (e.g. TOO_MANY_REQUESTS),
## all calls will be blocked for a defined backoff interval.
class AdaptiveRateLimit:
    def __init__(self, backoff_exception: Type[BaseException], backoff_interval_sec: int):
        self._backoff_until = 0
        self._backoff_interval_sec = backoff_interval_sec
        self._backoff_exception = backoff_exception

    def __call__(self, func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @wraps(func)
        async def inner(*args: P.args, **kwds: P.kwargs) -> R:
            while True:
                # If we're in a back-off period, wait until it ends.
                wait_secs = ceil(self._backoff_until - time.monotonic())
                while wait_secs > 0:
                    await asyncio.sleep(wait_secs)
                    wait_secs = ceil(self._backoff_until - time.monotonic())

                # Run the actual function
                try:
                    result = await func(*args, **kwds)
                    return result
                except self._backoff_exception:
                    logging.warning("Rate limit exception detected. Backing off.")
                    self._backoff_until = ceil(time.monotonic() + self._backoff_interval_sec)

        return inner
