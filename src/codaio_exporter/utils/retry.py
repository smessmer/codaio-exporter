from __future__ import annotations

import asyncio
import logging
import random
import sys
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")


## Function decorator that retries on exceptions, at most `max_num_retries` retries, i.e. `max_num_retries + 1` calls
## Only `Exception`s are retried. Other `BaseException`s (e.g. `asyncio.CancelledError`, `KeyboardInterrupt`) propagate
## immediately, so that cancelling a task or interrupting the program isn't delayed or ignored.
def retry(max_num_retries: int) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @wraps(func)
        async def inner(*args: P.args, **kwds: P.kwargs) -> R:
            remaining_retries = max_num_retries
            while True:
                try:
                    return await func(*args, **kwds)
                except Exception:
                    remaining_retries -= 1
                    if remaining_retries < 0:
                        raise
                    else:
                        logging.warning(f"Encountered error {sys.exc_info()[0]}. Retrying ({remaining_retries} remaining attempts)...")
                        logging.debug(f"Error was {sys.exc_info()[1]}")
                        # Sleep with jitter (5-10s) to avoid thundering herd on retry
                        await asyncio.sleep(5 + random.random() * 5)

        return inner

    return decorator
