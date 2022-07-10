from typing import ParamSpec, TypeVar, Callable, Awaitable
from functools import wraps
import logging
import asyncio
import sys


P = ParamSpec('P')
R = TypeVar('R')


## Function decorator that retries on exceptions, at most `max_num_retries` retries, i.e. `max_num_retries + 1` calls
def retry(max_num_retries: int) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @wraps(func)
        async def inner(*args: P.args, **kwds: P.kwargs) -> R:
            remaining_retries =  max_num_retries
            while True:
                try:
                    return await func(*args, **kwds)
                except:
                    remaining_retries -= 1
                    if remaining_retries < 0:
                        raise
                    else:
                        logging.warn(f"Encountered error {sys.exc_info()[0]}. Retrying ({remaining_retries} remaining attempts)...")
                        logging.debug(f"Error was {sys.exc_info()[1]}")
                        # Let's sleep a bit just in case the server is in a temporarily bad state
                        await asyncio.sleep(1)
        return inner
    return decorator
