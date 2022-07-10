from typing import Callable, Awaitable, TypeVar, ParamSpec, final, Final
from asyncio import Semaphore
from functools import wraps


P = ParamSpec('P')
R = TypeVar('R')

## Allows at most `max_num_tasks` concurrent calls and blocks other calls until a running one has returned
@final
class ConcurrencyLimit:
    def __init__(self, max_num_tasks: int):
        self._semaphore: Final = Semaphore(max_num_tasks)

    def __call__(self, func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @wraps(func)
        async def inner(*args: P.args, **kwds: P.kwargs) -> R:
            async with self._semaphore:
                return await func(*args, **kwds)

        return inner
