import asyncio
from typing import AsyncGenerator, List, TypeVar, Callable, Coroutine, Any, Optional

T = TypeVar('T')

# Collect an async generator into a list
async def collect(generator: AsyncGenerator[T, None], per_item_callback: Optional[Callable[[], None]] = None) -> List[T]:
    result = []
    async for item in generator:
        result.append(item)
        if per_item_callback is not None:
            per_item_callback()
    return result

# `async for` isn't concurrent and does one iteration strictly after the other. This function allows us
# to process multiple loop iterations concurrently.
async def concurrent_async_for(generator: AsyncGenerator[T, None], loop_body: Callable[[T], Coroutine[Any, Any, None]]) -> None:
    tasks = []
    async for item in generator:
        tasks.append(asyncio.create_task(loop_body(item)))
    await asyncio.gather(*tasks)
