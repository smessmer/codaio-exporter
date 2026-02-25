from __future__ import annotations

from collections.abc import AsyncGenerator, Callable
from typing import TypeVar

T = TypeVar("T")


# Collect an async generator into a list
async def collect(generator: AsyncGenerator[T, None], per_item_callback: Callable[[], None] | None = None) -> list[T]:
    result: list[T] = []
    async for item in generator:
        result.append(item)
        if per_item_callback is not None:
            per_item_callback()
    return result
