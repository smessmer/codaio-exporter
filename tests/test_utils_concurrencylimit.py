from __future__ import annotations

import asyncio

import pytest

from codaio_exporter.utils.concurrencylimit import ConcurrencyLimit


async def test_concurrency_limit_allows_up_to_max() -> None:
    limit = ConcurrencyLimit(2)
    concurrent_count = 0
    max_concurrent = 0
    event = asyncio.Event()

    async def _work() -> None:
        nonlocal concurrent_count, max_concurrent
        concurrent_count += 1
        max_concurrent = max(max_concurrent, concurrent_count)
        await event.wait()
        concurrent_count -= 1

    limited_work = limit(_work)

    tasks = [asyncio.ensure_future(limited_work()) for _ in range(3)]
    await asyncio.sleep(0.05)
    assert max_concurrent == 2

    event.set()
    await asyncio.gather(*tasks)


async def test_concurrency_limit_single() -> None:
    limit = ConcurrencyLimit(1)
    concurrent_count = 0
    max_concurrent = 0
    event = asyncio.Event()

    async def _work() -> None:
        nonlocal concurrent_count, max_concurrent
        concurrent_count += 1
        max_concurrent = max(max_concurrent, concurrent_count)
        await event.wait()
        concurrent_count -= 1

    limited_work = limit(_work)

    tasks = [asyncio.ensure_future(limited_work()) for _ in range(3)]
    await asyncio.sleep(0.05)
    assert max_concurrent == 1

    event.set()
    await asyncio.gather(*tasks)


async def test_concurrency_limit_passes_return_value() -> None:
    limit = ConcurrencyLimit(5)

    async def _work() -> int:
        return 42

    limited_work = limit(_work)
    assert await limited_work() == 42


async def test_concurrency_limit_passes_args() -> None:
    limit = ConcurrencyLimit(5)
    received_args: list[tuple[int, str]] = []

    async def _work(a: int, b: str) -> None:
        received_args.append((a, b))

    limited_work = limit(_work)
    await limited_work(1, b="hello")
    assert received_args == [(1, "hello")]


async def test_concurrency_limit_propagates_exception() -> None:
    limit = ConcurrencyLimit(5)

    async def _work() -> None:
        raise ValueError("boom")

    limited_work = limit(_work)
    with pytest.raises(ValueError, match="boom"):
        await limited_work()
