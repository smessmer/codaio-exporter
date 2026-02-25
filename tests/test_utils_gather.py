from __future__ import annotations

import asyncio

import pytest

from codaio_exporter.utils.gather import gather_cancel_on_first_error, gather_raise_first_error_after_all_tasks_complete

# --- gather_cancel_on_first_error ---


async def test_cancel_on_first_error_all_succeed() -> None:
    async def work(v: int) -> int:
        return v

    results = await gather_cancel_on_first_error(work(1), work(2), work(3))
    assert results == [1, 2, 3]


async def test_cancel_on_first_error_one_fails() -> None:
    cancelled = False

    async def failing() -> int:
        raise ValueError("boom")

    async def slow() -> int:
        nonlocal cancelled
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled = True
            raise
        return 0

    with pytest.raises(ValueError, match="boom"):
        await gather_cancel_on_first_error(failing(), slow())

    await asyncio.sleep(0.05)
    assert cancelled


async def test_cancel_on_first_error_empty() -> None:
    results: list[int] = await gather_cancel_on_first_error()
    assert results == []


# --- gather_raise_first_error_after_all_tasks_complete ---


async def test_raise_first_all_succeed() -> None:
    async def work(v: int) -> int:
        return v

    results = await gather_raise_first_error_after_all_tasks_complete(work(1), work(2), work(3))
    assert results == [1, 2, 3]


async def test_raise_first_one_fails() -> None:
    async def failing() -> int:
        raise ValueError("boom")

    async def ok() -> int:
        return 1

    with pytest.raises(ValueError, match="boom"):
        await gather_raise_first_error_after_all_tasks_complete(failing(), ok())


async def test_raise_first_multiple_fail() -> None:
    async def fail_first() -> int:
        raise ValueError("first")

    async def fail_second() -> int:
        raise RuntimeError("second")

    with pytest.raises(ValueError, match="first"):
        await gather_raise_first_error_after_all_tasks_complete(fail_first(), fail_second())


async def test_raise_first_waits_for_all() -> None:
    slow_completed = False

    async def failing() -> int:
        raise ValueError("boom")

    async def slow() -> int:
        nonlocal slow_completed
        await asyncio.sleep(0.05)
        slow_completed = True
        return 1

    with pytest.raises(ValueError, match="boom"):
        await gather_raise_first_error_after_all_tasks_complete(failing(), slow())

    assert slow_completed


async def test_raise_first_empty() -> None:
    results: list[int] = await gather_raise_first_error_after_all_tasks_complete()
    assert results == []
