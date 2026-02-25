from __future__ import annotations

from codaio_exporter.utils.generator import collect

from .conftest import async_generator_from_list


async def test_collect_basic() -> None:
    result = await collect(async_generator_from_list([1, 2, 3]))
    assert result == [1, 2, 3]


async def test_collect_empty() -> None:
    result = await collect(async_generator_from_list([]))
    assert result == []


async def test_collect_with_callback() -> None:
    call_count = 0

    def callback() -> None:
        nonlocal call_count
        call_count += 1

    result = await collect(async_generator_from_list([10, 20, 30]), per_item_callback=callback)
    assert result == [10, 20, 30]
    assert call_count == 3


async def test_collect_without_callback() -> None:
    result = await collect(async_generator_from_list(["a", "b"]), per_item_callback=None)
    assert result == ["a", "b"]
