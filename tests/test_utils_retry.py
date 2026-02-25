from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from codaio_exporter.utils.retry import retry


@patch("codaio_exporter.utils.retry.asyncio.sleep", new_callable=AsyncMock)
async def test_retry_succeeds_first_try(mock_sleep: AsyncMock) -> None:
    call_count = 0

    @retry(3)
    async def work() -> str:
        nonlocal call_count
        call_count += 1
        return "ok"

    result = await work()
    assert result == "ok"
    assert call_count == 1
    mock_sleep.assert_not_called()


@patch("codaio_exporter.utils.retry.asyncio.sleep", new_callable=AsyncMock)
async def test_retry_succeeds_after_failures(mock_sleep: AsyncMock) -> None:
    call_count = 0

    @retry(3)
    async def work() -> str:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ValueError("fail")
        return "ok"

    result = await work()
    assert result == "ok"
    assert call_count == 3


@patch("codaio_exporter.utils.retry.asyncio.sleep", new_callable=AsyncMock)
async def test_retry_exhausts_retries(mock_sleep: AsyncMock) -> None:
    call_count = 0

    @retry(2)
    async def work() -> str:
        nonlocal call_count
        call_count += 1
        raise ValueError("always fails")

    with pytest.raises(ValueError, match="always fails"):
        await work()
    # 1 initial + 2 retries = 3 total calls
    assert call_count == 3


@patch("codaio_exporter.utils.retry.asyncio.sleep", new_callable=AsyncMock)
async def test_retry_zero_retries(mock_sleep: AsyncMock) -> None:
    call_count = 0

    @retry(0)
    async def work() -> str:
        nonlocal call_count
        call_count += 1
        raise ValueError("fail")

    with pytest.raises(ValueError, match="fail"):
        await work()
    assert call_count == 1


@patch("codaio_exporter.utils.retry.asyncio.sleep", new_callable=AsyncMock)
async def test_retry_passes_through_args(mock_sleep: AsyncMock) -> None:
    @retry(1)
    async def work(a: int, b: str) -> str:
        return f"{a}-{b}"

    result = await work(1, b="hello")
    assert result == "1-hello"


@patch("codaio_exporter.utils.retry.asyncio.sleep", new_callable=AsyncMock)
async def test_retry_returns_value(mock_sleep: AsyncMock) -> None:
    @retry(1)
    async def work() -> int:
        return 42

    assert await work() == 42


@patch("codaio_exporter.utils.retry.asyncio.sleep", new_callable=AsyncMock)
async def test_retry_sleeps_between_retries(mock_sleep: AsyncMock) -> None:
    call_count = 0

    @retry(2)
    async def work() -> str:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ValueError("fail")
        return "ok"

    await work()
    assert mock_sleep.call_count == 2
    for call in mock_sleep.call_args_list:
        assert 5 <= call.args[0] <= 10
