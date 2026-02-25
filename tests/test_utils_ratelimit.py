from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from codaio_exporter.utils.ratelimit import AdaptiveRateLimit


class _BackoffError(Exception):
    pass


@patch("codaio_exporter.utils.ratelimit.asyncio.sleep", new_callable=AsyncMock)
@patch("codaio_exporter.utils.ratelimit.time.monotonic")
async def test_ratelimit_normal_passes_through(mock_monotonic: AsyncMock, mock_sleep: AsyncMock) -> None:
    mock_monotonic.return_value = 0.0
    limiter = AdaptiveRateLimit(_BackoffError, 10)

    @limiter
    async def work() -> str:
        return "ok"

    result = await work()
    assert result == "ok"


@patch("codaio_exporter.utils.ratelimit.asyncio.sleep", new_callable=AsyncMock)
@patch("codaio_exporter.utils.ratelimit.time.monotonic")
async def test_ratelimit_returns_value(mock_monotonic: AsyncMock, mock_sleep: AsyncMock) -> None:
    mock_monotonic.return_value = 0.0
    limiter = AdaptiveRateLimit(_BackoffError, 10)

    @limiter
    async def work() -> int:
        return 42

    assert await work() == 42


@patch("codaio_exporter.utils.ratelimit.asyncio.sleep", new_callable=AsyncMock)
@patch("codaio_exporter.utils.ratelimit.time.monotonic")
async def test_ratelimit_passes_args(mock_monotonic: AsyncMock, mock_sleep: AsyncMock) -> None:
    mock_monotonic.return_value = 0.0
    limiter = AdaptiveRateLimit(_BackoffError, 10)

    @limiter
    async def work(a: int, b: str) -> str:
        return f"{a}-{b}"

    result = await work(1, b="hello")
    assert result == "1-hello"


@patch("codaio_exporter.utils.ratelimit.asyncio.sleep", new_callable=AsyncMock)
@patch("codaio_exporter.utils.ratelimit.time.monotonic")
async def test_ratelimit_enters_backoff_then_recovers(mock_monotonic: AsyncMock, mock_sleep: AsyncMock) -> None:
    call_count = 0
    current_time = 0.0

    def get_time() -> float:
        return current_time

    mock_monotonic.side_effect = get_time

    async def advance_time_on_sleep(secs: float) -> None:
        nonlocal current_time
        current_time += secs

    mock_sleep.side_effect = advance_time_on_sleep

    limiter = AdaptiveRateLimit(_BackoffError, 1)

    @limiter
    async def work() -> str:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise _BackoffError("rate limited")
        return "ok"

    result = await work()
    assert result == "ok"
    assert call_count == 2


@patch("codaio_exporter.utils.ratelimit.asyncio.sleep", new_callable=AsyncMock)
@patch("codaio_exporter.utils.ratelimit.time.monotonic")
async def test_ratelimit_stays_in_backoff_on_repeated_failure(mock_monotonic: AsyncMock, mock_sleep: AsyncMock) -> None:
    call_count = 0
    current_time = 0.0

    def get_time() -> float:
        return current_time

    mock_monotonic.side_effect = get_time

    async def advance_time_on_sleep(secs: float) -> None:
        nonlocal current_time
        current_time += secs

    mock_sleep.side_effect = advance_time_on_sleep

    limiter = AdaptiveRateLimit(_BackoffError, 1)

    @limiter
    async def work() -> str:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise _BackoffError("rate limited")
        return "ok"

    result = await work()
    assert result == "ok"
    assert call_count == 3


@patch("codaio_exporter.utils.ratelimit.asyncio.sleep", new_callable=AsyncMock)
@patch("codaio_exporter.utils.ratelimit.time.monotonic")
async def test_ratelimit_non_backoff_exception_propagates(mock_monotonic: AsyncMock, mock_sleep: AsyncMock) -> None:
    mock_monotonic.return_value = 0.0
    limiter = AdaptiveRateLimit(_BackoffError, 10)

    @limiter
    async def work() -> str:
        raise ValueError("unrelated")

    with pytest.raises(ValueError, match="unrelated"):
        await work()
