from __future__ import annotations

import asyncio
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


@patch("codaio_exporter.utils.retry.asyncio.sleep", new_callable=AsyncMock)
async def test_retry_retries_timeout_error(mock_sleep: AsyncMock) -> None:
    # TimeoutError (e.g. aiohttp timeouts) is an Exception and is still retried
    call_count = 0

    @retry(2)
    async def work() -> str:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise TimeoutError("timed out")
        return "ok"

    assert await work() == "ok"
    assert call_count == 3
    assert mock_sleep.call_count == 2


@pytest.mark.parametrize("exception_type", [asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit])
@patch("codaio_exporter.utils.retry.asyncio.sleep", new_callable=AsyncMock)
async def test_retry_does_not_retry_base_exceptions(
    mock_sleep: AsyncMock, exception_type: type[BaseException], caplog: pytest.LogCaptureFixture
) -> None:
    # Cancellation, interpreter exit and the GeneratorExit thrown into a closed coroutine must propagate immediately,
    # without being logged and retried
    call_count = 0

    @retry(3)
    async def work() -> str:
        nonlocal call_count
        call_count += 1
        raise exception_type()

    with pytest.raises(exception_type):
        await work()
    assert call_count == 1
    mock_sleep.assert_not_called()
    assert "Retrying" not in caplog.text


@patch("codaio_exporter.utils.retry.asyncio.sleep", new_callable=AsyncMock)
async def test_retry_cancelling_task_during_call_cancels_task(mock_sleep: AsyncMock, caplog: pytest.LogCaptureFixture) -> None:
    call_count = 0
    started = asyncio.Event()

    @retry(3)
    async def work() -> str:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            started.set()
            await asyncio.Event().wait()  # Blocks until the task gets cancelled
        return "retried"

    task = asyncio.ensure_future(work())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()
    assert call_count == 1
    mock_sleep.assert_not_called()
    assert "Retrying" not in caplog.text


@patch("codaio_exporter.utils.retry.asyncio.sleep", new_callable=AsyncMock)
async def test_retry_cancelling_task_during_backoff_sleep_cancels_task(mock_sleep: AsyncMock) -> None:
    call_count = 0
    sleeping = asyncio.Event()

    async def blocking_sleep(delay: float) -> None:
        sleeping.set()
        await asyncio.Event().wait()  # Blocks until the task gets cancelled

    mock_sleep.side_effect = blocking_sleep

    @retry(3)
    async def work() -> str:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ValueError("fail")
        return "retried"

    task = asyncio.ensure_future(work())
    await sleeping.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()
    assert call_count == 1
    mock_sleep.assert_called_once()
