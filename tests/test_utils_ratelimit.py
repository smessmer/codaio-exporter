from __future__ import annotations

import asyncio
from typing import Any, Final, final
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from codaio_exporter.utils.ratelimit import AdaptiveRateLimit


class _BackoffError(Exception):
    pass


# Patching codaio_exporter.utils.ratelimit.asyncio.sleep patches asyncio.sleep itself, so keep the original
_real_sleep = asyncio.sleep


@final
class _FakeTime:
    """Simulated time for AdaptiveRateLimit, through its patched time.monotonic() and asyncio.sleep().

    Sleeping advances the time instead of waiting, but still yields to the event loop once, so that other tasks can run
    and the sleeping task can be cancelled during the sleep. The rate limiter waits only by sleeping, so to fail instead
    of hang when a request waits forever, sleeping raises once the time is past 1000 s or after more than 1000 sleeps.
    """

    def __init__(self, mock_monotonic: MagicMock, mock_sleep: AsyncMock) -> None:
        super().__init__()
        self._now = 0.0
        self._sleeps: Final[list[tuple[asyncio.Task[Any] | None, float]]] = []
        mock_monotonic.side_effect = lambda: self._now
        mock_sleep.side_effect = self._sleep

    def sleeps_of(self, task: asyncio.Task[Any] | None) -> list[float]:
        return [secs for sleeping_task, secs in self._sleeps if sleeping_task is task]

    async def _sleep(self, secs: float) -> None:
        self._sleeps.append((asyncio.current_task(), secs))
        self._now += max(secs, 0.0)  # Like asyncio.sleep(), which doesn't wait for durations <= 0
        if self._now > 1000 or len(self._sleeps) > 1000:
            raise AssertionError(f"Still waiting after {self._now:.0f} s and {len(self._sleeps)} sleeps. The rate limiter is stuck.")
        await _real_sleep(0)


async def _wait_for_event(event: asyncio.Event, *tasks: asyncio.Task[Any]) -> None:
    """Like event.wait(), but fails instead of hanging if one of the tasks finishes before the event is set.

    This continues in the same event loop iteration as event.wait() would, so that tests can act on a task right after
    it set the event, e.g. while it sleeps.
    """

    def wake_up(_: asyncio.Task[Any]) -> None:
        event.set()

    for task in tasks:
        task.add_done_callback(wake_up)
    try:
        await event.wait()
    finally:
        for task in tasks:
            task.remove_done_callback(wake_up)
    for task in tasks:
        if task.done() and not task.cancelled():
            task.result()  # Raises the exception the task failed with
        assert not task.done(), f"{task!r} has already finished"


async def _let_other_tasks_run() -> None:
    """Runs the event loop for 10 iterations.

    With _FakeTime, a request that waits in the rate limiter checks the limiter's state once per iteration.
    """
    for _ in range(10):
        await _real_sleep(0)


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


# The "recovery request" is the request that tries whether the rate limit is over, once the backoff interval has passed.
# All other requests wait until it has finished.


@patch("codaio_exporter.utils.ratelimit.asyncio.sleep", new_callable=AsyncMock)
@patch("codaio_exporter.utils.ratelimit.time.monotonic")
async def test_ratelimit_cancelling_recovery_request_propagates_immediately(mock_monotonic: MagicMock, mock_sleep: AsyncMock) -> None:
    # e.g. on Ctrl+C. This must not be delayed by the sleeps that follow an unrelated error.
    fake_time = _FakeTime(mock_monotonic, mock_sleep)
    limiter = AdaptiveRateLimit(_BackoffError, 1)
    call_count = 0
    recovery_started = asyncio.Event()

    @limiter
    async def work() -> str:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise _BackoffError("rate limited")
        if call_count == 2:
            recovery_started.set()
            await asyncio.Event().wait()  # Blocks until the task gets cancelled
        return "ok"

    task = asyncio.ensure_future(work())
    await _wait_for_event(recovery_started, task)
    sleeps_before_cancel = fake_time.sleeps_of(task)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert fake_time.sleeps_of(task) == sleeps_before_cancel
    # The next request takes over the recovery
    assert await work() == "ok"
    assert call_count == 3


@pytest.mark.parametrize("exception_type", [KeyboardInterrupt, SystemExit, GeneratorExit])
@patch("codaio_exporter.utils.ratelimit.asyncio.sleep", new_callable=AsyncMock)
@patch("codaio_exporter.utils.ratelimit.time.monotonic")
async def test_ratelimit_base_exception_in_recovery_request_propagates_immediately(
    mock_monotonic: MagicMock, mock_sleep: AsyncMock, exception_type: type[BaseException]
) -> None:
    # Like cancellation, interrupting or exiting the program and closing the coroutine must not be delayed either
    fake_time = _FakeTime(mock_monotonic, mock_sleep)
    limiter = AdaptiveRateLimit(_BackoffError, 1)
    call_count = 0
    sleeps_when_raised: list[float] = []

    @limiter
    async def work() -> str:
        nonlocal call_count, sleeps_when_raised
        call_count += 1
        if call_count == 1:
            raise _BackoffError("rate limited")
        if call_count == 2:
            sleeps_when_raised = fake_time.sleeps_of(asyncio.current_task())
            raise exception_type()
        return "ok"

    with pytest.raises(exception_type):
        await work()
    assert fake_time.sleeps_of(asyncio.current_task()) == sleeps_when_raised
    # The next request takes over the recovery
    assert await work() == "ok"
    assert call_count == 3


@patch("codaio_exporter.utils.ratelimit.asyncio.sleep", new_callable=AsyncMock)
@patch("codaio_exporter.utils.ratelimit.time.monotonic")
async def test_ratelimit_waiting_request_takes_over_recovery_after_recovery_request_is_cancelled_twice(
    mock_monotonic: MagicMock, mock_sleep: AsyncMock
) -> None:
    # A task can get cancelled again while it handles its cancellation, e.g. on Ctrl+C during a reimport, first through the
    # cancelled asyncio.gather() in gather_cancel_on_first_error() and then by gather_cancel_on_first_error() itself.
    _FakeTime(mock_monotonic, mock_sleep)
    limiter = AdaptiveRateLimit(_BackoffError, 1)
    call_count = 0
    recovery_started = asyncio.Event()

    @limiter
    async def work() -> str:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise _BackoffError("rate limited")
        if call_count == 2:
            recovery_started.set()
            await asyncio.Event().wait()  # Blocks until the task gets cancelled
        return "ok"

    recovery_task = asyncio.ensure_future(work())
    await _wait_for_event(recovery_started, recovery_task)
    waiting_task = asyncio.ensure_future(work())
    await _real_sleep(0)  # Let it start waiting for the recovery
    recovery_task.cancel()
    await _real_sleep(0)  # Let the cancellation arrive
    recovery_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await recovery_task
    assert await waiting_task == "ok"
    assert call_count == 3


@patch("codaio_exporter.utils.ratelimit.asyncio.sleep", new_callable=AsyncMock)
@patch("codaio_exporter.utils.ratelimit.time.monotonic")
async def test_ratelimit_cancelling_recovery_request_while_it_handles_unrelated_error_does_not_block_other_requests(
    mock_monotonic: MagicMock, mock_sleep: AsyncMock
) -> None:
    fake_time = _FakeTime(mock_monotonic, mock_sleep)
    limiter = AdaptiveRateLimit(_BackoffError, 1)
    call_count = 0
    recovery_failed = asyncio.Event()

    @limiter
    async def work() -> str:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise _BackoffError("rate limited")
        if call_count == 2:
            recovery_failed.set()
            raise ValueError("unrelated")
        return "ok"

    task = asyncio.ensure_future(work())
    await _wait_for_event(recovery_failed, task)
    # The request now sleeps 1 s before it hands the recovery over to another request
    assert fake_time.sleeps_of(task)[-1] == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # The next request takes over the recovery
    assert await work() == "ok"
    assert call_count == 3


@patch("codaio_exporter.utils.ratelimit.asyncio.sleep", new_callable=AsyncMock)
@patch("codaio_exporter.utils.ratelimit.time.monotonic")
async def test_ratelimit_unrelated_error_in_recovery_request_hands_over_recovery_between_sleeps(
    mock_monotonic: MagicMock, mock_sleep: AsyncMock
) -> None:
    # The failed request sleeps 1 s, hands the recovery over to a waiting request, sleeps 10 s more and then re-raises the error
    fake_time = _FakeTime(mock_monotonic, mock_sleep)
    limiter = AdaptiveRateLimit(_BackoffError, 1)
    call_count = 0
    recovery_started = asyncio.Event()
    fail_recovery = asyncio.Event()
    sleeps_of_recovery_task_at_handover: list[float] = []
    recovery_task_done_at_handover = True

    @limiter
    async def work() -> str:
        nonlocal call_count, sleeps_of_recovery_task_at_handover, recovery_task_done_at_handover
        call_count += 1
        if call_count == 1:
            raise _BackoffError("rate limited")
        if call_count == 2:
            recovery_started.set()
            await fail_recovery.wait()
            raise ValueError("unrelated")
        sleeps_of_recovery_task_at_handover = fake_time.sleeps_of(recovery_task)
        recovery_task_done_at_handover = recovery_task.done()
        return "ok"

    recovery_task = asyncio.ensure_future(work())
    await _wait_for_event(recovery_started, recovery_task)
    waiting_task = asyncio.ensure_future(work())
    await _real_sleep(0)  # Let it start waiting for the recovery
    fail_recovery.set()
    with pytest.raises(ValueError, match="unrelated"):
        await recovery_task
    assert await waiting_task == "ok"
    assert call_count == 3
    assert fake_time.sleeps_of(recovery_task)[-2:] == [1, 10]
    assert sleeps_of_recovery_task_at_handover[-2:] == [1, 10]
    assert not recovery_task_done_at_handover


@pytest.mark.parametrize("exception_type", [asyncio.CancelledError, ValueError])
@patch("codaio_exporter.utils.ratelimit.asyncio.sleep", new_callable=AsyncMock)
@patch("codaio_exporter.utils.ratelimit.time.monotonic")
async def test_ratelimit_only_one_waiting_request_takes_over_recovery(
    mock_monotonic: MagicMock, mock_sleep: AsyncMock, exception_type: type[BaseException]
) -> None:
    # When the recovery request gets cancelled or fails with an unrelated error, one of the waiting requests becomes the
    # next recovery request, and the others keep waiting until it has finished
    _FakeTime(mock_monotonic, mock_sleep)
    limiter = AdaptiveRateLimit(_BackoffError, 1)
    call_count = 0
    recovery_started = asyncio.Event()
    fail_recovery = asyncio.Event()
    calls_in_progress = 0
    max_calls_in_progress = 0

    @limiter
    async def work() -> str:
        nonlocal call_count, calls_in_progress, max_calls_in_progress
        call_count += 1
        if call_count == 1:
            raise _BackoffError("rate limited")
        if call_count == 2:
            recovery_started.set()
            await fail_recovery.wait()  # Blocks until the test lets it fail or cancels it
            raise ValueError("unrelated")
        calls_in_progress += 1
        max_calls_in_progress = max(max_calls_in_progress, calls_in_progress)
        await _let_other_tasks_run()  # Gives the other waiting request the chance to run at the same time
        calls_in_progress -= 1
        return "ok"

    recovery_task = asyncio.ensure_future(work())
    await _wait_for_event(recovery_started, recovery_task)
    waiting_tasks = [asyncio.ensure_future(work()) for _ in range(2)]
    await _real_sleep(0)  # Let them start waiting for the recovery
    if exception_type is asyncio.CancelledError:
        recovery_task.cancel()
    else:
        fail_recovery.set()
    assert await asyncio.gather(*waiting_tasks) == ["ok", "ok"]
    assert call_count == 4
    assert max_calls_in_progress == 1
    with pytest.raises(exception_type):
        await recovery_task


@pytest.mark.parametrize("exception_type", [asyncio.CancelledError, ValueError])
@patch("codaio_exporter.utils.ratelimit.asyncio.sleep", new_callable=AsyncMock)
@patch("codaio_exporter.utils.ratelimit.time.monotonic")
async def test_ratelimit_other_request_ending_during_recovery_does_not_start_another_recovery_request(
    mock_monotonic: MagicMock, mock_sleep: AsyncMock, exception_type: type[BaseException]
) -> None:
    # A request that was already running when the rate limit was hit isn't the recovery request. When it gets cancelled
    # or fails while the recovery request is running, the waiting requests keep waiting for the recovery request.
    _FakeTime(mock_monotonic, mock_sleep)
    limiter = AdaptiveRateLimit(_BackoffError, 1)
    call_count = 0
    fail_other_request = asyncio.Event()
    recovery_started = asyncio.Event()
    finish_recovery = asyncio.Event()

    @limiter
    async def work() -> str:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            await fail_other_request.wait()  # Blocks until the test lets it fail or cancels it
            raise ValueError("unrelated")
        if call_count == 2:
            raise _BackoffError("rate limited")
        if call_count == 3:
            recovery_started.set()
            await finish_recovery.wait()
        return "ok"

    other_task = asyncio.ensure_future(work())
    await _real_sleep(0)  # Let it start its call
    recovery_task = asyncio.ensure_future(work())
    await _wait_for_event(recovery_started, recovery_task, other_task)
    waiting_task = asyncio.ensure_future(work())
    await _real_sleep(0)  # Let it start waiting for the recovery
    if exception_type is asyncio.CancelledError:
        other_task.cancel()
    else:
        fail_other_request.set()
    await _let_other_tasks_run()  # Gives the waiting request the chance to start another recovery request
    assert other_task.done()
    assert call_count == 3
    finish_recovery.set()
    assert await recovery_task == "ok"
    assert await waiting_task == "ok"
    assert call_count == 4
    with pytest.raises(exception_type):
        await other_task


@patch("codaio_exporter.utils.ratelimit.asyncio.sleep", new_callable=AsyncMock)
@patch("codaio_exporter.utils.ratelimit.time.monotonic")
async def test_ratelimit_cancelling_request_without_rate_limit_does_not_delay_other_requests(
    mock_monotonic: MagicMock, mock_sleep: AsyncMock
) -> None:
    # Without a rate limit, there's no recovery request, and cancelling a request must not start a recovery either
    fake_time = _FakeTime(mock_monotonic, mock_sleep)
    limiter = AdaptiveRateLimit(_BackoffError, 1)
    call_count = 0
    calls_in_progress = 0
    max_calls_in_progress = 0

    @limiter
    async def work() -> str:
        nonlocal call_count, calls_in_progress, max_calls_in_progress
        call_count += 1
        if call_count == 1:
            await asyncio.Event().wait()  # Blocks until the task gets cancelled
        calls_in_progress += 1
        max_calls_in_progress = max(max_calls_in_progress, calls_in_progress)
        await _let_other_tasks_run()  # Gives the other request the chance to run at the same time
        calls_in_progress -= 1
        return "ok"

    cancelled_task = asyncio.ensure_future(work())
    await _real_sleep(0)  # Let it start its call
    cancelled_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_task
    tasks = [asyncio.ensure_future(work()) for _ in range(2)]
    assert await asyncio.gather(*tasks) == ["ok", "ok"]
    assert call_count == 3
    # Both ran at the same time, without waiting
    assert max_calls_in_progress == 2
    assert fake_time.sleeps_of(tasks[0]) == fake_time.sleeps_of(tasks[1]) == []
