from __future__ import annotations

from unittest.mock import AsyncMock, patch

from codaio_exporter.utils.tokenbucket import (
    _CEILING_EXPIRY,  # pyright: ignore[reportPrivateUsage]
    _CEILING_MARGIN,  # pyright: ignore[reportPrivateUsage]
    _DECREASE_FACTOR,  # pyright: ignore[reportPrivateUsage]
    _INCREASE_STEP,  # pyright: ignore[reportPrivateUsage]
    _INITIAL_RATE,  # pyright: ignore[reportPrivateUsage]
    _MAX_RATE,  # pyright: ignore[reportPrivateUsage]
    _MIN_RATE,  # pyright: ignore[reportPrivateUsage]
    _SUCCESS_WINDOW,  # pyright: ignore[reportPrivateUsage]
    AdaptiveTokenBucket,
)


def _trigger_increase(bucket: AdaptiveTokenBucket) -> None:
    """Call on_success() enough times to trigger one rate increase."""
    for _ in range(_SUCCESS_WINDOW):
        bucket.on_success()


# --- acquire ---


async def test_acquire_succeeds_immediately_when_tokens_available() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._tokens = 1.0  # pyright: ignore[reportPrivateUsage]
    with patch("codaio_exporter.utils.tokenbucket.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        epoch = await bucket.acquire()
        mock_sleep.assert_not_called()
    assert bucket._tokens == 0.0  # pyright: ignore[reportPrivateUsage]
    assert epoch == 0


async def test_acquire_waits_when_no_tokens() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._tokens = 0.0  # pyright: ignore[reportPrivateUsage]

    call_count = 0

    async def fake_sleep(_seconds: float) -> None:
        nonlocal call_count
        call_count += 1
        bucket._tokens = 1.0  # pyright: ignore[reportPrivateUsage]

    with patch("codaio_exporter.utils.tokenbucket.asyncio.sleep", side_effect=fake_sleep):
        epoch = await bucket.acquire()

    assert call_count == 1
    assert bucket._tokens == 0.0  # pyright: ignore[reportPrivateUsage]
    assert epoch == 0


async def test_acquire_returns_current_epoch() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._tokens = 1.0  # pyright: ignore[reportPrivateUsage]
    bucket._epoch = 5  # pyright: ignore[reportPrivateUsage]
    with patch("codaio_exporter.utils.tokenbucket.asyncio.sleep", new_callable=AsyncMock):
        epoch = await bucket.acquire()
    assert epoch == 5


# --- on_success ---


def test_on_success_increases_rate_after_window() -> None:
    bucket = AdaptiveTokenBucket()
    initial_rate = bucket._rate  # pyright: ignore[reportPrivateUsage]
    _trigger_increase(bucket)
    assert bucket._rate == initial_rate + _INCREASE_STEP  # pyright: ignore[reportPrivateUsage]


def test_on_success_does_not_increase_before_window() -> None:
    bucket = AdaptiveTokenBucket()
    initial_rate = bucket._rate  # pyright: ignore[reportPrivateUsage]
    for _ in range(_SUCCESS_WINDOW - 1):
        bucket.on_success()
    assert bucket._rate == initial_rate  # pyright: ignore[reportPrivateUsage]


def test_on_success_capped_at_max_rate() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._rate = _MAX_RATE  # pyright: ignore[reportPrivateUsage]
    _trigger_increase(bucket)
    assert bucket._rate == _MAX_RATE  # pyright: ignore[reportPrivateUsage]


def test_on_success_capped_at_ceiling() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._ceiling = 4.0  # pyright: ignore[reportPrivateUsage]
    bucket._rate = 4.0 * _CEILING_MARGIN - _INCREASE_STEP / 2  # pyright: ignore[reportPrivateUsage]
    _trigger_increase(bucket)
    assert bucket._rate == 4.0 * _CEILING_MARGIN  # pyright: ignore[reportPrivateUsage]


def test_on_success_does_not_increase_past_ceiling_margin() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._ceiling = 4.0  # pyright: ignore[reportPrivateUsage]
    bucket._rate = 4.0 * _CEILING_MARGIN  # pyright: ignore[reportPrivateUsage]
    _trigger_increase(bucket)
    assert bucket._rate == 4.0 * _CEILING_MARGIN  # pyright: ignore[reportPrivateUsage]


# --- on_rate_limited ---


def test_on_rate_limited_halves_rate() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._rate = 4.0  # pyright: ignore[reportPrivateUsage]
    bucket.on_rate_limited(bucket._epoch)  # pyright: ignore[reportPrivateUsage]
    assert bucket._rate == 4.0 * _DECREASE_FACTOR  # pyright: ignore[reportPrivateUsage]


def test_on_rate_limited_respects_min_rate() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._rate = _MIN_RATE * 1.5  # pyright: ignore[reportPrivateUsage]
    bucket.on_rate_limited(bucket._epoch)  # pyright: ignore[reportPrivateUsage]
    # 0.15 * 0.5 = 0.075, should be clamped to _MIN_RATE
    assert bucket._rate == _MIN_RATE  # pyright: ignore[reportPrivateUsage]


def test_on_rate_limited_with_retry_after_uses_implied_rate_if_lower() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._rate = 4.0  # pyright: ignore[reportPrivateUsage]
    # retry_after=10 implies 0.1 req/s, which is lower than 4.0 * 0.5 = 2.0
    bucket.on_rate_limited(bucket._epoch, retry_after=10.0)  # pyright: ignore[reportPrivateUsage]
    assert bucket._rate == _MIN_RATE  # pyright: ignore[reportPrivateUsage]


def test_on_rate_limited_with_retry_after_uses_halved_if_lower() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._rate = 4.0  # pyright: ignore[reportPrivateUsage]
    # retry_after=0.1 implies 10 req/s, which is higher than 4.0 * 0.5 = 2.0
    bucket.on_rate_limited(bucket._epoch, retry_after=0.1)  # pyright: ignore[reportPrivateUsage]
    assert bucket._rate == 4.0 * _DECREASE_FACTOR  # pyright: ignore[reportPrivateUsage]


def test_on_rate_limited_drains_tokens() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._tokens = 0.8  # pyright: ignore[reportPrivateUsage]
    bucket.on_rate_limited(bucket._epoch)  # pyright: ignore[reportPrivateUsage]
    assert bucket._tokens == 0.0  # pyright: ignore[reportPrivateUsage]


def test_on_rate_limited_resets_consecutive_successes() -> None:
    bucket = AdaptiveTokenBucket()
    for _ in range(_SUCCESS_WINDOW - 1):
        bucket.on_success()
    bucket.on_rate_limited(bucket._epoch)  # pyright: ignore[reportPrivateUsage]
    # Now _SUCCESS_WINDOW - 1 more successes should NOT trigger an increase
    rate_after_limit = bucket._rate  # pyright: ignore[reportPrivateUsage]
    for _ in range(_SUCCESS_WINDOW - 1):
        bucket.on_success()
    assert bucket._rate == rate_after_limit  # pyright: ignore[reportPrivateUsage]


def test_on_rate_limited_sets_ceiling() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._rate = 3.5  # pyright: ignore[reportPrivateUsage]
    bucket.on_rate_limited(bucket._epoch)  # pyright: ignore[reportPrivateUsage]
    assert bucket._ceiling == 3.5  # pyright: ignore[reportPrivateUsage]


def test_ceiling_limits_subsequent_increases() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._rate = 3.5  # pyright: ignore[reportPrivateUsage]
    bucket.on_rate_limited(bucket._epoch)  # pyright: ignore[reportPrivateUsage]
    # rate is now 1.75, ceiling is 3.5
    max_allowed = 3.5 * _CEILING_MARGIN
    # Use fewer iterations than would trigger ceiling expiry.
    # Each _trigger_increase does _SUCCESS_WINDOW on_success calls,
    # so stay well under _CEILING_EXPIRY total.
    num_iterations = (_CEILING_EXPIRY // _SUCCESS_WINDOW) - 1
    for _ in range(num_iterations):
        _trigger_increase(bucket)
        assert bucket._rate <= max_allowed + 1e-9  # pyright: ignore[reportPrivateUsage]


def test_ceiling_updated_on_new_rate_limit() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._rate = 3.5  # pyright: ignore[reportPrivateUsage]
    bucket.on_rate_limited(bucket._epoch)  # pyright: ignore[reportPrivateUsage]
    assert bucket._ceiling == 3.5  # pyright: ignore[reportPrivateUsage]
    # Rate is now 1.75, hit another 429
    bucket.on_rate_limited(bucket._epoch)  # pyright: ignore[reportPrivateUsage]
    assert bucket._ceiling == 1.75  # pyright: ignore[reportPrivateUsage]


# --- epoch dedup ---


def test_on_rate_limited_with_stale_epoch_is_noop() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._rate = 4.0  # pyright: ignore[reportPrivateUsage]
    stale_epoch = bucket._epoch - 1  # pyright: ignore[reportPrivateUsage]
    bucket.on_rate_limited(stale_epoch)
    # Rate should be unchanged — stale epoch means this 429 was already handled
    assert bucket._rate == 4.0  # pyright: ignore[reportPrivateUsage]
    assert bucket._ceiling is None  # pyright: ignore[reportPrivateUsage]


def test_on_rate_limited_increments_epoch() -> None:
    bucket = AdaptiveTokenBucket()
    initial_epoch = bucket._epoch  # pyright: ignore[reportPrivateUsage]
    bucket.on_rate_limited(initial_epoch)
    assert bucket._epoch == initial_epoch + 1  # pyright: ignore[reportPrivateUsage]


def test_multiple_429s_same_epoch_only_decrease_once() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._rate = 4.0  # pyright: ignore[reportPrivateUsage]
    epoch = bucket._epoch  # pyright: ignore[reportPrivateUsage]
    # Simulate 5 in-flight requests all getting 429'd with the same epoch
    bucket.on_rate_limited(epoch)
    bucket.on_rate_limited(epoch)
    bucket.on_rate_limited(epoch)
    bucket.on_rate_limited(epoch)
    bucket.on_rate_limited(epoch)
    # Rate should only be halved once, not 5 times
    assert bucket._rate == 4.0 * _DECREASE_FACTOR  # pyright: ignore[reportPrivateUsage]
    assert bucket._ceiling == 4.0  # pyright: ignore[reportPrivateUsage]


# --- ceiling expiry ---


def test_ceiling_expires_after_enough_successes() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._rate = 3.5  # pyright: ignore[reportPrivateUsage]
    bucket.on_rate_limited(bucket._epoch)  # pyright: ignore[reportPrivateUsage]
    assert bucket._ceiling == 3.5  # pyright: ignore[reportPrivateUsage]

    # Simulate _CEILING_EXPIRY consecutive successes
    for _ in range(_CEILING_EXPIRY):
        bucket.on_success()

    assert bucket._ceiling is None  # pyright: ignore[reportPrivateUsage]


def test_ceiling_expiry_counter_resets_on_rate_limit() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._rate = 3.5  # pyright: ignore[reportPrivateUsage]
    bucket.on_rate_limited(bucket._epoch)  # pyright: ignore[reportPrivateUsage]

    # Accumulate some successes, but not enough for expiry
    for _ in range(_CEILING_EXPIRY - 1):
        bucket.on_success()
    assert bucket._ceiling is not None  # pyright: ignore[reportPrivateUsage]

    # Hit another rate limit — counter should reset
    bucket.on_rate_limited(bucket._epoch)  # pyright: ignore[reportPrivateUsage]

    # Now _CEILING_EXPIRY - 1 successes should NOT expire the ceiling
    for _ in range(_CEILING_EXPIRY - 1):
        bucket.on_success()
    assert bucket._ceiling is not None  # pyright: ignore[reportPrivateUsage]


def test_rate_can_increase_past_old_ceiling_after_expiry() -> None:
    bucket = AdaptiveTokenBucket()
    bucket._rate = 2.0  # pyright: ignore[reportPrivateUsage]
    bucket.on_rate_limited(bucket._epoch)  # pyright: ignore[reportPrivateUsage]
    # ceiling = 2.0, rate = 1.0
    ceiling_cap = 2.0 * _CEILING_MARGIN

    # Expire the ceiling
    for _ in range(_CEILING_EXPIRY):
        bucket.on_success()
    assert bucket._ceiling is None  # pyright: ignore[reportPrivateUsage]

    # Now we should be able to increase past the old ceiling margin
    while bucket._rate <= ceiling_cap:  # pyright: ignore[reportPrivateUsage]
        _trigger_increase(bucket)
    assert bucket._rate > ceiling_cap  # pyright: ignore[reportPrivateUsage]


# --- initial state ---


def test_initial_rate() -> None:
    bucket = AdaptiveTokenBucket()
    assert bucket._rate == _INITIAL_RATE  # pyright: ignore[reportPrivateUsage]


def test_initial_ceiling_is_none() -> None:
    bucket = AdaptiveTokenBucket()
    assert bucket._ceiling is None  # pyright: ignore[reportPrivateUsage]


def test_initial_epoch_is_zero() -> None:
    bucket = AdaptiveTokenBucket()
    assert bucket._epoch == 0  # pyright: ignore[reportPrivateUsage]
