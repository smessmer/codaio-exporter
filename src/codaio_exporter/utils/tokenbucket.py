from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Final, final

## Token bucket fill rate on startup (requests per second).
## Set conservatively at the known Coda.io read rate limit (~200 req/min ≈ 3.3 req/s).
_INITIAL_RATE: Final = 3.0

## Absolute floor: never go below 0.1 req/s (1 request per 10 seconds).
_MIN_RATE: Final = 0.1

## Absolute ceiling: never exceed 10 req/s regardless of what the adaptive algorithm computes.
_MAX_RATE: Final = 10.0

## Maximum accumulated tokens. Set to 1.0 to prevent bursting — tokens never
## accumulate above 1, so requests are spaced out evenly at the current rate.
## This is critical: without it, a long idle period would accumulate tokens and
## cause a burst that immediately triggers a 429.
_MAX_BURST: Final = 1.0

## Additive increase: add this many req/s each time a success window completes.
_INCREASE_STEP: Final = 0.1

## Multiplicative decrease: multiply rate by this factor on a 429.
_DECREASE_FACTOR: Final = 0.5

## Stop increasing rate at this fraction of the remembered ceiling.
## E.g., if the ceiling is 3.5 req/s, we stop at 3.5 * 0.9 = 3.15 req/s.
_CEILING_MARGIN: Final = 0.9

## Number of consecutive successful requests before one rate increase step.
_SUCCESS_WINDOW: Final = 20

## Number of consecutive successful requests before the ceiling expires.
## Allows re-probing the true limit after a transient server hiccup that may
## have set the ceiling artificially low.
_CEILING_EXPIRY: Final = 500


@final
class AdaptiveTokenBucket:
    """Proactive rate limiter using AIMD with ceiling memory.

    This class prevents HTTP 429 (Too Many Requests) responses by metering
    outgoing requests to stay below the server's rate limit, rather than
    reactively backing off after hitting it.

    ## Algorithm: AIMD (Additive Increase, Multiplicative Decrease)

    Inspired by TCP congestion control:

    - **Additive Increase:** After every `_SUCCESS_WINDOW` (20) consecutive
      successful requests, increase the rate by `_INCREASE_STEP` (0.1 req/s).
    - **Multiplicative Decrease:** On any 429, immediately halve the rate.

    ## Ceiling Memory (like TCP's ssthresh)

    Plain AIMD would oscillate: increase until hitting the limit, halve, climb
    back up, hit the limit again, repeat — causing periodic 429s. To avoid
    this, we remember the rate at which the last 429 occurred as `_ceiling`.
    Subsequent increases are capped at `_ceiling * _CEILING_MARGIN` (90% of
    the ceiling). After discovering the limit once, we settle just below it
    and stay there.

    The ceiling expires after `_CEILING_EXPIRY` (500) consecutive successes
    to handle the case where a transient server hiccup set the ceiling
    artificially low. After expiry, the rate can probe higher again.

    ## Epoch-Based Deduplication

    With multiple concurrent requests, several may be in-flight when the
    server starts returning 429s. Without dedup, each would call
    `on_rate_limited()`, cascading the rate down (3.0 → 1.5 → 0.75 → ...)
    and corrupting the ceiling with artificially low values.

    The `_epoch` counter solves this: `acquire()` returns the current epoch,
    and `on_rate_limited()` only acts if the epoch hasn't changed since the
    token was issued. The first 429 in a batch does the decrease and
    increments the epoch; all subsequent 429s from the same batch are no-ops.

    ## Usage Pattern

    Used as the innermost decorator on HTTP client methods:

        @_concurrency_limit    # outermost: bounds concurrent tasks
        @retry(10)             # 2nd: retries on any Exception (cancellation propagates)
        @_request_limit        # 3rd: reactive backoff (blocks all on 429)
        @_token_bucket_limit   # innermost: proactive metering

    It must be innermost because `_request_limit` (AdaptiveRateLimit)
    swallows TooManyRequests in its internal while-loop. The token bucket
    needs to see the raw 429 before it gets absorbed.

    ## Threading Model

    `on_success()` and `on_rate_limited()` are synchronous. They are safe
    to call from multiple concurrent coroutines because CPython's asyncio
    event loop is single-threaded: synchronous code between `await` points
    runs atomically. Neither method contains `await`, so each call completes
    without interleaving. This would NOT be safe with multi-threaded executors.
    """

    def __init__(self) -> None:
        self._rate: float = _INITIAL_RATE
        self._tokens: float = 0.0
        self._last_refill: float = time.monotonic()
        self._lock: Final = asyncio.Lock()
        self._consecutive_successes: int = 0
        self._ceiling: float | None = None
        self._epoch: int = 0
        self._successes_since_last_limit: int = 0

    async def acquire(self) -> int:
        """Wait until a token is available, then consume it.

        Returns the current epoch. The caller must pass this epoch to
        on_rate_limited() so that duplicate 429s from the same batch
        can be detected and ignored.
        """
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return self._epoch
                wait_time = (1.0 - self._tokens) / self._rate

            # Add ±10% jitter to spread out requests from concurrent coroutines
            # that are all waiting for tokens simultaneously.
            jitter = wait_time * 0.1 * (random.random() * 2 - 1)
            await asyncio.sleep(wait_time + jitter)

    def on_success(self) -> None:
        """Call after a successful request to potentially increase the rate.

        After _SUCCESS_WINDOW consecutive successes, increases the rate by
        _INCREASE_STEP, but never above the ceiling margin or _MAX_RATE.
        After _CEILING_EXPIRY consecutive successes, expires the ceiling
        to allow re-probing the true server limit.
        """
        self._consecutive_successes += 1
        self._successes_since_last_limit += 1

        # Expire the ceiling after enough consecutive successes, allowing
        # the rate to probe higher in case the ceiling was set by a transient
        # server hiccup rather than the real rate limit.
        if self._ceiling is not None and self._successes_since_last_limit >= _CEILING_EXPIRY:
            logging.debug(f"Token bucket: ceiling expired (was {self._ceiling:.2f} req/s)")
            self._ceiling = None

        if self._consecutive_successes >= _SUCCESS_WINDOW:
            self._consecutive_successes = 0
            max_rate = _MAX_RATE
            if self._ceiling is not None:
                max_rate = min(max_rate, self._ceiling * _CEILING_MARGIN)
            old_rate = self._rate
            self._rate = min(self._rate + _INCREASE_STEP, max_rate)
            if self._rate > old_rate:
                logging.debug(f"Token bucket: rate increased to {self._rate:.2f} req/s")

    def on_rate_limited(self, epoch: int, retry_after: float | None = None) -> None:
        """Call after a 429 to decrease the rate.

        Uses epoch-based deduplication: if epoch != self._epoch, this 429
        is from an in-flight request that was issued before a sibling already
        handled the rate decrease, so we skip it. This prevents cascading
        halvings (3.0 → 1.5 → 0.75 → ...) when multiple concurrent requests
        all get 429'd from the same burst.

        If retry_after is provided (from the Retry-After header), and it
        implies an even lower rate than the multiplicative decrease, the
        implied rate (1/retry_after) is used instead.
        """
        if epoch != self._epoch:
            return

        self._epoch += 1
        self._ceiling = self._rate
        self._consecutive_successes = 0
        self._successes_since_last_limit = 0
        new_rate = self._rate * _DECREASE_FACTOR
        if retry_after is not None and retry_after > 0:
            implied_rate = 1.0 / retry_after
            new_rate = min(new_rate, implied_rate)
        self._rate = max(new_rate, _MIN_RATE)
        self._tokens = 0.0
        logging.debug(f"Token bucket: rate decreased to {self._rate:.2f} req/s (ceiling={self._ceiling:.2f})")

    def _refill(self) -> None:
        """Add tokens based on elapsed time since last refill."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._last_refill = now
        self._tokens = min(self._tokens + elapsed * self._rate, _MAX_BURST)
