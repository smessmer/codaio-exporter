from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from enum import Enum
from functools import wraps
from math import ceil
from typing import Final, ParamSpec, TypeVar, final


@final
class _State(Enum):
    ## Everything is normal, all requests go through
    normal = "normal"

    ## We've encountered a rate limit exception and are currently backing off.
    ## Any new requests will wait.
    backoff = "backoff"

    ## We're at the end of the wait window of a backoff but not going back to
    ## normal operations just yet. We've sent one request and are waiting for
    ## that request to succeed or fail to decide whether we go back to normal
    ## or back to backoff.
    recover = "recover"


P = ParamSpec("P")
R = TypeVar("R")


## Allows concurrent calls to an async function, but if a predefined exception happens (e.g. TOO_MANY_REQUESTS),
## all calls will be blocked for a defined backoff interval.
@final
class AdaptiveRateLimit:
    def __init__(self, backoff_exception: type[BaseException], backoff_interval_sec: int):
        self._state = _State.normal
        self._backoff_until = 0
        self._backoff_interval_sec: Final = backoff_interval_sec
        self._backoff_exception: Final = backoff_exception
        self._request_counter = 0

    def __call__(self, func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @wraps(func)
        async def inner(*args: P.args, **kwds: P.kwargs) -> R:
            request_index = self._request_counter
            self._request_counter += 1

            while True:
                if self._state == _State.recover:
                    # Another request is currently running to decide for whether we go to normal or back to backoff, let's pause ourselves
                    await asyncio.sleep(1 + random.random())
                    continue

                # Remember whether we are in backoff because other requests can change it while we're running
                is_backoff = self._state == _State.backoff

                if is_backoff:
                    await self._wait_backoff()

                    if self._state == _State.recover:  # pyright: ignore[reportUnnecessaryComparison]  # state changes concurrently
                        # While we were in backoff, another request went into backoff, then into recover.
                        # Since we're waiting for that recover thread, we should pause ourselves.
                        continue
                    elif self._state == _State.backoff:
                        # We're the first request to make it out of backoff.
                        self._state = _State.recover
                    elif self._state == _State.normal:
                        # Some other thread already went through _State.recover and put us back to _State.normal
                        # while we were in backoff.
                        continue

                # If we're here, then either
                # * is_backoff == False => We're in _State.normal
                # * is_backoff == True  => We were in _State.backoff but we're the first request to make it out of _State.backoff.
                #                          We're the request that put us into _State.recover and that will decide whether we go
                #                          back to _State.backoff or _State.normal.
                if is_backoff:
                    assert self._state == _State.recover
                else:
                    assert self._state == _State.normal

                try:
                    if is_backoff:
                        logging.debug(f"Request {request_index}: Attempting another request after backoff")
                    logging.debug(f"Request {request_index}: Running request")
                    result = await func(*args, **kwds)
                    if is_backoff:
                        # Only change state to _State.normal if we're actually the one request running in _State.recover mode.
                        # Otherwise, race conditions could cause some requests to set _State.backoff and another request to immediately
                        # reset it without any actual backoff period.
                        self._state = _State.normal
                        logging.debug(f"Request {request_index}: Attempting another request after backoff...succeeded. Backoff ended.")
                    return result
                except self._backoff_exception as e:
                    if is_backoff:
                        logging.debug(
                            f"Request {request_index}: Attempting another request after backoff...still hitting rate limit. Backing off again."
                        )
                        self._state = _State.backoff
                    elif self._state == _State.normal:
                        logging.debug(f"Request {request_index}: Rate limit exception detected. Backing off.")
                        self._state = _State.backoff
                    elif self._state == _State.recover:
                        # Another task (not ourselves) is currently trying to recover. Ignore this failure.
                        continue
                    else:
                        # Another concurrent task already put us into backoff while we were running.
                        # Don't do anything special, just increase self._backoff_until accordingly
                        assert self._state == _State.backoff
                    # Use Retry-After from the exception if available, otherwise use the default interval.
                    retry_after = getattr(e, "retry_after", None)
                    backoff_secs: int | float = self._backoff_interval_sec
                    if isinstance(retry_after, int | float) and retry_after > 0:
                        backoff_secs = max(float(retry_after), 1.0)
                    self._backoff_until = ceil(time.monotonic() + backoff_secs)
                except Exception:
                    # An unrelated error happened
                    if is_backoff:
                        # But we're the task responsible for recovering from _State.recovery
                        logging.debug(
                            f"Request {request_index}: Attempting another request after backoff...failed with error unrelated to rate limit. Waking a different request."
                        )
                        try:
                            # Sleep for a bit in case the server has temporary issues
                            await asyncio.sleep(1)
                        finally:
                            # Setting the state back to _State.backoff but without a timeout.
                            # This will cause one (and only one) of the backoff threads to wake up
                            # and go into _State.recover.
                            # This also needs to happen if the sleep is interrupted (e.g. the task gets cancelled),
                            # otherwise we'd stay in _State.recover forever and all other requests would wait forever.
                            self._state = _State.backoff
                        # Sleep a bit more to make sure it's not ourselves but a different task that gets woken up
                        await asyncio.sleep(10)
                    raise
                except BaseException:
                    # The request was cancelled (asyncio.CancelledError) or the program is being interrupted (e.g. KeyboardInterrupt).
                    # Don't delay that with the sleeps above, but if we're the task responsible for recovering from _State.recover,
                    # hand that over to a different request right away.
                    if is_backoff:
                        logging.debug(f"Request {request_index}: Attempting another request after backoff...interrupted. Waking a different request.")
                        self._state = _State.backoff
                    raise

        return inner

    async def _wait_backoff(self) -> None:
        wait_secs = ceil(self._backoff_until - time.monotonic())
        while wait_secs > 0:
            # Add 0-25% jitter to spread out requests resuming after backoff.
            jitter = random.random() * 0.25 * wait_secs
            await asyncio.sleep(wait_secs + jitter)
            wait_secs = ceil(self._backoff_until - time.monotonic())
