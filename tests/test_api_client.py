from __future__ import annotations

import asyncio
import itertools
import threading
import time
from collections.abc import AsyncGenerator, Awaitable, Iterable, Iterator, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, Final, TypeVar, cast, final
from unittest.mock import AsyncMock, MagicMock, PropertyMock

import aiohttp
import pytest

from codaio_exporter.api.client import (
    _API_ENDPOINT,  # pyright: ignore[reportPrivateUsage]
    Client,
    CodaError,
    ContentTypeError,
    NotFound,
    RequestId,
    ResponseFormatError,
    StatusCodeError,
    TooManyRequests,
    _concurrency_limit,  # pyright: ignore[reportPrivateUsage]
    _handle_mutation_response,  # pyright: ignore[reportPrivateUsage]
    _handle_potential_error,  # pyright: ignore[reportPrivateUsage]
    _request_limit,  # pyright: ignore[reportPrivateUsage]
    _token_bucket,  # pyright: ignore[reportPrivateUsage]
)
from codaio_exporter.utils.ratelimit import _State  # pyright: ignore[reportPrivateUsage]


def _make_mock_response(*, ok: bool, status: int, json_data: dict[str, Any], headers: dict[str, str] | None = None) -> MagicMock:
    response = MagicMock()
    type(response).ok = PropertyMock(return_value=ok)
    type(response).status = PropertyMock(return_value=status)
    response.json = AsyncMock(return_value=json_data)
    response.text = AsyncMock(return_value="")
    response.headers = headers or {}
    return response


# --- Exception hierarchy ---


def test_coda_error_is_exception() -> None:
    assert issubclass(CodaError, Exception)


def test_exception_hierarchy() -> None:
    assert issubclass(NotFound, CodaError)
    assert issubclass(TooManyRequests, CodaError)
    assert issubclass(ContentTypeError, CodaError)
    assert issubclass(StatusCodeError, CodaError)
    assert issubclass(ResponseFormatError, CodaError)


# --- _handle_potential_error ---


async def test_handle_potential_error_ok() -> None:
    response = _make_mock_response(ok=True, status=200, json_data={})
    await _handle_potential_error(response)
    # No exception means success


async def test_handle_potential_error_404() -> None:
    response = _make_mock_response(ok=False, status=404, json_data={"message": "not found"})
    with pytest.raises(NotFound, match="404"):
        await _handle_potential_error(response)


async def test_handle_potential_error_429() -> None:
    response = _make_mock_response(ok=False, status=429, json_data={"message": "rate limited"})
    with pytest.raises(TooManyRequests, match="429") as exc_info:
        await _handle_potential_error(response)
    assert exc_info.value.retry_after is None


async def test_handle_potential_error_429_with_retry_after() -> None:
    response = _make_mock_response(ok=False, status=429, json_data={"message": "rate limited"}, headers={"Retry-After": "5"})
    with pytest.raises(TooManyRequests, match="429") as exc_info:
        await _handle_potential_error(response)
    assert exc_info.value.retry_after == 5.0


async def test_handle_potential_error_429_with_invalid_retry_after() -> None:
    response = _make_mock_response(ok=False, status=429, json_data={"message": "rate limited"}, headers={"Retry-After": "invalid"})
    with pytest.raises(TooManyRequests, match="429") as exc_info:
        await _handle_potential_error(response)
    assert exc_info.value.retry_after is None


async def test_handle_potential_error_500() -> None:
    response = _make_mock_response(ok=False, status=500, json_data={"message": "server error"})
    with pytest.raises(CodaError, match="500"):
        await _handle_potential_error(response)


# --- _handle_mutation_response ---


async def test_handle_mutation_response_success() -> None:
    response = _make_mock_response(ok=True, status=202, json_data={"requestId": "req-123"})
    request_id = await _handle_mutation_response(response)
    assert request_id == "req-123"


async def test_handle_mutation_response_wrong_status_code() -> None:
    response = _make_mock_response(ok=True, status=200, json_data={"requestId": "req-123"})
    with pytest.raises(StatusCodeError, match="202"):
        await _handle_mutation_response(response)


async def test_handle_mutation_response_missing_request_id() -> None:
    response = _make_mock_response(ok=True, status=202, json_data={"other": "data"})
    with pytest.raises(ResponseFormatError, match="requestId"):
        await _handle_mutation_response(response)


async def test_handle_mutation_response_error_status() -> None:
    response = _make_mock_response(ok=False, status=404, json_data={"message": "not found"})
    with pytest.raises(NotFound):
        await _handle_mutation_response(response)


# --- Client.post() / Client.delete() ---

_T = TypeVar("_T")

_REAL_SLEEP: Final = asyncio.sleep
_ROWS_ENDPOINT: Final = "/docs/doc-1/tables/grid-1/rows"
# In fake time (see fake_clock), so waiting for it doesn't take long
_TIMEOUT_SECS: Final = 3600
# In real time. Each test takes far less than a second.
_REAL_TIMEOUT_SECS: Final = 10


@final
class _FakeClock:
    """Stands in for time.monotonic() and asyncio.sleep(): sleeping doesn't wait but advances the clock and lets other tasks run."""

    def __init__(self, now: float) -> None:
        self._now = now

    def monotonic(self) -> float:
        return self._now

    async def sleep(self, delay: float) -> None:
        # Like a real sleep, take at least a bit of time: AdaptiveTokenBucket.acquire() can wait for e.g. 1e-16 s, which wouldn't
        # change the value of the clock, so the token bucket would never refill
        self._now += max(delay, 0.001)
        await _REAL_SLEEP(0)


@pytest.fixture
def fake_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Backoffs, retry pauses and polling take no real time. The fake clock only advances while a task sleeps, so a timeout in fake
    time never expires while all tasks wait without sleeping, e.g. in a deadlock on a semaphore. See _run_with_timeouts()."""
    # Patched on their modules, so the rate limiters, @retry, Client and the event loop (i.e. asyncio timeouts) all use the fake clock
    clock = _FakeClock(time.monotonic())
    monkeypatch.setattr(time, "monotonic", clock.monotonic)
    monkeypatch.setattr(asyncio, "sleep", clock.sleep)


@pytest.fixture
def restore_client_limits() -> Iterator[None]:
    """The rate limiters and the concurrency limit of Client are module level singletons. Restore their state after the test,
    so that e.g. a backoff or a reduced request rate doesn't leak into other tests."""
    limits: list[object] = [_request_limit, _token_bucket, _concurrency_limit._semaphore]  # pyright: ignore[reportPrivateUsage]
    states = [dict(vars(limit)) for limit in limits]
    yield
    for limit, state in zip(limits, states, strict=True):
        vars(limit).clear()
        vars(limit).update(state)


@final
class _FakeSession:
    """Stands in for aiohttp.ClientSession: logs each request with its JSON body and answers it with the next response given for
    its method."""

    def __init__(self, responses: Mapping[str, Iterable[MagicMock]], log: list[str]) -> None:
        self._responses: Final = {method: iter(method_responses) for method, method_responses in responses.items()}
        self._log: Final = log

    def get(self, url: str, **kwargs: Any) -> AbstractAsyncContextManager[MagicMock]:
        return self._request("GET", url, kwargs)

    def post(self, url: str, **kwargs: Any) -> AbstractAsyncContextManager[MagicMock]:
        return self._request("POST", url, kwargs)

    def delete(self, url: str, **kwargs: Any) -> AbstractAsyncContextManager[MagicMock]:
        return self._request("DELETE", url, kwargs)

    @asynccontextmanager
    async def _request(self, method: str, url: str, kwargs: Mapping[str, Any]) -> AsyncGenerator[MagicMock, None]:
        request = f"{method} {url.removeprefix(_API_ENDPOINT)}"
        if "json" in kwargs:
            request += f" json={kwargs['json']}"
        self._log.append(request)
        response = next(self._responses[method], None)
        if response is None:
            raise AssertionError(f"Unexpected request {request}")
        yield response


def _accepted(request_id: str) -> MagicMock:
    return _make_mock_response(ok=True, status=202, json_data={"requestId": request_id})


def _too_many_requests() -> MagicMock:
    return _make_mock_response(ok=False, status=429, json_data={"message": "rate limited"}, headers={"Retry-After": "1"})


def _server_error() -> MagicMock:
    return _make_mock_response(ok=False, status=500, json_data={"message": "server error"})


def _mutation_status(*, completed: bool) -> MagicMock:
    return _make_mock_response(ok=True, status=200, json_data={"completed": completed})


def _mutation_data(method: str) -> dict[str, Any]:
    """The data of the mutation that _mutate() sends"""
    if method == "POST":
        return {"rows": [{"cells": [{"column": "c-1", "value": "new"}]}]}
    return {"rowIds": ["i-1", "i-2"]}


def _mutation_request(method: str) -> str:
    """How _FakeSession logs the mutation request that _mutate() sends"""
    return f"{method} {_ROWS_ENDPOINT} json={_mutation_data(method)}"


async def _run_with_timeouts(call: Awaitable[_T]) -> _T:
    """Awaits call and fails the test if that takes longer than _TIMEOUT_SECS in fake time or _REAL_TIMEOUT_SECS in real time.
    Without the timeout in real time, a deadlock in which no task sleeps would make the test hang forever (see fake_clock)."""
    task = asyncio.create_task(asyncio.wait_for(call, timeout=_TIMEOUT_SECS))
    loop = asyncio.get_running_loop()
    timed_out = False

    def on_real_timeout() -> None:
        nonlocal timed_out
        timed_out = True
        task.cancel()

    # A thread, because the event loop's timers use the fake clock
    watchdog = threading.Timer(_REAL_TIMEOUT_SECS, lambda: loop.call_soon_threadsafe(on_real_timeout))
    watchdog.daemon = True
    watchdog.start()
    try:
        return await task
    except asyncio.CancelledError:
        if timed_out:
            pytest.fail(f"Still running after {_REAL_TIMEOUT_SECS} s in real time, e.g. because of a deadlock")
        raise
    finally:
        watchdog.cancel()
        watchdog.join()


async def _mutate(method: str, responses: Mapping[str, Iterable[MagicMock]], log: list[str]) -> RequestId:
    """Calls Client.post() or Client.delete() and logs the requests it sends and its calls of on_issued."""
    client = Client(cast(aiohttp.ClientSession, _FakeSession(responses, log)), "token")

    def on_issued() -> None:
        log.append("on_issued()")

    if method == "POST":
        call = client.post(_ROWS_ENDPOINT, data=_mutation_data(method), on_issued=on_issued)
    else:
        call = client.delete(_ROWS_ENDPOINT, data=_mutation_data(method), on_issued=on_issued)
    return await _run_with_timeouts(call)


@pytest.mark.parametrize("method", ["POST", "DELETE"])
@pytest.mark.usefixtures("fake_clock", "restore_client_limits")
async def test_mutation_waits_for_completion_after_too_many_requests(method: str) -> None:
    # After the 429, the rate limiter lets one request through to probe whether the rate limit is over: the next attempt of the
    # mutation. The requests for the mutation status go through the same rate limiter, which makes them wait for the probe to finish.
    log: list[str] = []
    responses = {method: [_too_many_requests(), _accepted("req-1")], "GET": [_mutation_status(completed=False), _mutation_status(completed=True)]}

    assert await _mutate(method, responses, log) == "req-1"

    assert log == [
        _mutation_request(method),
        _mutation_request(method),
        "on_issued()",
        "GET /mutationStatus/req-1",
        "GET /mutationStatus/req-1",
    ]


@pytest.mark.parametrize("method", ["POST", "DELETE"])
@pytest.mark.usefixtures("fake_clock", "restore_client_limits")
async def test_mutation_waits_for_completion_if_started_during_backoff(method: str) -> None:
    # Another request got a 429. Then the mutation request is the request that probes whether the rate limit is over.
    _request_limit._state = _State.backoff  # pyright: ignore[reportPrivateUsage]
    log: list[str] = []
    responses = {method: [_accepted("req-1")], "GET": [_mutation_status(completed=True)]}

    assert await _mutate(method, responses, log) == "req-1"

    assert log == [_mutation_request(method), "on_issued()", "GET /mutationStatus/req-1"]


@pytest.mark.parametrize("method", ["POST", "DELETE"])
@pytest.mark.usefixtures("fake_clock", "restore_client_limits")
async def test_mutation_waits_for_completion_without_taking_another_concurrency_slot(method: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # The mutation keeps its slot of the concurrency limit while it waits for completion. If the requests for its status needed
    # another slot, mutations holding all slots would wait for each other forever. With a single slot, one mutation is enough.
    monkeypatch.setattr(_concurrency_limit, "_semaphore", asyncio.Semaphore(1))
    log: list[str] = []
    responses = {method: [_accepted("req-1")], "GET": [_mutation_status(completed=False), _mutation_status(completed=True)]}

    assert await _mutate(method, responses, log) == "req-1"

    assert log == [_mutation_request(method), "on_issued()", "GET /mutationStatus/req-1", "GET /mutationStatus/req-1"]


@pytest.mark.parametrize("method", ["POST", "DELETE"])
@pytest.mark.usefixtures("fake_clock", "restore_client_limits")
async def test_mutation_is_sent_again_if_the_request_fails(method: str) -> None:
    log: list[str] = []
    responses = {method: [_server_error(), _accepted("req-1")], "GET": [_mutation_status(completed=True)]}

    assert await _mutate(method, responses, log) == "req-1"

    assert log == [_mutation_request(method), _mutation_request(method), "on_issued()", "GET /mutationStatus/req-1"]


@pytest.mark.parametrize("method", ["POST", "DELETE"])
@pytest.mark.usefixtures("fake_clock", "restore_client_limits")
async def test_mutation_waits_for_completion_if_status_requests_fail_for_a_while(method: str) -> None:
    # The requests for the mutation status fail for longer than each of them is retried (11 attempts), e.g. during an outage of the
    # status endpoint. Waiting for completion is retried as well, but the mutation isn't sent again.
    log: list[str] = []
    responses = {method: itertools.repeat(_accepted("req-1")), "GET": [_server_error()] * 15 + [_mutation_status(completed=True)]}

    assert await _mutate(method, responses, log) == "req-1"

    assert log == [_mutation_request(method), "on_issued()"] + ["GET /mutationStatus/req-1"] * 16


@pytest.mark.parametrize("method", ["POST", "DELETE"])
@pytest.mark.usefixtures("fake_clock", "restore_client_limits")
async def test_mutation_is_not_sent_again_if_waiting_for_completion_fails(method: str) -> None:
    # The server accepted the mutation, so sending it again would apply it twice, e.g. insert the rows twice
    log: list[str] = []
    responses = {method: itertools.repeat(_accepted("req-1")), "GET": itertools.repeat(_server_error())}

    with pytest.raises(CodaError, match="500"):
        await _mutate(method, responses, log)

    # Each request for the mutation status is attempted 11 times, and waiting for completion is attempted 11 times
    assert log == [_mutation_request(method), "on_issued()"] + ["GET /mutationStatus/req-1"] * 11 * 11
