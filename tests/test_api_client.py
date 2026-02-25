from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, PropertyMock

import pytest

from codaio_exporter.api.client import (
    CodaError,
    ContentTypeError,
    NotFound,
    ResponseFormatError,
    StatusCodeError,
    TooManyRequests,
    _handle_mutation_response,  # pyright: ignore[reportPrivateUsage]
    _handle_potential_error,  # pyright: ignore[reportPrivateUsage]
)


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
