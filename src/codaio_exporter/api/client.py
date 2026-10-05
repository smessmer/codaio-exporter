from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from functools import wraps
from typing import Any, Final, NewType, ParamSpec, TypeVar, final

import aiohttp

from codaio_exporter.api.parse import parse_bool, parse_dict_str_any, parse_str
from codaio_exporter.utils.concurrencylimit import ConcurrencyLimit
from codaio_exporter.utils.ratelimit import AdaptiveRateLimit
from codaio_exporter.utils.retry import retry
from codaio_exporter.utils.tokenbucket import AdaptiveTokenBucket


@asynccontextmanager
async def make_client(api_token: str) -> AsyncGenerator[Client, None]:
    async with aiohttp.ClientSession() as session:
        yield Client(session, api_token)


class CodaError(Exception):
    pass


class NotFound(CodaError):
    pass


class TooManyRequests(CodaError):
    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after: Final = retry_after


class ContentTypeError(CodaError):
    pass


class StatusCodeError(CodaError):
    pass


class ResponseFormatError(CodaError):
    pass


_P = ParamSpec("_P")
_R = TypeVar("_R")

_MAX_PAGE_SIZE = 200
_API_ENDPOINT = "https://coda.io/apis/v1"


@final
class _TokenBucketDecorator:
    """Decorator that acquires a token before each request and feeds back
    success/failure to the adaptive token bucket for rate adjustment.

    Must be the innermost decorator (closest to the actual HTTP call) because
    AdaptiveRateLimit swallows TooManyRequests in its internal retry loop.
    """

    def __init__(self, bucket: AdaptiveTokenBucket) -> None:
        self._bucket: Final = bucket

    def __call__(self, func: Callable[_P, Awaitable[_R]]) -> Callable[_P, Awaitable[_R]]:
        @wraps(func)
        async def inner(*args: _P.args, **kwds: _P.kwargs) -> _R:
            epoch = await self._bucket.acquire()
            try:
                result = await func(*args, **kwds)
                self._bucket.on_success()
                return result
            except TooManyRequests as e:
                self._bucket.on_rate_limited(epoch, e.retry_after)
                raise

        return inner


_token_bucket = AdaptiveTokenBucket()
_token_bucket_limit = _TokenBucketDecorator(_token_bucket)
_request_limit = AdaptiveRateLimit(TooManyRequests, 10)
_concurrency_limit = ConcurrencyLimit(10)

RequestId = NewType("RequestId", str)


@final
class Client:
    def __init__(self, session: aiohttp.ClientSession, api_token: str):
        self._session: Final = session
        self._authorization: Final = {"Authorization": f"Bearer {api_token}"}

    @_concurrency_limit
    async def get_item(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if params is None:
            params = {}
        response = await self._get_item(endpoint, params=params)
        return response

    @retry(10)
    @_request_limit
    @_token_bucket_limit
    async def _get_item(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if params is None:
            params = {}
        logging.info(f"GET {endpoint} {params!s}")
        async with self._session.get(_API_ENDPOINT + endpoint, params=params, headers=self._authorization) as response:
            try:
                await _handle_potential_error(response)
                content = await response.json()
                logging.info(f"GET {endpoint}: responded")
            except aiohttp.ContentTypeError as e:
                content_text = await response.text()
                raise ContentTypeError(f"Content type error for {content_text}", e) from e

            return parse_dict_str_any(content)

    async def get_list(self, endpoint: str, params: dict[str, Any] | None = None) -> AsyncGenerator[Any, None]:
        if params is None:
            params = {}
        logging.info(f"GET {endpoint} {params!s}")

        params["limit"] = _MAX_PAGE_SIZE

        page = await self._get_page(_API_ENDPOINT + endpoint, params)
        for item in page.pop("items"):
            yield item

        while page.get("nextPageLink") is not None:
            nextPageLink = page.get("nextPageLink")
            assert nextPageLink is not None
            page = await self._get_page(nextPageLink, params={})
            for item in page.pop("items"):
                yield item

        logging.info(f"GET {endpoint}: responded")

    @_concurrency_limit
    @retry(10)
    @_request_limit
    @_token_bucket_limit
    async def _get_page(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        async with self._session.get(url, params=params, headers=self._authorization) as response:
            try:
                await _handle_potential_error(response)
                content = await response.json()
            except aiohttp.ContentTypeError as e:
                content_text = await response.text()
                raise ContentTypeError(f"Content type error for {content_text}", e) from e
            return parse_dict_str_any(content)

    # A mutation is sent by _post() or _delete(), which are retried and rate limited. post() and delete() then wait for its
    # completion outside of them, in _wait_until_mutation_is_completed():
    # - The mutation status requests go through the same rate limiter. After a 429, the rate limiter lets one request through to
    #   probe whether the rate limit is over. If that request waited for the mutation to complete, the status requests would wait
    #   for it forever.
    # - If waiting fails, it is retried on its own. Sending the mutation again would apply it twice: the server already accepted it.
    # - post() and delete() keep their concurrency slot while waiting, so the status requests must not need another slot. Otherwise
    #   mutations holding all slots would wait for each other forever.
    @_concurrency_limit
    async def post(
        self, endpoint: str, data: dict[str, Any], on_issued: Callable[[], None] | None = None, wait_for_completion: bool = True
    ) -> RequestId:
        request_id = await self._post(endpoint, data)
        if on_issued is not None:
            on_issued()
        if wait_for_completion:
            await self._wait_until_mutation_is_completed(request_id)
            logging.info(f"POST {endpoint}: completed")
        return request_id

    @retry(10)
    @_request_limit
    @_token_bucket_limit
    async def _post(self, endpoint: str, data: dict[str, Any]) -> RequestId:
        logging.info(f"POST {endpoint}")
        async with self._session.post(
            _API_ENDPOINT + endpoint,
            json=data,
            headers={**self._authorization, "Content-Type": "application/json"},
        ) as response:
            request_id = await _handle_mutation_response(response)
            logging.info(f"POST {endpoint}: responded")
            return request_id

    @_concurrency_limit
    async def delete(
        self, endpoint: str, data: dict[str, Any] | None = None, on_issued: Callable[[], None] | None = None, wait_for_completion: bool = True
    ) -> RequestId:
        if data is None:
            data = {}
        request_id = await self._delete(endpoint, data)
        if on_issued is not None:
            on_issued()
        if wait_for_completion:
            await self._wait_until_mutation_is_completed(request_id)
            logging.info(f"DELETE {endpoint}: completed")
        return request_id

    @retry(10)
    @_request_limit
    @_token_bucket_limit
    async def _delete(self, endpoint: str, data: dict[str, Any]) -> RequestId:
        logging.info(f"DELETE {endpoint} {data!s}")

        async with self._session.delete(_API_ENDPOINT + endpoint, json=data, headers=self._authorization) as response:
            request_id = await _handle_mutation_response(response)
            logging.info(f"DELETE {endpoint}: responded")
            return request_id

    async def _get_mutation_is_completed(self, request_id: RequestId) -> bool:
        response = await self._get_item(f"/mutationStatus/{request_id}")
        if "completed" not in response:
            raise ResponseFormatError(f"Expected 'completed' to be in response but response was {response}")
        return parse_bool(response["completed"])

    # Each status request is already retried by _get_item(). Retrying the whole wait as well lets a mutation outlast a longer outage
    # of the status endpoint without being sent again (see the comment above post()).
    @retry(10)
    async def _wait_until_mutation_is_completed(self, request_id: RequestId) -> None:
        while not await self._get_mutation_is_completed(request_id):
            await asyncio.sleep(1)


async def _handle_potential_error(response: aiohttp.ClientResponse) -> None:
    if response.ok:
        return

    content = await response.json()
    message = f"Status code: {response.status}. Message: {content['message']}"

    if response.status == 404:
        raise NotFound(message)
    if response.status == 429:
        raise TooManyRequests(message, retry_after=_parse_retry_after(response))

    raise CodaError(message)


def _parse_retry_after(response: aiohttp.ClientResponse) -> float | None:
    """Parse the Retry-After header value as seconds, or None if absent/invalid."""
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


async def _handle_mutation_response(response: aiohttp.ClientResponse) -> RequestId:
    try:
        await _handle_potential_error(response)
        if response.status != 202:
            raise StatusCodeError(f"Expected status code 202 but found {response.status}")
        content = await response.json()
        if "requestId" not in content:
            raise ResponseFormatError(f"Expected 'requestId' in response but response was {content}")
        return RequestId(parse_str(content["requestId"]))
    except aiohttp.ContentTypeError as e:
        content_text = await response.text()
        raise ContentTypeError(f"Content type error for {content_text}", e) from e
