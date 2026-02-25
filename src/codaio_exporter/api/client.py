import asyncio
import logging
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from typing import Any, Final, NewType, final

import aiohttp

from codaio_exporter.api.parse import parse_bool, parse_dict_str_any, parse_str
from codaio_exporter.utils.concurrencylimit import ConcurrencyLimit
from codaio_exporter.utils.ratelimit import AdaptiveRateLimit
from codaio_exporter.utils.retry import retry


@asynccontextmanager
async def make_client(api_token: str) -> AsyncGenerator["Client", None]:
    async with aiohttp.ClientSession() as session:
        yield Client(session, api_token)


class CodaError(Exception):
    pass


class NotFound(CodaError):
    pass


class TooManyRequests(CodaError):
    pass


class ContentTypeError(CodaError):
    pass


class StatusCodeError(CodaError):
    pass


class ResponseFormatError(CodaError):
    pass


_MAX_PAGE_SIZE = 200
_API_ENDPOINT = "https://coda.io/apis/v1"

_request_limit = AdaptiveRateLimit(TooManyRequests, 10)
_concurrency_limit = ConcurrencyLimit(50)

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
    async def _get_page(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        async with self._session.get(url, params=params, headers=self._authorization) as response:
            try:
                await _handle_potential_error(response)
                content = await response.json()
            except aiohttp.ContentTypeError as e:
                content_text = await response.text()
                raise ContentTypeError(f"Content type error for {content_text}", e) from e
            return parse_dict_str_any(content)

    @_concurrency_limit
    @retry(10)
    @_request_limit
    async def post(
        self, endpoint: str, data: dict[str, Any], on_issued: Callable[[], None] | None = None, wait_for_completion: bool = True
    ) -> RequestId:
        logging.info(f"POST {endpoint}")
        async with self._session.post(
            _API_ENDPOINT + endpoint,
            json=data,
            headers={**self._authorization, "Content-Type": "application/json"},
        ) as response:
            request_id = await _handle_mutation_response(response)
            logging.info(f"POST {endpoint}: responded")
            if on_issued is not None:
                on_issued()
            if wait_for_completion:
                await self._wait_until_mutation_is_completed(request_id)
                logging.info(f"POST {endpoint}: completed")
            return request_id

    @_concurrency_limit
    @retry(10)
    @_request_limit
    async def delete(
        self, endpoint: str, data: dict[str, Any] | None = None, on_issued: Callable[[], None] | None = None, wait_for_completion: bool = True
    ) -> RequestId:
        if data is None:
            data = {}
        logging.info(f"DELETE {endpoint} {data!s}")

        async with self._session.delete(_API_ENDPOINT + endpoint, json=data, headers=self._authorization) as response:
            request_id = await _handle_mutation_response(response)
            logging.info(f"DELETE {endpoint}: responded")
            if on_issued is not None:
                on_issued()
            if wait_for_completion:
                await self._wait_until_mutation_is_completed(request_id)
                logging.info(f"DELETE {endpoint}: completed")
            return request_id

    async def _get_mutation_is_completed(self, request_id: RequestId) -> bool:
        response = await self._get_item(f"/mutationStatus/{request_id}")
        if "completed" not in response:
            raise ResponseFormatError(f"Expected 'completed' to be in response but response was {response}")
        return parse_bool(response["completed"])

    async def _wait_until_mutation_is_completed(self, request_id: RequestId) -> None:
        while not await self._get_mutation_is_completed(request_id):
            await asyncio.sleep(1)


async def _handle_potential_error(response: aiohttp.ClientResponse) -> None:
    if response.ok:
        return

    content = await response.json()

    error_dict = {404: NotFound, 429: TooManyRequests}

    if response.status in error_dict:
        raise error_dict[response.status](f"Status code: {response.status}. Message: {content['message']}")

    raise CodaError(f"Status code: {response.status}. Message: {content['message']}")


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
