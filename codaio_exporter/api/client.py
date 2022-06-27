from typing import List, Dict, Any, Optional, AsyncGenerator
import aiohttp
import logging
import asyncio
from contextlib import asynccontextmanager

from codaio_exporter.api.parse import parse_dict_str_any
from codaio_exporter.utils.ratelimit import AdaptiveRateLimit
from codaio_exporter.utils.retry import retry

@asynccontextmanager
async def make_client(api_token: str) -> AsyncGenerator['Client', None]:
    async with aiohttp.ClientSession() as session:
        yield Client(session, api_token)


class CodaError(Exception):
    pass

class NotFound(CodaError):
    pass

class TooManyRequests(CodaError):
    pass


_MAX_PAGE_SIZE = 200
_API_ENDPOINT = "https://coda.io/apis/v1"

_request_limit = AdaptiveRateLimit(TooManyRequests, 10)


class Client:
    def __init__(self, session: aiohttp.ClientSession, api_token: str):
        self._session = session
        self._authorization = {"Authorization": f"Bearer {api_token}"}


    async def get(self, endpoint: str, data: Dict[str, Any] = {}) -> AsyncGenerator[Any, None]:
        logging.info(f"GET {endpoint} {str(data)}")

        data["limit"] = _MAX_PAGE_SIZE

        page = await self._get_page(_API_ENDPOINT + endpoint, data)
        for item in page.pop("items"):
            yield item

        while page.get("nextPageLink") is not None:
            nextPageLink = page.get("nextPageLink")
            assert nextPageLink is not None
            page = await self._get_page(nextPageLink, data={})
            for item in page.pop("items"):
                yield item

        logging.info(f"GET {endpoint}: responded")
    
    @retry(5)
    @_request_limit
    async def _get_page(self, url: str, data: Dict[str, Any]) -> Dict[str, Any]:
        async with self._session.get(url, params=data, headers=self._authorization) as response:
            try:
                await _handle_potential_error(response)
                content = await response.json()
            except aiohttp.client_exceptions.ContentTypeError as e:
                content_text = await response.text()
                raise Exception(f"Content type error for {content_text}", e)
            return parse_dict_str_any(content)

    @retry(5)
    @_request_limit
    async def post(self, endpoint: str, data: Dict[str, Any]) -> None:
        logging.info(f"POST {endpoint}")
        async with self._session.post(
            _API_ENDPOINT + endpoint,
            json=data,
            headers={**self._authorization, "Content-Type": "application/json"},
        ) as response:
            await _handle_potential_error(response)
            logging.info(f"POST {endpoint}: responded")

    @retry(5)
    @_request_limit
    async def put(self, endpoint: str, data: Dict[str, Any]) -> None:
        logging.info(f"PUT {endpoint}")
        async with self._session.put(_API_ENDPOINT + endpoint, json=data, headers=self._authorization) as response:
            await _handle_potential_error(response)
            logging.info(f"PUT {endpoint}: responded")


async def _handle_potential_error(response: aiohttp.ClientResponse) -> None:
    if response.ok:
        return

    content = await response.json()

    error_dict = {404: NotFound, 429: TooManyRequests}

    if response.status in error_dict:
        raise error_dict[response.status](
            f'Status code: {response.status}. Message: {content["message"]}'
        )

    raise CodaError(
        f'Status code: {response.status}. Message: {content["message"]}'
    )
