from contextlib import asynccontextmanager
from typing import AsyncGenerator
from codaio_exporter.api.doc import DocAPI
from codaio_exporter.api.client import Client, make_client

@asynccontextmanager
async def make_api(api_token: str) -> AsyncGenerator['API', None]:
    async with make_client(api_token) as client:
        yield API(client)

class API:
    def __init__(self, client: Client):
        self._client = client

    async def get_all_docs(self) -> AsyncGenerator[DocAPI, None]:
        async for doc in self._client.get_list("/docs"):
            yield DocAPI(self._client, doc)

    async def get_doc(self, doc_id: str) -> DocAPI:
        doc = await self._client.get_item(f"/docs/{doc_id}")
        return DocAPI(self._client, doc)
