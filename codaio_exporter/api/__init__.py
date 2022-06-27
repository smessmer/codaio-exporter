from contextlib import asynccontextmanager
from typing import AsyncGenerator
from codaio_exporter.api.doc import Doc
from codaio_exporter.api.client import Client, make_client

@asynccontextmanager
async def make_api(api_token: str) -> AsyncGenerator['API', None]:
    async with make_client(api_token) as client:
        yield API(client)

class API:
    def __init__(self, client: Client):
        self._client = client

    async def get_all_docs(self) -> AsyncGenerator[Doc, None]:
        async for doc in self._client.get("/docs"):
            yield Doc(self._client, doc)
