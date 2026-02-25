from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any, Final, final

from codaio_exporter.api.client import Client
from codaio_exporter.api.parse import parse_str
from codaio_exporter.api.table import TableAPI


@final
class DocAPI:
    def __init__(self, client: Client, data: dict[str, Any]):
        self._client: Final = client
        self._data: Final = data
        self._api_root: Final = f"/docs/{self.id()}"

    def raw_data(self) -> dict[str, Any]:
        return self._data

    def id(self) -> str:
        return parse_str(self._data["id"])

    def name(self) -> str:
        return parse_str(self._data["name"])

    def folder_id(self) -> str:
        return parse_str(self._data["folder"]["id"])

    def folder_name(self) -> str | None:
        if "name" in self._data["folder"]:
            return parse_str(self._data["folder"]["name"])
        else:
            return None

    async def get_all_tables(self) -> AsyncGenerator[TableAPI, None]:
        async for table in self._client.get_list(f"{self._api_root}/tables"):
            yield TableAPI(self._client, self._api_root, table)

    async def get_table(self, table_id: str) -> TableAPI:
        table = await self._client.get_item(f"{self._api_root}/tables/{table_id}")
        return TableAPI(self._client, self._api_root, table)
