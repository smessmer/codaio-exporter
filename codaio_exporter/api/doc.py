from typing import Dict, Any, AsyncGenerator
from codaio_exporter.api.client import Client
from codaio_exporter.api.parse import parse_str
from codaio_exporter.api.table import Table


class Doc:
    def __init__(self, client: Client, data: Dict[str, Any]):
        self._client = client
        self._data = data
        self._api_root = f"/docs/{self.id()}"

    def raw_data(self) -> Dict[str, Any]:
        return self._data

    def id(self) -> str:
        return parse_str(self._data["id"])

    def name(self) -> str:
        return parse_str(self._data["name"])

    def folder_id(self) -> str:
        return parse_str(self._data["folder"]["id"])
    
    def folder_name(self) -> str:
        return parse_str(self._data["folder"]["name"])

    async def get_all_tables(self) -> AsyncGenerator[Table, None]:
        async for table in self._client.get(f"{self._api_root}/tables"):
            yield Table(self._client, self._api_root, table)
    
    async def get_num_tables(self) -> int:
        # TODO
        return 0
