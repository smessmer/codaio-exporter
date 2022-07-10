from typing import Dict, Any, AsyncGenerator, List, Optional, Callable, final, Final
from enum import Enum
from codaio_exporter.api.parse import parse_str
from codaio_exporter.api.column import ColumnAPI
from codaio_exporter.api.row import RowAPI
from codaio_exporter.api.client import Client


@final
class TableType(Enum):
    view = "view"
    table = "table"

    def to_str(self) -> str:
        if self == TableType.view:
            return "view"
        elif self == TableType.table:
            return "table"
        else:
            raise Exception(f"Unknown table type {self}")


@final
class TableAPI:
    def __init__(self, client: Client, doc_api_root: str, data: Dict[str, Any]):
        self._data: Final = data
        self._client: Final = client
        self._api_root: Final = f"{doc_api_root}/tables/{self.id()}"

    def raw_data(self) -> Dict[str, Any]:
        return self._data

    def id(self) -> str:
        return parse_str(self._data["id"])

    def name(self) -> str:
        return parse_str(self._data["name"])

    def type(self) -> TableType:
        parsed = parse_str(self._data["tableType"])
        if parsed == "view":
            return TableType.view
        elif parsed == "table":
            return TableType.table
        else:
            raise Exception(f"Unknown table type {parsed}")

    async def get_all_columns(self) -> AsyncGenerator[ColumnAPI, None]:
        async for column in self._client.get_list(f"{self._api_root}/columns"):
            yield ColumnAPI(column)

    async def get_all_rows(self) -> AsyncGenerator[RowAPI, None]:
        async for row in self._client.get_list(f"{self._api_root}/rows"):
            yield RowAPI(row)

    async def delete_rows(self, row_ids: List[str], on_issued: Optional[Callable[[], None]] = None) -> None:
        await self._client.delete(f"{self._api_root}/rows", data={"rowIds": row_ids}, on_issued=on_issued)

    # Each entry of rows is a Dict from column id to value
    async def insert_rows(self, rows: List[Dict[str, str]], on_issued: Optional[Callable[[], None]] = None) -> None:
        def format_cell(column: str, value: str) -> Dict[str, str]:
            return {"column": column, "value": value}
        def format_row(row: Dict[str, str]) -> List[Dict[str, str]]:
            return [format_cell(column, value) for (column, value) in row.items()]
        rows_data = [{"cells": format_row(row)} for row in rows]
        await self._client.post(f"{self._api_root}/rows", data={"rows": rows_data}, on_issued=on_issued)
