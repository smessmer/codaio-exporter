from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from codaio_exporter.api.client import Client
from codaio_exporter.table import Column, Row, Table


def make_column_api_data(
    *,
    id: str = "col-1",
    name: str = "Column 1",
    calculated: bool | None = None,
    formula: str | None = None,
) -> dict[str, Any]:
    data: dict[str, Any] = {"id": id, "name": name}
    if calculated is not None:
        data["calculated"] = calculated
    if formula is not None:
        data["formula"] = formula
    return data


def make_row_api_data(
    *,
    id: str = "row-1",
    name: str = "Row 1",
    index: int = 0,
    values: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {"id": id, "name": name, "index": index, "values": values or {}}


def make_doc_api_data(
    *,
    id: str = "doc-1",
    name: str = "My Doc",
    folder_id: str = "folder-1",
    folder_name: str | None = "My Folder",
) -> dict[str, Any]:
    folder: dict[str, Any] = {"id": folder_id}
    if folder_name is not None:
        folder["name"] = folder_name
    return {"id": id, "name": name, "folder": folder}


def make_table_api_data(
    *,
    id: str = "table-1",
    name: str = "My Table",
    table_type: str = "table",
) -> dict[str, Any]:
    return {"id": id, "name": name, "tableType": table_type}


def make_column(
    *,
    id: str = "col-1",
    name: str = "Column 1",
    calculated: bool = False,
    formula: str | None = None,
) -> Column:
    return Column(id=id, name=name, calculated=calculated, formula=formula)


def make_row(
    *,
    index: int = 0,
    id: str = "row-1",
    name: str = "Row 1",
    cells: list[str] | None = None,
    raw_data: dict[str, Any] | None = None,
) -> Row:
    return Row(index=index, id=id, name=name, cells=cells or [], raw_data=raw_data or {})


def make_table(
    *,
    id: str = "table-1",
    name: str = "My Table",
    columns: list[Column] | None = None,
    rows: list[Row] | None = None,
) -> Table:
    return Table(id=id, name=name, columns=columns or [], rows=rows or [])


async def async_generator_from_list(items: list[Any]) -> AsyncGenerator[Any, None]:
    for item in items:
        yield item


def make_mock_client() -> MagicMock:
    client = MagicMock(spec=Client)
    client.get_item = AsyncMock()
    client.get_list = MagicMock()
    client.post = AsyncMock()
    client.delete = AsyncMock()
    return client
