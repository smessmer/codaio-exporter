from __future__ import annotations

import pytest

from codaio_exporter.api.column import ColumnAPI
from codaio_exporter.api.row import RowAPI
from codaio_exporter.api.table import TableAPI, TableType

from .conftest import (
    async_generator_from_list,
    make_column_api_data,
    make_mock_client,
    make_row_api_data,
    make_table_api_data,
)


def test_table_type_view_to_str() -> None:
    assert TableType.view.to_str() == "view"


def test_table_type_table_to_str() -> None:
    assert TableType.table.to_str() == "table"


def test_table_api_id() -> None:
    client = make_mock_client()
    table = TableAPI(client, "/docs/d-1", make_table_api_data(id="t-1"))
    assert table.id() == "t-1"


def test_table_api_name() -> None:
    client = make_mock_client()
    table = TableAPI(client, "/docs/d-1", make_table_api_data(name="Tasks"))
    assert table.name() == "Tasks"


def test_table_api_raw_data() -> None:
    client = make_mock_client()
    data = make_table_api_data()
    table = TableAPI(client, "/docs/d-1", data)
    assert table.raw_data() is data


def test_table_api_type_table() -> None:
    client = make_mock_client()
    table = TableAPI(client, "/docs/d-1", make_table_api_data(table_type="table"))
    assert table.type() == TableType.table


def test_table_api_type_view() -> None:
    client = make_mock_client()
    table = TableAPI(client, "/docs/d-1", make_table_api_data(table_type="view"))
    assert table.type() == TableType.view


def test_table_api_type_unknown_raises() -> None:
    client = make_mock_client()
    table = TableAPI(client, "/docs/d-1", make_table_api_data(table_type="other"))
    with pytest.raises(Exception, match="Unknown table type"):
        table.type()


async def test_table_api_get_all_columns() -> None:
    client = make_mock_client()
    col_data = make_column_api_data(id="c-1", name="Name")
    client.get_list.return_value = async_generator_from_list([col_data])

    table = TableAPI(client, "/docs/d-1", make_table_api_data(id="t-1"))
    columns: list[ColumnAPI] = []
    async for col in table.get_all_columns():
        columns.append(col)

    assert len(columns) == 1
    assert columns[0].id() == "c-1"


async def test_table_api_get_all_rows() -> None:
    client = make_mock_client()
    row_data = make_row_api_data(id="r-1", name="Row 1", index=0, values={"c1": "v1"})
    client.get_list.return_value = async_generator_from_list([row_data])

    table = TableAPI(client, "/docs/d-1", make_table_api_data(id="t-1"))
    rows: list[RowAPI] = []
    async for row in table.get_all_rows():
        rows.append(row)

    assert len(rows) == 1
    assert rows[0].id() == "r-1"


async def test_table_api_delete_rows() -> None:
    client = make_mock_client()
    table = TableAPI(client, "/docs/d-1", make_table_api_data(id="t-1"))

    await table.delete_rows(["r-1", "r-2"])

    client.delete.assert_called_once_with(
        "/docs/d-1/tables/t-1/rows",
        data={"rowIds": ["r-1", "r-2"]},
        on_issued=None,
    )


async def test_table_api_delete_rows_with_callback() -> None:
    client = make_mock_client()
    table = TableAPI(client, "/docs/d-1", make_table_api_data(id="t-1"))
    callback_called = False

    def callback() -> None:
        nonlocal callback_called
        callback_called = True

    await table.delete_rows(["r-1"], on_issued=callback)

    client.delete.assert_called_once()
    # The callback is passed through to the client
    call_kwargs = client.delete.call_args[1]
    assert call_kwargs["on_issued"] is callback


async def test_table_api_insert_rows() -> None:
    client = make_mock_client()
    table = TableAPI(client, "/docs/d-1", make_table_api_data(id="t-1"))

    await table.insert_rows([{"c1": "v1"}])

    client.post.assert_called_once_with(
        "/docs/d-1/tables/t-1/rows",
        data={"rows": [{"cells": [{"column": "c1", "value": "v1"}]}]},
        on_issued=None,
    )


async def test_table_api_insert_rows_multiple_columns() -> None:
    client = make_mock_client()
    table = TableAPI(client, "/docs/d-1", make_table_api_data(id="t-1"))

    await table.insert_rows([{"c1": "v1", "c2": "v2"}])

    call_args = client.post.call_args
    rows_data = call_args[1]["data"]["rows"]
    assert len(rows_data) == 1
    cells = rows_data[0]["cells"]
    assert len(cells) == 2
    assert {"column": "c1", "value": "v1"} in cells
    assert {"column": "c2", "value": "v2"} in cells
