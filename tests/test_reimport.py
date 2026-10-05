from __future__ import annotations

import codecs
import re
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from codaio_exporter.api.column import ColumnAPI
from codaio_exporter.api.table import TableAPI, TableType
from codaio_exporter.errors import DataFormatError, SchemaValidationError
from codaio_exporter.export import _write_file  # pyright: ignore[reportPrivateUsage]
from codaio_exporter.reimport import (
    ProgressHandler,
    _check_columns_are_compatible,  # pyright: ignore[reportPrivateUsage]
    _check_table_is_compatible,  # pyright: ignore[reportPrivateUsage]
    _insert_rows,  # pyright: ignore[reportPrivateUsage]
    _load_table,  # pyright: ignore[reportPrivateUsage]
)

from .conftest import (
    async_generator_from_list,
    make_column,
    make_column_api_data,
    make_mock_client,
    make_row,
    make_table,
    make_table_api_data,
    run_python_with_ascii_locale,
)


def _make_mock_table_api(
    *,
    name: str = "My Table",
    table_type: TableType = TableType.table,
    client: MagicMock | None = None,
) -> MagicMock:
    table_api = MagicMock()
    table_api.name.return_value = name
    table_api.type.return_value = table_type
    if client is not None:
        table_api._client = client
    table_api.get_all_columns = MagicMock()
    table_api.insert_rows = AsyncMock()
    table_api.get_all_rows = MagicMock()
    table_api.delete_rows = AsyncMock()
    return table_api


def _make_mock_progress_handler() -> MagicMock:
    handler = MagicMock()
    handler.increment_reimport_issued = MagicMock()
    handler.increment_reimport_complete = MagicMock()
    return handler


# --- _check_table_is_compatible ---


async def test_check_table_compatible_success() -> None:
    table_api = _make_mock_table_api(name="My Table", table_type=TableType.table)
    col = ColumnAPI(make_column_api_data(id="c1", name="Col1", calculated=False))
    table_api.get_all_columns.return_value = async_generator_from_list([col])

    table = make_table(name="My Table", columns=[make_column(id="c1", name="Col1", calculated=False)])

    # Should not raise
    await _check_table_is_compatible(table_api, table)


async def test_check_table_name_mismatch() -> None:
    table_api = _make_mock_table_api(name="Server Table")
    table = make_table(name="Export Table")

    with pytest.raises(SchemaValidationError, match="Export states table name"):
        await _check_table_is_compatible(table_api, table)


async def test_check_table_name_mismatch_message_shows_server_name() -> None:
    table_api = TableAPI(make_mock_client(), "/docs/d-1", make_table_api_data(name="Server Table"))
    table = make_table(name="Export Table")

    with pytest.raises(SchemaValidationError, match=re.escape("table name is Export Table but server thinks it is Server Table.")) as exc_info:
        await _check_table_is_compatible(table_api, table)
    assert "bound method" not in str(exc_info.value)


async def test_check_table_type_not_table() -> None:
    table_api = _make_mock_table_api(name="My Table", table_type=TableType.view)
    table = make_table(name="My Table")

    with pytest.raises(SchemaValidationError, match="expected it to be 'table'"):
        await _check_table_is_compatible(table_api, table)


# --- _check_columns_are_compatible ---


async def test_check_columns_compatible_success() -> None:
    table_api = _make_mock_table_api()
    col = ColumnAPI(make_column_api_data(id="c1", name="Col1", calculated=False))
    table_api.get_all_columns.return_value = async_generator_from_list([col])

    table = make_table(columns=[make_column(id="c1", name="Col1", calculated=False)])

    await _check_columns_are_compatible(table_api, table)


async def test_check_columns_missing_on_server() -> None:
    table_api = _make_mock_table_api()
    table_api.get_all_columns.return_value = async_generator_from_list([])

    table = make_table(columns=[make_column(id="c1", name="Col1")])

    with pytest.raises(SchemaValidationError, match="found in export but not on server"):
        await _check_columns_are_compatible(table_api, table)


async def test_check_columns_name_mismatch() -> None:
    table_api = _make_mock_table_api()
    col = ColumnAPI(make_column_api_data(id="c1", name="Server Name"))
    table_api.get_all_columns.return_value = async_generator_from_list([col])

    table = make_table(columns=[make_column(id="c1", name="Export Name")])

    with pytest.raises(SchemaValidationError, match="Export states column name"):
        await _check_columns_are_compatible(table_api, table)


async def test_check_columns_name_mismatch_message_shows_server_name() -> None:
    # Table and column names all differ, so the assertion can tell which object's name ended up in the message.
    client = make_mock_client()
    client.get_list.return_value = async_generator_from_list([make_column_api_data(id="c1", name="Server Column")])
    table_api = TableAPI(client, "/docs/d-1", make_table_api_data(name="Server Table"))
    table = make_table(name="Export Table", columns=[make_column(id="c1", name="Export Column")])

    with pytest.raises(SchemaValidationError, match=re.escape("column name is Export Column but server thinks it is Server Column.")) as exc_info:
        await _check_columns_are_compatible(table_api, table)
    assert "bound method" not in str(exc_info.value)


async def test_check_columns_manual_to_calculated_mismatch() -> None:
    table_api = _make_mock_table_api()
    col = ColumnAPI(make_column_api_data(id="c1", name="Col1", calculated=True))
    table_api.get_all_columns.return_value = async_generator_from_list([col])

    table = make_table(columns=[make_column(id="c1", name="Col1", calculated=False)])

    with pytest.raises(SchemaValidationError, match="manual column but server states it is a calculated"):
        await _check_columns_are_compatible(table_api, table)


async def test_check_columns_calculated_to_manual_mismatch() -> None:
    table_api = _make_mock_table_api()
    col = ColumnAPI(make_column_api_data(id="c1", name="Col1", calculated=False))
    table_api.get_all_columns.return_value = async_generator_from_list([col])

    table = make_table(columns=[make_column(id="c1", name="Col1", calculated=True)])

    with pytest.raises(SchemaValidationError, match="calculated column but server states it is a manual"):
        await _check_columns_are_compatible(table_api, table)


# --- _insert_rows (skips formula columns, validates column count) ---


async def test_insert_rows_skips_formula_columns() -> None:
    table_api = _make_mock_table_api()
    progress = _make_mock_progress_handler()

    col_manual = make_column(id="c1", name="Manual", calculated=False, formula=None)
    col_formula = make_column(id="c2", name="Formula", calculated=True, formula="=A+B")
    row = make_row(index=0, id="r1", name="Row", cells=["val1", "val2"])
    table = make_table(columns=[col_manual, col_formula], rows=[row])

    await _insert_rows(table_api, table, progress)

    call_args = table_api.insert_rows.call_args
    inserted_rows = call_args.args[0]
    # Only the manual column should be in the inserted data
    assert len(inserted_rows) == 1
    assert inserted_rows[0] == {"c1": "val1"}


async def test_insert_rows_wrong_column_count_raises() -> None:
    table_api = _make_mock_table_api()
    progress = _make_mock_progress_handler()

    col = make_column(id="c1", name="Col")
    row = make_row(index=0, id="r1", name="Row", cells=["a", "b", "c"])
    table = make_table(columns=[col], rows=[row])

    with pytest.raises(DataFormatError, match="columns but a row"):
        await _insert_rows(table_api, table, progress)


# --- _read_file ---


async def test_read_file_reads_exported_file_with_ascii_locale(tmp_path: Path) -> None:
    # The export may have run with a different locale, e.g. on a different system
    text = '{"cells": ["Grüße ☕ 5 € 日本語 😀"]}'
    await _write_file(tmp_path / "table.json", text)

    output = run_python_with_ascii_locale(
        """
        import asyncio
        from pathlib import Path
        from codaio_exporter.reimport import _read_file
        print(ascii(asyncio.run(_read_file(Path("table.json")))))
        """,
        cwd=tmp_path,
    )

    assert output.strip() == ascii(text)


# --- _load_table ---


async def test_load_table_reads_table_json_with_byte_order_mark(tmp_path: Path) -> None:
    # Some editors write a byte order mark at the start of UTF-8 files, e.g. when saving table.json after editing it
    table = make_table(columns=[make_column(name="Größe")], rows=[make_row(cells=["☕"])])
    path = tmp_path / "table.json"
    path.write_bytes(codecs.BOM_UTF8 + table.to_json(ensure_ascii=False).encode("utf-8"))

    assert await _load_table(path, ProgressHandler(1, None)) == table
