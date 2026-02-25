from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from codaio_exporter.api.column import ColumnAPI
from codaio_exporter.api.table import TableType
from codaio_exporter.export import (
    _column_name_for_path,  # pyright: ignore[reportPrivateUsage]
    _doc_path,  # pyright: ignore[reportPrivateUsage]
    _format_index,  # pyright: ignore[reportPrivateUsage]
    _remove_path_unsafe_characters,  # pyright: ignore[reportPrivateUsage]
    _row_name_for_path,  # pyright: ignore[reportPrivateUsage]
    _table_path,  # pyright: ignore[reportPrivateUsage]
)

from .conftest import make_column_api_data, make_row

# --- _remove_path_unsafe_characters ---


def test_remove_path_unsafe_characters_with_slashes() -> None:
    assert _remove_path_unsafe_characters("a/b/c") == "a_b_c"


def test_remove_path_unsafe_characters_no_slashes() -> None:
    assert _remove_path_unsafe_characters("hello world") == "hello world"


# --- _format_index ---


def test_format_index_single_digit() -> None:
    assert _format_index(3, 9) == "3"


def test_format_index_zero_padding_two_digits() -> None:
    assert _format_index(3, 99) == "03"


def test_format_index_zero_padding_three_digits() -> None:
    assert _format_index(3, 999) == "003"


def test_format_index_max_value() -> None:
    assert _format_index(99, 99) == "99"


def test_format_index_zero() -> None:
    assert _format_index(0, 9) == "0"


# --- _row_name_for_path ---


def test_row_name_for_path_normal() -> None:
    row = make_row(index=3, id="r-1", name="My Row")
    result = _row_name_for_path(row, 10)
    assert result == "03 - r-1 - My Row"


def test_row_name_for_path_long_name() -> None:
    row = make_row(index=0, id="r-1", name="x" * 150)
    result = _row_name_for_path(row, 1)
    assert "ROWNAME_TOO_LONG" in result


def test_row_name_for_path_exactly_100_chars() -> None:
    name_100 = "a" * 100
    row = make_row(index=0, id="r-1", name=name_100)
    result = _row_name_for_path(row, 1)
    assert name_100 in result


def test_row_name_for_path_101_chars() -> None:
    row = make_row(index=0, id="r-1", name="a" * 101)
    result = _row_name_for_path(row, 1)
    assert "ROWNAME_TOO_LONG" in result


def test_row_name_for_path_with_slash() -> None:
    row = make_row(index=0, id="r-1", name="a/b")
    result = _row_name_for_path(row, 1)
    assert "/" not in result
    assert "a_b" in result


# --- _column_name_for_path ---


def test_column_name_for_path_format() -> None:
    col = ColumnAPI(make_column_api_data(id="c-1", name="Amount"))
    result = _column_name_for_path(0, col, 5)
    assert result == "0 - c-1 - Amount"


def test_column_name_for_path_with_slash() -> None:
    col = ColumnAPI(make_column_api_data(id="c-1", name="A/B"))
    result = _column_name_for_path(0, col, 1)
    assert "/" not in result
    assert "A_B" in result


# --- _doc_path ---


def _make_mock_doc(*, id: str = "d-1", name: str = "Doc", folder_id: str = "f-1", folder_name: str | None = "Folder") -> MagicMock:
    doc = MagicMock()
    doc.id.return_value = id
    doc.name.return_value = name
    doc.folder_id.return_value = folder_id
    doc.folder_name.return_value = folder_name
    return doc


def test_doc_path_normal() -> None:
    doc = _make_mock_doc(id="d-1", name="MyDoc", folder_id="f-1", folder_name="MyFolder")
    result = _doc_path(Path("/root"), doc)
    assert result == Path("/root/MyFolder f-1/MyDoc d-1")


def test_doc_path_no_folder_name() -> None:
    doc = _make_mock_doc(folder_name=None)
    result = _doc_path(Path("/root"), doc)
    assert "NO_FOLDER_NAME" in str(result)


def test_doc_path_with_slashes() -> None:
    doc = _make_mock_doc(name="A/B", folder_name="C/D")
    result = _doc_path(Path("/root"), doc)
    parts_str = str(result)
    # Slashes in names should be replaced with underscores (only path-component slashes remain)
    assert "A_B" in parts_str
    assert "C_D" in parts_str


# --- _table_path ---


def _make_mock_table(*, id: str = "t-1", name: str = "Table", table_type: TableType = TableType.table) -> MagicMock:
    table = MagicMock()
    table.id.return_value = id
    table.name.return_value = name
    table.type.return_value = table_type
    return table


def test_table_path_table_type() -> None:
    table = _make_mock_table(id="t-1", name="Tasks", table_type=TableType.table)
    result = _table_path(Path("/doc"), table)
    assert result == Path("/doc/tables/table/Tasks t-1")


def test_table_path_view_type() -> None:
    table = _make_mock_table(id="t-1", name="View1", table_type=TableType.view)
    result = _table_path(Path("/doc"), table)
    assert result == Path("/doc/tables/view/View1 t-1")
