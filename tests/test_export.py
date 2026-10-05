from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from codaio_exporter.api.column import ColumnAPI
from codaio_exporter.api.doc import DocAPI
from codaio_exporter.api.table import TableType
from codaio_exporter.export import (
    _column_name_for_path,  # pyright: ignore[reportPrivateUsage]
    _doc_path,  # pyright: ignore[reportPrivateUsage]
    _export_doc,  # pyright: ignore[reportPrivateUsage]
    _format_index,  # pyright: ignore[reportPrivateUsage]
    _remove_path_unsafe_characters,  # pyright: ignore[reportPrivateUsage]
    _row_name_for_path,  # pyright: ignore[reportPrivateUsage]
    _table_path,  # pyright: ignore[reportPrivateUsage]
)

from .conftest import make_column_api_data, make_row, run_python_with_ascii_locale

# --- _remove_path_unsafe_characters ---


def test_remove_path_unsafe_characters_with_slashes() -> None:
    assert _remove_path_unsafe_characters("a/b/c") == "a_b_c"


def test_remove_path_unsafe_characters_no_slashes() -> None:
    assert _remove_path_unsafe_characters("hello world") == "hello world"


def _use_file_system_encoding(monkeypatch: pytest.MonkeyPatch, encoding: str, errors: str) -> None:
    """Makes the export see the given file system encoding and error handler."""
    monkeypatch.setattr("sys.getfilesystemencoding", lambda: encoding)
    monkeypatch.setattr("sys.getfilesystemencodeerrors", lambda: errors)


@pytest.mark.parametrize(
    ("encoding", "errors", "name", "expected"),
    [
        # The C/POSIX locale without UTF-8 mode
        pytest.param("ascii", "surrogateescape", "Café ☕ Plan", "Caf_ _ Plan", id="ascii"),
        # e.g. the de_DE.ISO-8859-1 locale
        pytest.param("latin-1", "surrogateescape", "Café ☕ Plan", "Café _ Plan", id="latin-1"),
        pytest.param("latin-1", "surrogateescape", "Größe/naïve 日本", "Größe_naïve __", id="latin-1-with-slash"),
        # "e" and a combining acute accent (NFD), which latin-1 can only encode as the single character "é" (NFC). NFKC would also change "½".
        pytest.param("latin-1", "surrogateescape", "Cafe\u0301 ½", "Caf\u00e9 ½", id="latin-1-decomposed"),
        # EUC-KR (the ko_KR.EUC-KR locale) can encode U+F92C (a Korean hanja), but not U+90CE, its NFC form
        pytest.param("euc_kr", "surrogateescape", "\uf92c ☕", "\uf92c _", id="euc_kr-compatibility-ideograph"),
        # big5hkscs (the zh_HK locale) can encode U+00CA U+0304 ("Ê" and a combining macron) as a pair, but not U+0304 on its own
        pytest.param("big5hkscs", "surrogateescape", "\u00ca\u0304", "\u00ca\u0304", id="big5hkscs-character-sequence"),
        # e.g. a UTF-8 locale, also with "e" and a combining acute accent (NFD)
        pytest.param("utf-8", "surrogateescape", "Cafe\u0301 ☕ Plan naïve 日本", "Cafe\u0301 ☕ Plan naïve 日本", id="utf-8"),
        # "surrogateescape" encodes the lone surrogates that stand for undecodable bytes (U+DC80 to U+DCFF), but no other ones
        pytest.param("utf-8", "surrogateescape", "a\udce9b\ud800c", "a\udce9b_c", id="utf-8-lone-surrogates"),
        # Windows, where "surrogatepass" encodes all lone surrogates
        pytest.param("utf-8", "surrogatepass", "a\udce9b\ud800c", "a\udce9b\ud800c", id="utf-8-surrogatepass"),
    ],
)
def test_remove_path_unsafe_characters_replaces_unencodable_characters(
    monkeypatch: pytest.MonkeyPatch, encoding: str, errors: str, name: str, expected: str
) -> None:
    _use_file_system_encoding(monkeypatch, encoding, errors)
    assert _remove_path_unsafe_characters(name) == expected


# --- _export_doc ---


async def test_export_doc_replaces_unencodable_characters_in_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # The C/POSIX locale without UTF-8 mode
    _use_file_system_encoding(monkeypatch, "ascii", "surrogateescape")
    items_by_endpoint: dict[str, list[dict[str, object]]] = {
        "/docs/d-1/tables": [{"id": "t-1", "name": "naïve 日本", "tableType": "table"}],
        # ASCII, because column names also end up in the contents of table.csv and table.html, which this test isn't about
        "/docs/d-1/tables/t-1/columns": [make_column_api_data(id="c-1", name="Name")],
        "/docs/d-1/tables/t-1/rows": [{"id": "r-1", "name": "Zeile Ä ☕", "index": 0, "values": {"c-1": "x"}}],
    }

    async def get_list(endpoint: str) -> AsyncIterator[dict[str, object]]:
        for item in items_by_endpoint[endpoint]:
            yield item

    client = MagicMock()
    client.get_list.side_effect = get_list
    doc = DocAPI(client, {"id": "d-1", "name": "Café ☕ Plan", "folder": {"id": "f-1", "name": "Größe"}})

    await _export_doc(tmp_path, doc, None)

    doc_path = "Gr__e f-1/Caf_ _ Plan d-1"
    table_path = f"{doc_path}/tables/table/na_ve __ t-1"
    assert {path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*")} == {
        "Gr__e f-1",
        doc_path,
        f"{doc_path}/api_object.json",
        f"{doc_path}/api_object.yaml",
        f"{doc_path}/tables",
        f"{doc_path}/tables/table",
        table_path,
        f"{table_path}/api_object.json",
        f"{table_path}/api_object.yaml",
        f"{table_path}/columns",
        f"{table_path}/columns/0 - c-1 - Name.json",
        f"{table_path}/columns/0 - c-1 - Name.yaml",
        f"{table_path}/rows",
        f"{table_path}/rows/0 - r-1 - Zeile _ _.json",
        f"{table_path}/rows/0 - r-1 - Zeile _ _.yaml",
        f"{table_path}/table.csv",
        f"{table_path}/table.html",
        f"{table_path}/table.json",
        f"{table_path}/table.yaml",
    }


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


# --- _write_file ---


def test_write_file_writes_utf8_with_ascii_locale(tmp_path: Path) -> None:
    # e.g. cell values in table.csv, which the encoding of the locale can't represent
    text = "Grüße ☕ 5 € 日本語 😀"

    run_python_with_ascii_locale(
        f"""
        import asyncio
        from pathlib import Path
        from codaio_exporter.export import _write_file
        asyncio.run(_write_file(Path("table.csv"), {text!a}))
        """,
        cwd=tmp_path,
    )

    assert (tmp_path / "table.csv").read_bytes() == text.encode("utf-8")


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


def test_column_name_for_path_replaces_unencodable_characters(monkeypatch: pytest.MonkeyPatch) -> None:
    # The C/POSIX locale without UTF-8 mode
    _use_file_system_encoding(monkeypatch, "ascii", "surrogateescape")
    col = ColumnAPI(make_column_api_data(id="c-1", name="Größe ☕"))
    result = _column_name_for_path(0, col, 1)
    assert result == "0 - c-1 - Gr__e _"


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
