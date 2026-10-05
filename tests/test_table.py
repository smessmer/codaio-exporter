from __future__ import annotations

from xml.etree import ElementTree

import pytest

from codaio_exporter.api.column import ColumnAPI
from codaio_exporter.api.row import RowAPI
from codaio_exporter.errors import DataFormatError
from codaio_exporter.table import Column, Row, Table, parse_table_from_api

from .conftest import make_column, make_column_api_data, make_row, make_row_api_data, make_table

# --- Column dataclass ---


def test_column_fields() -> None:
    col = make_column(id="c1", name="Name", calculated=False, formula=None)
    assert col.id == "c1"
    assert col.name == "Name"
    assert col.calculated is False
    assert col.formula is None


def test_column_json_roundtrip() -> None:
    col = make_column(id="c1", name="Name", calculated=True, formula="=A+B")
    restored = Column.from_json(col.to_json())
    assert restored == col


# --- Row dataclass ---


def test_row_fields() -> None:
    row = make_row(index=3, id="r1", name="Row 1", cells=["a", "b"], raw_data={"key": "val"})
    assert row.index == 3
    assert row.id == "r1"
    assert row.name == "Row 1"
    assert row.cells == ["a", "b"]
    assert row.raw_data == {"key": "val"}


def test_row_json_roundtrip() -> None:
    row = make_row(index=0, id="r1", name="Row", cells=["x"], raw_data={"k": "v"})
    restored = Row.from_json(row.to_json())
    assert restored == row


# --- Table dataclass ---


def test_table_fields() -> None:
    col = make_column(id="c1", name="Col")
    row = make_row(index=0, id="r1", name="Row", cells=["val"])
    table = make_table(id="t1", name="T", columns=[col], rows=[row])
    assert table.id == "t1"
    assert table.name == "T"
    assert len(table.columns) == 1
    assert len(table.rows) == 1


def test_table_json_roundtrip() -> None:
    col = make_column(id="c1", name="Col", calculated=False, formula=None)
    row = make_row(index=0, id="r1", name="Row", cells=["val"], raw_data={"k": "v"})
    table = make_table(id="t1", name="T", columns=[col], rows=[row])
    restored = Table.from_json(table.to_json())
    assert restored == table


# --- to_csv ---


def test_table_to_csv_basic() -> None:
    col1 = make_column(id="c1", name="Name")
    col2 = make_column(id="c2", name="Value")
    row1 = make_row(cells=["Alice", "100"])
    row2 = make_row(cells=["Bob", "200"])
    table = make_table(columns=[col1, col2], rows=[row1, row2])

    csv_output = table.to_csv()
    lines = csv_output.strip().splitlines()
    assert lines[0] == '"Name","Value"'
    assert lines[1] == '"Alice","100"'
    assert lines[2] == '"Bob","200"'


def test_table_to_csv_empty_rows() -> None:
    col = make_column(id="c1", name="Header")
    table = make_table(columns=[col], rows=[])

    csv_output = table.to_csv()
    lines = csv_output.strip().splitlines()
    assert len(lines) == 1
    assert lines[0] == '"Header"'


def test_table_to_csv_special_characters() -> None:
    col = make_column(id="c1", name="Data")
    row = make_row(cells=['value with "quotes" and, comma'])
    table = make_table(columns=[col], rows=[row])

    csv_output = table.to_csv()
    # CSV should properly escape the content
    assert '"quotes"' in csv_output or '""quotes""' in csv_output


# --- to_html ---


def test_table_to_html_basic_structure() -> None:
    col = make_column(id="c1", name="Col1", formula=None)
    row = make_row(cells=["val1"])
    table = make_table(columns=[col], rows=[row])

    html = table.to_html()
    assert html.startswith("<html>")
    assert "<thead>" in html
    assert "<tbody>" in html
    assert "<td>val1</td>" in html


def test_table_to_html_declares_utf8_charset() -> None:
    # Export writes table.html as UTF-8
    col = make_column(id="c1", name="Größe")
    row = make_row(cells=["☕"])
    table = make_table(columns=[col], rows=[row])

    html = table.to_html()
    assert html.startswith('<html><head><meta charset="utf-8"/></head><body>')
    assert '<th title="no formula">Größe</th>' in html
    assert "<td>☕</td>" in html
    # Still well-formed XML, as without the declaration
    meta = ElementTree.fromstring(html).find("head/meta")
    assert meta is not None
    assert meta.attrib == {"charset": "utf-8"}


def test_table_to_html_escapes_cell_content() -> None:
    col = make_column(id="c1", name="Col")
    row = make_row(cells=["<script>alert('x')</script>"])
    table = make_table(columns=[col], rows=[row])

    html = table.to_html()
    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_table_to_html_escapes_column_name() -> None:
    col = make_column(id="c1", name="<b>Bold</b>")
    table = make_table(columns=[col], rows=[])

    html = table.to_html()
    assert "<b>Bold</b>" not in html
    assert "&lt;b&gt;Bold&lt;/b&gt;" in html


def test_table_to_html_formula_in_title() -> None:
    col = make_column(id="c1", name="Total", formula="=SUM(A)")
    table = make_table(columns=[col], rows=[])

    html = table.to_html()
    assert 'title="=SUM(A)"' in html


def test_table_to_html_no_formula() -> None:
    col = make_column(id="c1", name="Name", formula=None)
    table = make_table(columns=[col], rows=[])

    html = table.to_html()
    assert 'title="no formula"' in html


def test_table_to_html_formula_escaping() -> None:
    col = make_column(id="c1", name="Col", formula='=IF(A>"B",1,0)')
    table = make_table(columns=[col], rows=[])

    html = table.to_html()
    assert "&gt;" in html or "&quot;" in html


def test_table_to_html_empty_rows() -> None:
    col = make_column(id="c1", name="Col")
    table = make_table(columns=[col], rows=[])

    html = table.to_html()
    assert "<tbody></tbody>" in html


# --- parse_table_from_api ---


def test_parse_table_from_api_basic() -> None:
    cols = [
        ColumnAPI(make_column_api_data(id="c1", name="Name")),
        ColumnAPI(make_column_api_data(id="c2", name="Value")),
    ]
    rows = [
        RowAPI(make_row_api_data(id="r1", name="Row 1", index=0, values={"c1": "Alice", "c2": "100"})),
    ]

    table = parse_table_from_api("t1", "My Table", cols, rows)

    assert table.id == "t1"
    assert table.name == "My Table"
    assert len(table.columns) == 2
    assert len(table.rows) == 1
    assert table.rows[0].cells == ["Alice", "100"]


def test_parse_table_from_api_sorts_by_index() -> None:
    cols = [ColumnAPI(make_column_api_data(id="c1", name="Col"))]
    rows = [
        RowAPI(make_row_api_data(id="r2", name="Second", index=2, values={"c1": "b"})),
        RowAPI(make_row_api_data(id="r1", name="First", index=0, values={"c1": "a"})),
    ]

    table = parse_table_from_api("t1", "T", cols, rows)

    assert table.rows[0].index == 0
    assert table.rows[1].index == 2


def test_parse_table_from_api_wrong_cell_count_raises() -> None:
    cols = [ColumnAPI(make_column_api_data(id="c1", name="Col"))]
    rows = [
        RowAPI(make_row_api_data(id="r1", name="Row", index=0, values={"c1": "a", "c2": "b"})),
    ]

    with pytest.raises(DataFormatError, match="wrong number of cells"):
        parse_table_from_api("t1", "T", cols, rows)


def test_parse_table_from_api_empty_rows() -> None:
    cols = [ColumnAPI(make_column_api_data(id="c1", name="Col"))]

    table = parse_table_from_api("t1", "T", cols, [])

    assert table.rows == []
    assert len(table.columns) == 1


def test_parse_table_from_api_cell_values_stringified() -> None:
    cols = [ColumnAPI(make_column_api_data(id="c1", name="Col"))]
    rows = [
        RowAPI(make_row_api_data(id="r1", name="Row", index=0, values={"c1": 42})),
    ]

    table = parse_table_from_api("t1", "T", cols, rows)

    assert table.rows[0].cells == ["42"]
