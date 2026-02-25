from __future__ import annotations

import pytest

from codaio_exporter.api.row import RowAPI

from .conftest import make_row_api_data


def test_row_id() -> None:
    row = RowAPI(make_row_api_data(id="r-1"))
    assert row.id() == "r-1"


def test_row_name() -> None:
    row = RowAPI(make_row_api_data(name="First Row"))
    assert row.name() == "First Row"


def test_row_index() -> None:
    row = RowAPI(make_row_api_data(index=5))
    assert row.index() == 5


def test_row_raw_data() -> None:
    data = make_row_api_data()
    row = RowAPI(data)
    assert row.raw_data() is data


def test_row_num_cells() -> None:
    row = RowAPI(make_row_api_data(values={"c1": "a", "c2": "b"}))
    assert row.num_cells() == 2


def test_row_num_cells_empty() -> None:
    row = RowAPI(make_row_api_data(values={}))
    assert row.num_cells() == 0


def test_row_get_cell_value_string() -> None:
    row = RowAPI(make_row_api_data(values={"c1": "hello"}))
    assert row.get_cell_value("c1") == "hello"


def test_row_get_cell_value_int() -> None:
    row = RowAPI(make_row_api_data(values={"c1": 42}))
    assert row.get_cell_value("c1") == "42"


def test_row_get_cell_value_none() -> None:
    row = RowAPI(make_row_api_data(values={"c1": None}))
    assert row.get_cell_value("c1") == "None"


def test_row_get_cell_value_list() -> None:
    row = RowAPI(make_row_api_data(values={"c1": [1, 2]}))
    assert row.get_cell_value("c1") == "[1, 2]"


def test_row_get_cell_value_missing_column_raises() -> None:
    row = RowAPI(make_row_api_data(values={"c1": "x"}))
    with pytest.raises(KeyError):
        row.get_cell_value("nonexistent")
