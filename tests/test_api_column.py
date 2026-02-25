from __future__ import annotations

import pytest

from codaio_exporter.api.column import ColumnAPI

from .conftest import make_column_api_data


def test_column_id() -> None:
    col = ColumnAPI(make_column_api_data(id="c-1"))
    assert col.id() == "c-1"


def test_column_name() -> None:
    col = ColumnAPI(make_column_api_data(name="Amount"))
    assert col.name() == "Amount"


def test_column_raw_data() -> None:
    data = make_column_api_data()
    col = ColumnAPI(data)
    assert col.raw_data() is data


def test_column_calculated_true() -> None:
    col = ColumnAPI(make_column_api_data(calculated=True))
    assert col.calculated() is True


def test_column_calculated_false() -> None:
    col = ColumnAPI(make_column_api_data(calculated=False))
    assert col.calculated() is False


def test_column_calculated_missing() -> None:
    col = ColumnAPI(make_column_api_data())
    assert col.calculated() is False


def test_column_formula_present() -> None:
    col = ColumnAPI(make_column_api_data(formula="=A+B"))
    assert col.formula() == "=A+B"


def test_column_formula_missing() -> None:
    col = ColumnAPI(make_column_api_data())
    assert col.formula() is None


def test_column_id_wrong_type_raises() -> None:
    col = ColumnAPI({"id": 123, "name": "X"})
    with pytest.raises(Exception, match="as str"):
        col.id()
