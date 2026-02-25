from __future__ import annotations

import pytest

from codaio_exporter.api.parse import parse_bool, parse_dict_str_any, parse_int, parse_str


def test_parse_int_with_int() -> None:
    assert parse_int(42) == 42


def test_parse_int_with_zero() -> None:
    assert parse_int(0) == 0


def test_parse_int_with_negative() -> None:
    assert parse_int(-5) == -5


def test_parse_int_with_bool_accepts() -> None:
    # In Python, isinstance(True, int) is True, so parse_int accepts bools
    assert parse_int(True) is True


def test_parse_int_with_string_raises() -> None:
    with pytest.raises(Exception, match="as int"):
        parse_int("42")


def test_parse_int_with_float_raises() -> None:
    with pytest.raises(Exception, match="as int"):
        parse_int(3.14)


def test_parse_int_with_none_raises() -> None:
    with pytest.raises(Exception, match="as int"):
        parse_int(None)


def test_parse_str_with_str() -> None:
    assert parse_str("hello") == "hello"


def test_parse_str_with_empty_str() -> None:
    assert parse_str("") == ""


def test_parse_str_with_int_raises() -> None:
    with pytest.raises(Exception, match="as str"):
        parse_str(42)


def test_parse_str_with_none_raises() -> None:
    with pytest.raises(Exception, match="as str"):
        parse_str(None)


def test_parse_bool_with_true() -> None:
    assert parse_bool(True) is True


def test_parse_bool_with_false() -> None:
    assert parse_bool(False) is False


def test_parse_bool_with_int_raises() -> None:
    # In Python, isinstance(1, bool) is False, so parse_bool rejects ints
    with pytest.raises(Exception, match="as bool"):
        parse_bool(1)


def test_parse_bool_with_string_raises() -> None:
    with pytest.raises(Exception, match="as bool"):
        parse_bool("true")


def test_parse_bool_with_none_raises() -> None:
    with pytest.raises(Exception, match="as bool"):
        parse_bool(None)


def test_parse_dict_str_any_with_valid_dict() -> None:
    d = {"a": 1, "b": "x"}
    assert parse_dict_str_any(d) == {"a": 1, "b": "x"}


def test_parse_dict_str_any_with_empty_dict() -> None:
    assert parse_dict_str_any({}) == {}


def test_parse_dict_str_any_with_non_dict_raises() -> None:
    with pytest.raises(Exception, match="as Dict"):
        parse_dict_str_any([1, 2])


def test_parse_dict_str_any_with_none_raises() -> None:
    with pytest.raises(Exception, match="as Dict"):
        parse_dict_str_any(None)


def test_parse_dict_str_any_with_non_string_key_raises() -> None:
    with pytest.raises(Exception, match="as str"):
        parse_dict_str_any({1: "val"})
