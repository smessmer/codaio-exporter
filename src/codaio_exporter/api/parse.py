from typing import Any

from ensure import check  # type: ignore[import-untyped]


def parse_int(v: Any) -> int:
    check(v).is_a(int).or_raise(lambda _: Exception(f"Tried to read {v} as int"))  # pyright: ignore[reportUnknownMemberType, reportUnknownLambdaType]
    assert isinstance(v, int)
    return v


def parse_str(v: Any) -> str:
    check(v).is_a(str).or_raise(lambda _: Exception(f"Tried to read {v} as str"))  # pyright: ignore[reportUnknownMemberType, reportUnknownLambdaType]
    assert isinstance(v, str)
    return v


def parse_bool(v: Any) -> bool:
    check(v).is_a(bool).or_raise(lambda _: Exception(f"Tried to read {v} as bool"))  # pyright: ignore[reportUnknownMemberType, reportUnknownLambdaType]
    assert isinstance(v, bool)
    return v


def parse_dict_str_any(v: Any) -> dict[str, Any]:
    check(v).is_a(dict).or_raise(lambda _: Exception(f"Tried to read {v} as Dict"))  # pyright: ignore[reportUnknownMemberType, reportUnknownLambdaType]
    assert isinstance(v, dict)
    for key in v:  # pyright: ignore[reportUnknownVariableType]
        parse_str(key)
    return v  # pyright: ignore[reportUnknownVariableType]
