from typing import Any, cast


def parse_int(v: Any) -> int:
    if not isinstance(v, int):
        raise Exception(f"Tried to read {v} as int")
    return v


def parse_str(v: Any) -> str:
    if not isinstance(v, str):
        raise Exception(f"Tried to read {v} as str")
    return v


def parse_bool(v: Any) -> bool:
    if not isinstance(v, bool):
        raise Exception(f"Tried to read {v} as bool")
    return v


def parse_dict_str_any(v: Any) -> dict[str, Any]:
    if not isinstance(v, dict):
        raise Exception(f"Tried to read {v} as Dict")
    d = cast(dict[str, Any], v)
    for key in d:
        parse_str(key)
    return d
