from typing import Any, Dict
from ensure import check  # type: ignore

def parse_int(v: Any) -> int:
    check(v).is_a(int).or_raise(
        lambda _: Exception(f"Tried to read {v} as int"))
    assert isinstance(v, int)
    return v

def parse_str(v: Any) -> str:
    check(v).is_a(str).or_raise(
        lambda _: Exception(f"Tried to read {v} as str"))
    assert isinstance(v, str)
    return v

def parse_dict_str_any(v: Any) -> Dict[str, Any]:
    check(v).is_a(Dict).or_raise(
        lambda _: Exception(f"Tried to read {v} as Dict"))
    assert isinstance(v, Dict)
    for key in v.keys():
        parse_str(key)
    return v
