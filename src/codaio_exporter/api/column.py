from __future__ import annotations

from typing import Any, Final, final

from codaio_exporter.api.parse import parse_bool, parse_str


@final
class ColumnAPI:
    def __init__(self, data: dict[str, Any]):
        self._data: Final = data

    def id(self) -> str:
        return parse_str(self._data["id"])

    def raw_data(self) -> dict[str, Any]:
        return self._data

    def name(self) -> str:
        return parse_str(self._data["name"])

    def calculated(self) -> bool:
        return "calculated" in self._data and parse_bool(self._data["calculated"])

    def formula(self) -> str | None:
        if "formula" in self._data:
            return parse_str(self._data["formula"])
        else:
            return None
