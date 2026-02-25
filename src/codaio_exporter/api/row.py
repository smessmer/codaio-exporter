from typing import Any, Final, final

from codaio_exporter.api.parse import parse_int, parse_str


@final
class RowAPI:
    def __init__(self, data: dict[str, Any]):
        self._data: Final = data

    def raw_data(self) -> dict[str, Any]:
        return self._data

    def id(self) -> str:
        return parse_str(self._data["id"])

    def name(self) -> str:
        return parse_str(self._data["name"])

    def index(self) -> int:
        return parse_int(self._data["index"])

    def num_cells(self) -> int:
        return len(self._data["values"])

    def get_cell_value(self, column_id: str) -> str:
        return str(self._data["values"][column_id])
