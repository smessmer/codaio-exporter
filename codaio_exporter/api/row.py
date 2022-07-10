from typing import Dict, Any, final, Final
from codaio_exporter.api.parse import parse_str, parse_int


@final
class RowAPI:
    def __init__(self, data: Dict[str, Any]):
        self._data: Final = data

    def id(self) -> str:
        return parse_str(self._data["id"])
    
    def index(self) -> int:
        return parse_int(self._data["index"])
    
    def num_cells(self) -> int:
        return len(self._data["values"])
    
    def get_cell_value(self, column_id: str) -> str:
        return str(self._data["values"][column_id])
