from typing import Dict, Any, Optional, final, Final
from codaio_exporter.api.parse import parse_str, parse_bool
from ensure import check  # type: ignore


@final
class ColumnAPI:
    def __init__(self, data: Dict[str, Any]):
        self._data: Final = data

    def id(self) -> str:
        return parse_str(self._data["id"])

    def raw_data(self) -> Dict[str, Any]:
        return self._data

    def name(self) -> str:
        return parse_str(self._data["name"])
    
    def calculated(self) -> bool:
        return "calculated" in self._data and parse_bool(self._data["calculated"])

    def formula(self) -> Optional[str]:
        if "formula" in self._data:
            return parse_str(self._data["formula"])
        else:
            return None
