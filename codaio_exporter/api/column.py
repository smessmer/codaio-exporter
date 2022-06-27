from typing import Dict, Any, Optional
from codaio_exporter.api.parse import parse_str

class Column:
    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def id(self) -> str:
        return parse_str(self._data["id"])

    def raw_data(self) -> Dict[str, Any]:
        return self._data

    def name(self) -> str:
        return parse_str(self._data["name"])

    def formula(self) -> Optional[str]:
        if "formula" in self._data:
            return parse_str(self._data["formula"])
        else:
            return None
