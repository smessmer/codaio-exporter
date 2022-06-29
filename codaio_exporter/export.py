import os
import asyncio
from typing import List, Optional, Callable

from codaio_exporter.utils.gather import gather_raise_first_error_after_all_tasks_complete
from codaio_exporter.utils.generator import collect, concurrent_async_for
from codaio_exporter.api import make_api
from codaio_exporter.api.doc import DocAPI
from codaio_exporter.api.table import TableAPI
from codaio_exporter.api.column import ColumnAPI
from codaio_exporter.api.row import RowAPI
from codaio_exporter.table import Table, parse_table_from_api


# Parameters: (doc_name, progress, progress_total)
ProgressCallback = Callable[[str, int, int], None]

class ProgressHandler:
    def __init__(self, name: str, total: int, callback: Optional[ProgressCallback]):
        self._name = name
        self._done = 0
        self._total = 0
        self._callback = callback
        self._call()
    
    def increment_done(self) -> None:
        self._done += 1
        self._call()
    
    def increment_total(self) -> None:
        self._total += 1
        self._call()
    
    def _call(self) -> None:
        if self._callback is not None:
            self._callback(self._name, self._done, self._total)


async def export_all_docs(api_token: str, dest_path: str, progress_callback: Optional[ProgressCallback] = None) -> None:
    os.makedirs(dest_path, exist_ok=False)
    async with make_api(api_token) as api:
        await concurrent_async_for(api.get_all_docs(), lambda doc: _export_doc(dest_path, doc, progress_callback))

async def _export_doc(dest_path: str, doc: DocAPI, progress_callback: Optional[ProgressCallback]) -> None:
    doc_path = _doc_path(dest_path, doc)
    os.makedirs(doc_path, exist_ok=False)
    with open(os.path.join(doc_path, "api_object.json"), 'w') as file:
        file.write(str(doc.raw_data()))

    num_tables = await doc.get_num_tables()
    progress_handler = ProgressHandler(doc.name(), num_tables, progress_callback)
    await concurrent_async_for(doc.get_all_tables(), lambda table: _export_table(doc_path, table, progress_handler))

async def _export_table(doc_path: str, table: TableAPI, progress_handler: ProgressHandler) -> None:
    progress_handler.increment_total()
    table_path = _table_path(doc_path, table)
    os.makedirs(table_path, exist_ok=False)
    with open(os.path.join(table_path, "api_object.json"), 'w') as file:
        file.write(str(table.raw_data()))
    columns, rows = await asyncio.gather(
        collect(table.get_all_columns()),
        collect(table.get_all_rows())
    )
    _export_columns(table_path, columns)
    _export_rows(table_path, columns, rows)
    progress_handler.increment_done()

def _export_columns(table_path: str, columns: List[ColumnAPI]) -> None:
    columns_path = os.path.join(table_path, "columns")
    os.makedirs(columns_path, exist_ok=False)
    for column in columns:
        with open(os.path.join(columns_path, _column_name_for_path(column) + ".json"), 'w') as file:
            file.write(str(column.raw_data()))

def _export_rows(table_path: str, columns: List[ColumnAPI], rows: List[RowAPI]) -> None:
    table = parse_table_from_api(columns, rows)
    table_csv = table.to_csv()
    table_html = table.to_html()
    table_json = table.to_json()
    with open(os.path.join(table_path, "table.csv"), 'w') as file:
        file.write(table_csv)
    with open(os.path.join(table_path, "table.html"), 'w') as file:
        file.write(table_html)
    with open(os.path.join(table_path, "table.json"), 'w') as file:
        file.write(table_json)

def _doc_path(root_path: str, doc: DocAPI) -> str:
    folder_name = _remove_path_unsafe_characters(doc.folder_name() + " " + doc.folder_id())
    doc_name = _remove_path_unsafe_characters(doc.name() + " " + doc.id())
    return os.path.join(root_path, folder_name, doc_name)

def _table_path(doc_path: str, table: TableAPI) -> str:
    table_name = _remove_path_unsafe_characters(table.name() + " " + table.id())
    return os.path.join(doc_path, "tables", table.type().to_str(), table_name)

def _column_name_for_path(column: ColumnAPI) -> str:
    return _remove_path_unsafe_characters(column.name() + " " + column.id())

def _remove_path_unsafe_characters(name: str) -> str:
    return name.replace('/', '_')
