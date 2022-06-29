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
from codaio_exporter.progress import ProgressCallback


class ProgressHandler:
    def __init__(self, name: str, callback: Optional[ProgressCallback]):
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
        # We could use concurrent_async_for for more concurrency (i.e. already downloading data for the first document while we're still finding new documents)
        # but that looks a bit weird in the UI since the `max_progress` property of the progress bar would keep increasing. So instead, let's enumerate
        # all documents first and then start querying the data.
        documents = await collect(api.get_all_docs())
        await gather_raise_first_error_after_all_tasks_complete(*(_export_doc(dest_path, doc, progress_callback) for doc in documents))

async def export_doc(api_token: str, dest_path: str, doc_id: str, progress_callback: Optional[ProgressCallback] = None) -> None:
    os.makedirs(dest_path, exist_ok=False)
    async with make_api(api_token) as api:
        doc = await api.get_doc(doc_id)
        await _export_doc(dest_path, doc, progress_callback)

async def _export_doc(dest_path: str, doc: DocAPI, progress_callback: Optional[ProgressCallback]) -> None:
    doc_path = _doc_path(dest_path, doc)
    os.makedirs(doc_path, exist_ok=False)
    with open(os.path.join(doc_path, "api_object.json"), 'w') as file:
        file.write(str(doc.raw_data()))

    progress_handler = ProgressHandler(doc.name(), progress_callback)
    # We could use concurrent_async_for for more concurrency (i.e. already downloading data for the first tables while we're still finding new tables)
    # but that looks a bit weird in the UI since the `max_progress` property of the progress bar would keep increasing. So instead, let's enumerate
    # all tables first and then start querying the data.
    tables = await collect(doc.get_all_tables(), lambda: progress_handler.increment_total())
    await gather_raise_first_error_after_all_tasks_complete(*(_export_table(doc_path, table, progress_handler) for table in tables))

async def _export_table(doc_path: str, table: TableAPI, progress_handler: ProgressHandler) -> None:
    table_path = _table_path(doc_path, table)
    os.makedirs(table_path, exist_ok=False)
    with open(os.path.join(table_path, "api_object.json"), 'w') as file:
        file.write(str(table.raw_data()))
    columns, rows = await asyncio.gather(
        collect(table.get_all_columns()),
        collect(table.get_all_rows())
    )
    _export_columns(table_path, columns)
    _export_rows(table_path, table, columns, rows)
    progress_handler.increment_done()

def _export_columns(table_path: str, columns: List[ColumnAPI]) -> None:
    columns_path = os.path.join(table_path, "columns")
    os.makedirs(columns_path, exist_ok=False)
    for column in columns:
        with open(os.path.join(columns_path, _column_name_for_path(column) + ".json"), 'w') as file:
            file.write(str(column.raw_data()))

def _export_rows(table_path: str, table_api: TableAPI, columns: List[ColumnAPI], rows: List[RowAPI]) -> None:
    table = parse_table_from_api(table_api.id(), columns, rows)
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
