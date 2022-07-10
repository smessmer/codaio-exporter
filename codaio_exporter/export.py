import os
import asyncio
from typing import List, Optional, final
import aiofiles

from codaio_exporter.utils.gather import gather_raise_first_error_after_all_tasks_complete
from codaio_exporter.utils.generator import collect, concurrent_async_for
from codaio_exporter.api import make_api
from codaio_exporter.api.doc import DocAPI
from codaio_exporter.api.table import TableAPI
from codaio_exporter.api.column import ColumnAPI
from codaio_exporter.api.row import RowAPI
from codaio_exporter.table import Table, parse_table_from_api
from codaio_exporter.progress import ProgressDisplay, ProgressBar


@final
class ProgressHandler:
    def __init__(self, name: str, progress_display: Optional[ProgressDisplay]):
        self._bar = None
        if progress_display is not None:
            self._bar = progress_display.add_task(name, total=None)
    
    def increment_done(self) -> None:
        if self._bar is not None:
            self._bar.increment_progress()
    
    def increment_total(self) -> None:
        if self._bar is not None:
            # No update=False because we don't want to keep the spinning animation instead of displaying the total until the first call of increment_done()
            self._bar.increment_total(update=False)


async def export_all_docs(api_token: str, dest_path: str, progress_display: Optional[ProgressDisplay] = None) -> None:
    os.makedirs(dest_path, exist_ok=False)
    async with make_api(api_token) as api:
        # We could use concurrent_async_for for more concurrency (i.e. already downloading data for the first document while we're still finding new documents)
        # but that looks a bit weird in the UI since the `max_progress` property of the progress bar would keep increasing. So instead, let's enumerate
        # all documents first and then start querying the data.
        documents = await collect(api.get_all_docs())
        await gather_raise_first_error_after_all_tasks_complete(*(_export_doc(dest_path, doc, progress_display) for doc in documents))

async def export_doc(api_token: str, dest_path: str, doc_id: str, progress_display: Optional[ProgressDisplay] = None) -> None:
    os.makedirs(dest_path, exist_ok=False)
    async with make_api(api_token) as api:
        doc = await api.get_doc(doc_id)
        await _export_doc(dest_path, doc, progress_display)

async def _export_doc(dest_path: str, doc: DocAPI, progress_display: Optional[ProgressDisplay]) -> None:
    doc_path = _doc_path(dest_path, doc)
    os.makedirs(doc_path, exist_ok=False)
    await _write_file(os.path.join(doc_path, "api_object.json"), str(doc.raw_data()))

    progress_handler = ProgressHandler(doc.name(), progress_display)
    # We could use concurrent_async_for for more concurrency (i.e. already downloading data for the first tables while we're still finding new tables)
    # but that looks a bit weird in the UI since the `max_progress` property of the progress bar would keep increasing. So instead, let's enumerate
    # all tables first and then start querying the data.
    tables = await collect(doc.get_all_tables(), lambda: progress_handler.increment_total())
    await gather_raise_first_error_after_all_tasks_complete(*(_export_table(doc_path, table, progress_handler) for table in tables))

async def _export_table(doc_path: str, table: TableAPI, progress_handler: ProgressHandler) -> None:
    # TODO We can improve concurrency in this function
    table_path = _table_path(doc_path, table)
    os.makedirs(table_path, exist_ok=False)
    await _write_file(os.path.join(table_path, "api_object.json"), str(table.raw_data()))
    columns, rows = await asyncio.gather(
        collect(table.get_all_columns()),
        collect(table.get_all_rows())
    )
    await gather_raise_first_error_after_all_tasks_complete(*(
        _export_columns(table_path, columns),
        _export_rows(table_path, table, columns, rows),
    ))
    progress_handler.increment_done()

async def _export_columns(table_path: str, columns: List[ColumnAPI]) -> None:
    columns_path = os.path.join(table_path, "columns")
    os.makedirs(columns_path, exist_ok=False)
    await gather_raise_first_error_after_all_tasks_complete(*(
        _write_file(os.path.join(columns_path, _column_name_for_path(column) + ".json"), str(column.raw_data()))
        for column in columns
    ))

async def _export_rows(table_path: str, table_api: TableAPI, columns: List[ColumnAPI], rows: List[RowAPI]) -> None:
    table = parse_table_from_api(table_api.id(), table_api.name(), columns, rows)
    table_csv = table.to_csv()
    table_html = table.to_html()
    table_json = table.to_json()
    await gather_raise_first_error_after_all_tasks_complete(*(
        _write_file(os.path.join(table_path, "table.csv"), table_csv),
        _write_file(os.path.join(table_path, "table.html"), table_html),
        _write_file(os.path.join(table_path, "table.json"), table_json),
    ))

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

# Semaphore to make sure we don't get 'too many open files'
write_semaphore = asyncio.Semaphore(512)

async def _write_file(path: str, content: str) -> None:
    async with write_semaphore:
        async with aiofiles.open(path, 'w') as file:
            await file.write(content)
