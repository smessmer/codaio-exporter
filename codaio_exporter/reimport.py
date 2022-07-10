import os
from typing import Optional, List, Dict
from ensure import check  # type: ignore
import aiofiles
import asyncio

from codaio_exporter.utils.generator import collect
from codaio_exporter.api import make_api
from codaio_exporter.api.doc import DocAPI
from codaio_exporter.api.table import TableAPI, TableType
from codaio_exporter.progress import ProgressDisplay
from codaio_exporter.table import Table, Row
from codaio_exporter.utils.gather import gather_cancel_on_first_error, gather_raise_first_error_after_all_tasks_complete


class ProgressHandler:
    def __init__(self, num_tables: int, progress_display: Optional[ProgressDisplay]):
        self._progress_load_table = None
        self._progress_compatibility_check = None
        self._progress_clear_issued = None
        self._progress_clear_complete = None
        self._progress_reimport_issued = None
        self._progress_reimport_complete = None
        if progress_display is not None:
            self._progress_load_table = progress_display.add_task("Loading Tables", total=num_tables)
            self._progress_compatibility_check = progress_display.add_task("Checking Schemas", total=num_tables)
            self._progress_clear_issued = progress_display.add_task("Clearing Tables (issued)", total=num_tables)
            self._progress_clear_complete = progress_display.add_task("Clearing Tables (completed)", total=num_tables)
            self._progress_reimport_issued = progress_display.add_task("Reimporting Tables (issued)", total=num_tables)
            self._progress_reimport_complete = progress_display.add_task("Reimporting Tables (completed)", total=num_tables)
    
    def increment_load_table(self) -> None:
        if self._progress_load_table is not None:
            self._progress_load_table.increment_progress()
    
    def increment_compatibility_check(self) -> None:
        if self._progress_compatibility_check is not None:
            self._progress_compatibility_check.increment_progress()
    
    def increment_clear_issued(self) -> None:
        if self._progress_clear_issued is not None:
            self._progress_clear_issued.increment_progress()

    def increment_clear_complete(self) -> None:
        if self._progress_clear_complete is not None:
            self._progress_clear_complete.increment_progress()
    
    def increment_reimport_issued(self) -> None:
        if self._progress_reimport_issued is not None:
            self._progress_reimport_issued.increment_progress()

    def increment_reimport_complete(self) -> None:
        if self._progress_reimport_complete is not None:
            self._progress_reimport_complete.increment_progress()


async def reimport_doc(api_token: str, source_path: str, dest_doc_id: str, progress_display: Optional[ProgressDisplay] = None) -> None:
    async with make_api(api_token) as api:
        doc = await api.get_doc(dest_doc_id)

        print("Reading tables from export...")
        tables_path = os.path.join(source_path, "tables/table")
        tables: List[Table] = await gather_cancel_on_first_error(*(_load_table(os.path.join(tables_path, table_dir, "table.json")) for table_dir in os.listdir(tables_path)))
        print("Reading tables from export...done")

        progress_handler = ProgressHandler(len(tables), progress_display)

        print("Importing tables to coda.io...")
        await gather_cancel_on_first_error(*(_check_table_is_compatible(doc, table, progress_handler) for table in tables))
        await gather_cancel_on_first_error(*(_reimport_table(doc, table, progress_handler) for table in tables))
        print("Importing tables to coda.io...done")

async def _load_table(path: str) -> Table:
    json = await _read_file(path)
    return Table.from_json(json)

async def _check_table_is_compatible(doc: DocAPI, table: Table, progress_handler: ProgressHandler) -> None:
    table_api = await doc.get_table(table.id)
    if table_api.name() != table.name:
        raise Exception(f"Table {table.id}: Export states table name is {table.name} but server thinks it is {table_api.name}. Aborting this reimport just to be safe.")
    if table_api.type() != TableType.table:
        raise Exception(f"Table {table.name} {table.id}: Server type is {table_api.type()} but expected it to be 'table'")
    progress_handler.increment_load_table()

    await _check_columns_are_compatible(table_api, table, progress_handler)
    progress_handler.increment_compatibility_check()

async def _check_columns_are_compatible(server_side_table: TableAPI, table: Table, progress_handler: ProgressHandler) -> None:
    columns_api = await collect(server_side_table.get_all_columns())
    columns_api_by_id = {column.id(): column for column in columns_api}
    for column in table.columns:
        if column.id not in columns_api_by_id:
            raise Exception(f"Table {table.name} {table.id}: Column {column.name} {column.id} found in export but not on server")
        server_column = columns_api_by_id[column.id]
        if column.name != server_column.name():
            raise Exception(f"Table {table.name} {table.id}: Column {column.id}: Export states column name is {column.name} but server thinks it is {server_side_table.name}. Aborting this reimport just to be safe.")
        if (not column.calculated) and server_column.calculated():
            raise Exception(f"Table {table.name} {table.id}: Column {column.name} {column.id}: Export states column is a manual column but server states it is a calculated column")
        if column.calculated and not server_column.calculated():
            raise Exception(f"Table {table.name} {table.id}: Column {column.name} {column.id}: Export states column is a calculated column but server states it is a manual column")


async def _reimport_table(doc: DocAPI, table: Table, progress_handler: ProgressHandler) -> None:
    # TODO We run doc.get_table here again after we already ran it in _check_columns_are_compatible. This can be optimized
    table_api = await doc.get_table(table.id)
    await _delete_all_rows(table_api, progress_handler)
    await _insert_rows(table_api, table, progress_handler)

async def _delete_all_rows(table_api: TableAPI, progress_handler: ProgressHandler) -> None:
    rows = await collect(table_api.get_all_rows())
    row_ids = [row.id() for row in rows]
    await table_api.delete_rows(row_ids, on_issued=progress_handler.increment_clear_issued)
    progress_handler.increment_clear_complete()
    
async def _insert_rows(table_api: TableAPI, table: Table, progress_handler: ProgressHandler) -> None:
    def format_row(row: Row) -> Dict[str, str]:
        check(len(table.columns)).equals(len(row.cells)).or_raise(lambda _: Exception(f"Table {table.name} {table.id}: Export has {len(table.columns)} columns but a row in the export has {len(row.cells)} columns"))
        result = {}
        for i in range(len(row.cells)):
            if table.columns[i].formula is None:
                result[table.columns[i].id] = row.cells[i]
        return result
    cells = [format_row(row) for row in table.rows]
    await table_api.insert_rows(cells, on_issued=progress_handler.increment_reimport_issued)
    progress_handler.increment_reimport_complete()


# Semaphore to make sure we don't get 'too many open files'
read_semaphore = asyncio.Semaphore(512)

async def _read_file(path: str) -> str:
    async with read_semaphore:
        async with aiofiles.open(path, 'r') as file:
            return await file.read()
