import os
from typing import Optional, Dict, final, Final
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


@final
class ProgressHandler:
    def __init__(self, num_tables: int, progress_display: Optional[ProgressDisplay]):
        self._progress_load_export = None
        self._progress_load_table = None
        self._progress_compatibility_check = None
        self._progress_list_rows = None
        self._progress_delete_rows_issued = None
        self._progress_delete_rows_complete = None
        self._progress_reimport_issued = None
        self._progress_reimport_complete = None
        if progress_display is not None:
            self._progress_load_export = progress_display.add_task("Loading Tables from Export", total=num_tables)
            self._progress_load_table = progress_display.add_task("Loading Tables from coda.io", total=num_tables)
            self._progress_compatibility_check = progress_display.add_task("Comparing Schemas", total=num_tables)
            self._progress_list_rows = progress_display.add_task("Listing Rows from coda.io", total=num_tables)
            self._progress_delete_rows_issued = progress_display.add_task("Deleting Rows from coda.io (issued)", total=num_tables)
            self._progress_delete_rows_complete = progress_display.add_task("Deleting Rows from coda.io (completed)", total=num_tables)
            self._progress_reimport_issued = progress_display.add_task("Reimporting Rows from Export to coda.io (issued)", total=num_tables)
            self._progress_reimport_complete = progress_display.add_task("Reimporting Rows from Export to coda.io (completed)", total=num_tables)

    def increment_load_export(self) -> None:
        if self._progress_load_export is not None:
            self._progress_load_export.increment_progress()

    def increment_load_table(self) -> None:
        if self._progress_load_table is not None:
            self._progress_load_table.increment_progress()
    
    def increment_compatibility_check(self) -> None:
        if self._progress_compatibility_check is not None:
            self._progress_compatibility_check.increment_progress()

    def increment_list_rows(self) -> None:
        if self._progress_list_rows is not None:
            self._progress_list_rows.increment_progress()

    def increment_delete_rows_issued(self) -> None:
        if self._progress_delete_rows_issued is not None:
            self._progress_delete_rows_issued.increment_progress()

    def increment_delete_rows_complete(self) -> None:
        if self._progress_delete_rows_complete is not None:
            self._progress_delete_rows_complete.increment_progress()
    
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
        table_dirs = os.listdir(tables_path)
        progress_handler = ProgressHandler(len(table_dirs), progress_display)

        tables = await gather_cancel_on_first_error(*(_load_table(os.path.join(tables_path, table_dir, "table.json"), progress_handler) for table_dir in table_dirs))
        print("Reading tables from export...done")

        print("Importing tables to coda.io...")
        loaded_tables = await gather_cancel_on_first_error(*(_load_table_api_and_check_schema(doc, table, progress_handler) for table in tables))
        await gather_cancel_on_first_error(*(_reimport_table(table_api, table, progress_handler) for table, table_api in zip(tables, loaded_tables)))
        print("Importing tables to coda.io...done")

async def _load_table(path: str, progress_handler: ProgressHandler) -> Table:
    json = await _read_file(path)
    result = Table.from_json(json)
    progress_handler.increment_load_export()
    return result

async def _load_table_api_and_check_schema(doc: DocAPI, table: Table, progress_handler: ProgressHandler) -> TableAPI:
    table_api = await doc.get_table(table.id)
    progress_handler.increment_load_table()
    await _check_table_is_compatible(table_api, table)
    progress_handler.increment_compatibility_check()
    return table_api

async def _check_table_is_compatible(table_api: TableAPI, table: Table) -> None:
    if table_api.name() != table.name:
        raise Exception(f"Table {table.id}: Export states table name is {table.name} but server thinks it is {table_api.name}. Aborting this reimport just to be safe.")
    if table_api.type() != TableType.table:
        raise Exception(f"Table {table.name} {table.id}: Server type is {table_api.type()} but expected it to be 'table'")

    await _check_columns_are_compatible(table_api, table)

async def _check_columns_are_compatible(server_side_table: TableAPI, table: Table) -> None:
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


async def _reimport_table(table_api: TableAPI, table: Table, progress_handler: ProgressHandler) -> None:
    await _delete_all_rows(table_api, progress_handler)
    await _insert_rows(table_api, table, progress_handler)

async def _delete_all_rows(table_api: TableAPI, progress_handler: ProgressHandler) -> None:
    rows = await collect(table_api.get_all_rows())
    row_ids = [row.id() for row in rows]
    progress_handler.increment_list_rows()
    await table_api.delete_rows(row_ids, on_issued=progress_handler.increment_delete_rows_issued)
    progress_handler.increment_delete_rows_complete()
    
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
