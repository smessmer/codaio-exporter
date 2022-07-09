import os
from typing import Optional, List, Dict
import logging
from ensure import check  # type: ignore

from codaio_exporter.utils.generator import collect
from codaio_exporter.api import make_api
from codaio_exporter.api.doc import DocAPI
from codaio_exporter.api.table import TableAPI, TableType
from codaio_exporter.progress import ProgressCallback
from codaio_exporter.table import Table, Row
from codaio_exporter.utils.gather import gather_cancel_on_first_error, gather_raise_first_error_after_all_tasks_complete


class ProgressHandler:
    def __init__(self, num_tables: int, callback: Optional[ProgressCallback]):
        self._total = num_tables
        self._progress_load_table = 0
        self._progress_compatibility_check = 0
        self._progress_cleared_table = 0
        self._progress_reimported = 0
        self._callback = callback
        self._call()
    
    def increment_load_table(self) -> None:
        self._progress_load_table += 1
        self._call()
    
    def increment_compatibility_check(self) -> None:
        self._progress_compatibility_check += 1
        self._call()
    
    def increment_cleared_table(self) -> None:
        self._progress_cleared_table += 1
        self._call()
    
    def increment_reimported(self) -> None:
        self._progress_reimported += 1
        self._call()

    def _call(self) -> None:
        if self._callback is not None:
            self._callback("A: Load Table", self._progress_load_table, self._total)
            self._callback("B: Compatibility check", self._progress_compatibility_check, self._total)
            self._callback("C: Cleared table", self._progress_cleared_table, self._total)
            self._callback("D: Finished reimport", self._progress_reimported, self._total)


async def reimport_doc(api_token: str, source_path: str, dest_doc_id: str, progress_callback: Optional[ProgressCallback] = None) -> None:
    async with make_api(api_token) as api:
        doc = await api.get_doc(dest_doc_id)

        print("Reading tables from export...")
        tables_path = os.path.join(source_path, "tables/table")
        tables = []
        for table_dir in os.listdir(tables_path):
            with open(os.path.join(tables_path, table_dir, "table.json"), 'r') as file:
                json = file.read()
                tables.append(Table.from_json(json))
        print("Reading tables from export...done")

        progress_handler = ProgressHandler(len(tables), progress_callback)

        print("Importing tables to coda.io...")
        await gather_cancel_on_first_error(*(_check_table_is_compatible(doc, table, progress_handler) for table in tables))
        await gather_cancel_on_first_error(*(_reimport_table(doc, table, progress_handler) for table in tables))
        print("Importing tables to coda.io...done")

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
    await _delete_all_rows(table_api)
    progress_handler.increment_cleared_table()
    await _insert_rows(table_api, table)
    progress_handler.increment_reimported()

async def _delete_all_rows(table_api: TableAPI) -> None:
    rows = await collect(table_api.get_all_rows())
    row_ids = [row.id() for row in rows]
    await table_api.delete_rows(row_ids)
    
async def _insert_rows(table_api: TableAPI, table: Table) -> None:
    def format_row(row: Row) -> Dict[str, str]:
        check(len(table.columns)).equals(len(row.cells)).or_raise(lambda _: Exception(f"Table {table.name} {table.id}: Export has {len(table.columns)} columns but a row in the export has {len(row.cells)} columns"))
        result = {}
        for i in range(len(row.cells)):
            if table.columns[i].formula is None:
                result[table.columns[i].id] = row.cells[i]
        return result
    cells = [format_row(row) for row in table.rows]
    await table_api.insert_rows(cells)
