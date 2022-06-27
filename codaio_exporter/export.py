from ast import Num
import os
import csv
import html
import asyncio
from io import StringIO
from typing import List, Optional, Callable, Tuple

from codaio_exporter.utils.gather import gather_raise_first_error_after_all_tasks_complete
from codaio_exporter.utils.generator import collect, concurrent_async_for
from codaio_exporter.api import make_api
from codaio_exporter.api.doc import Doc
from codaio_exporter.api.table import Table
from codaio_exporter.api.column import Column
from codaio_exporter.api.row import Row


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

async def _export_doc(dest_path: str, doc: Doc, progress_callback: Optional[ProgressCallback]) -> None:
    doc_path = _doc_path(dest_path, doc)
    os.makedirs(doc_path, exist_ok=False)
    with open(os.path.join(doc_path, "doc.json"), 'w') as file:
        file.write(str(doc.raw_data()))

    num_tables = await doc.get_num_tables()
    progress_handler = ProgressHandler(doc.name(), num_tables, progress_callback)
    await concurrent_async_for(doc.get_all_tables(), lambda table: _export_table(doc_path, table, progress_handler))

async def _export_table(doc_path: str, table: Table, progress_handler: ProgressHandler) -> None:
    progress_handler.increment_total()
    table_path = _table_path(doc_path, table)
    os.makedirs(table_path, exist_ok=False)
    with open(os.path.join(table_path, "table.json"), 'w') as file:
        file.write(str(table.raw_data()))
    columns, rows = await asyncio.gather(
        collect(table.get_all_columns()),
        collect(table.get_all_rows())
    )
    _export_columns(table_path, columns)
    _export_rows(table_path, columns, rows)
    progress_handler.increment_done()

def _export_columns(table_path: str, columns: List[Column]) -> None:
    columns_path = os.path.join(table_path, "columns")
    os.makedirs(columns_path, exist_ok=False)
    for column in columns:
        with open(os.path.join(columns_path, _column_name_for_path(column) + ".json"), 'w') as file:
            file.write(str(column.raw_data()))

def _export_rows(table_path: str, columns: List[Column], rows: List[Row]) -> None:
    values = _parse_rows(columns, rows)
    table_csv = _make_csv(columns, values)
    with open(os.path.join(table_path, "values.csv"), 'w') as file:
        file.write(table_csv)
    table_html = _make_html(columns, values)
    with open(os.path.join(table_path, "values.html"), 'w') as file:
        file.write(table_html)

def _parse_rows(columns: List[Column], rows: List[Row]) -> List[List[str]]:
    rows.sort(key=lambda row: row.index())
    return [_parse_row(columns, row) for row in rows]

def _parse_row(columns: List[Column], row: Row) -> List[str]:
    if row.num_cells() != len(columns):
        raise Exception(f"_Row {row.id()} has wrong number of cells. Expected {len(columns)} columns but found {row.num_cells()}")
    return [row.get_cell_value(column.id()) for column in columns]

def _make_csv(columns: List[Column], values: List[List[str]]) -> str:
    output = StringIO()
    wr = csv.writer(output, quoting=csv.QUOTE_ALL)
    wr.writerow([column.name() for column in columns])
    wr.writerows(values)
    return output.getvalue()

def _make_html(columns: List[Column], values: List[List[str]]) -> str:
    column_headers_html = "".join(_make_html_column_header(column) for column in columns)
    rows = [
        "".join([f"<td>{html.escape(str(cell))}</td>" for cell in row]) for row in values
    ]
    rows_html = "".join(f"<tr>{row}</tr>" for row in rows)
    table_html =  f"<table><thead><tr>{column_headers_html}</tr></thead><tbody>{rows_html}</tbody></table>" 
    return f"<html><head/><body>{table_html}</body></html>"

def _make_html_column_header(column: Column) -> str:
    formula = column.formula()
    if formula is None:
        formula = "no formula"
    return f"<th title=\"{html.escape(formula)}\">{html.escape(str(column.name()))}</th>"

def _doc_path(root_path: str, doc: Doc) -> str:
    folder_name = _remove_path_unsafe_characters(doc.folder_name() + " " + doc.folder_id())
    doc_name = _remove_path_unsafe_characters(doc.name() + " " + doc.id())
    return os.path.join(root_path, folder_name, doc_name)

def _table_path(doc_path: str, table: Table) -> str:
    table_name = _remove_path_unsafe_characters(table.name() + " " + table.id())
    return os.path.join(doc_path, "tables", table.type().to_str(), table_name)

def _column_name_for_path(column: Column) -> str:
    return _remove_path_unsafe_characters(column.name() + " " + column.id())

def _remove_path_unsafe_characters(name: str) -> str:
    return name.replace('/', '_')
