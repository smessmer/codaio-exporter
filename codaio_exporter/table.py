from dataclasses import dataclass
from dataclasses_json import DataClassJsonMixin
from typing import List, Optional, final
from io import StringIO
import html, csv

from codaio_exporter.api.column import ColumnAPI
from codaio_exporter.api.row import RowAPI


@final
@dataclass
class Column(DataClassJsonMixin):
    id: str
    name: str
    calculated: bool
    formula: Optional[str]

@final
@dataclass
class Row(DataClassJsonMixin):
    cells: List[str]

@final
@dataclass
class Table(DataClassJsonMixin):
    id: str
    name: str
    columns: List[Column]
    rows: List[Row]

    def to_csv(self) -> str:
        output = StringIO()
        wr = csv.writer(output, quoting=csv.QUOTE_ALL)
        wr.writerow([column.name for column in self.columns])
        for row in self.rows:
            wr.writerow(row.cells)
        return output.getvalue()
    
    def to_html(self) -> str:
        column_headers_html = "".join(_make_html_column_header(column) for column in self.columns)
        rows = [
            "".join([f"<td>{html.escape(str(cell))}</td>" for cell in row.cells]) for row in self.rows
        ]
        rows_html = "".join(f"<tr>{row}</tr>" for row in rows)
        table_html =  f"<table><thead><tr>{column_headers_html}</tr></thead><tbody>{rows_html}</tbody></table>" 
        return f"<html><head/><body>{table_html}</body></html>"


def parse_table_from_api(table_id: str, table_name: str, columns: List[ColumnAPI], rows: List[RowAPI]) -> Table:
    rows.sort(key=lambda row: row.index())
    parsed_columns = [_parse_column(column) for column in columns]
    parsed_rows = [_parse_row(parsed_columns, row) for row in rows]
    return Table(
        id=table_id,
        name=table_name,
        columns=parsed_columns,
        rows=parsed_rows,
    )

def _parse_column(column: ColumnAPI) -> Column:
    return Column(
        id=column.id(),
        name=column.name(),
        calculated=column.calculated(),
        formula=column.formula(),
    )

def _parse_row(columns: List[Column], row: RowAPI) -> Row:
    if row.num_cells() != len(columns):
        raise Exception(f"_Row {row.id()} has wrong number of cells. Expected {len(columns)} columns but found {row.num_cells()}")
    cells = [row.get_cell_value(column.id) for column in columns]
    return Row(
        cells=cells,
    )

def _make_html_column_header(column: Column) -> str:
    formula = column.formula
    if formula is None:
        formula = "no formula"
    return f"<th title=\"{html.escape(formula)}\">{html.escape(str(column.name))}</th>"
