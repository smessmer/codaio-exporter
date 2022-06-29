from dataclasses import dataclass
from typing import List, Optional
from io import StringIO
import html, csv

from codaio_exporter.api.column import ColumnAPI
from codaio_exporter.api.row import RowAPI


@dataclass
class Column:
    id: str
    name: str
    formula: Optional[str]

@dataclass
class Row:
    cells: List[str]

@dataclass
class Table:
    columns: List[Column]
    rows: List[Row]

    def to_csv_with_column_ids(self) -> str:
        output = StringIO()
        wr = csv.writer(output, quoting=csv.QUOTE_ALL)
        wr.writerow([column.id for column in self.columns])
        for row in self.rows:
            wr.writerow(row.cells)
        return output.getvalue()

    def to_csv_with_column_names(self) -> str:
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


def parse_table_from_api(columns: List[ColumnAPI], rows: List[RowAPI]) -> Table:
    rows.sort(key=lambda row: row.index())
    parsed_columns = [_parse_column(column) for column in columns]
    parsed_rows = [_parse_row(parsed_columns, row) for row in rows]
    return Table(
        columns=parsed_columns,
        rows=parsed_rows,
    )

def _parse_column(column: ColumnAPI) -> Column:
    return Column(
        id=column.id(),
        name=column.name(),
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
