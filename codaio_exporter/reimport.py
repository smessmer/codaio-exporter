import os
from typing import Optional

from codaio_exporter.progress import ProgressCallback
from codaio_exporter.table import Table


async def reimport_doc(api_token: str, source_path: str, dest_doc_id: str, progress_callback: Optional[ProgressCallback] = None) -> None:
    tables_path = os.path.join(source_path, "tables/table")
    for table_path in os.listdir(tables_path):
        with open(os.path.join(table_path, "table.json"), 'r') as file:
            json = file.read()
            table = Table.from_json(json)
            print(table)

