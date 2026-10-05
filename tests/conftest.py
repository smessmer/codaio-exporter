from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any, Final
from unittest.mock import AsyncMock, MagicMock

import pytest

from codaio_exporter.api.client import Client
from codaio_exporter.table import Column, Row, Table


def make_column_api_data(
    *,
    id: str = "col-1",
    name: str = "Column 1",
    calculated: bool | None = None,
    formula: str | None = None,
) -> dict[str, Any]:
    data: dict[str, Any] = {"id": id, "name": name}
    if calculated is not None:
        data["calculated"] = calculated
    if formula is not None:
        data["formula"] = formula
    return data


def make_row_api_data(
    *,
    id: str = "row-1",
    name: str = "Row 1",
    index: int = 0,
    values: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {"id": id, "name": name, "index": index, "values": values or {}}


def make_doc_api_data(
    *,
    id: str = "doc-1",
    name: str = "My Doc",
    folder_id: str = "folder-1",
    folder_name: str | None = "My Folder",
) -> dict[str, Any]:
    folder: dict[str, Any] = {"id": folder_id}
    if folder_name is not None:
        folder["name"] = folder_name
    return {"id": id, "name": name, "folder": folder}


def make_table_api_data(
    *,
    id: str = "table-1",
    name: str = "My Table",
    table_type: str = "table",
) -> dict[str, Any]:
    return {"id": id, "name": name, "tableType": table_type}


def make_column(
    *,
    id: str = "col-1",
    name: str = "Column 1",
    calculated: bool = False,
    formula: str | None = None,
) -> Column:
    return Column(id=id, name=name, calculated=calculated, formula=formula)


def make_row(
    *,
    index: int = 0,
    id: str = "row-1",
    name: str = "Row 1",
    cells: list[str] | None = None,
    raw_data: dict[str, Any] | None = None,
) -> Row:
    return Row(index=index, id=id, name=name, cells=cells or [], raw_data=raw_data or {})


def make_table(
    *,
    id: str = "table-1",
    name: str = "My Table",
    columns: list[Column] | None = None,
    rows: list[Row] | None = None,
) -> Table:
    return Table(id=id, name=name, columns=columns or [], rows=rows or [])


async def async_generator_from_list(items: list[Any]) -> AsyncGenerator[Any, None]:
    for item in items:
        yield item


def make_mock_client() -> MagicMock:
    client = MagicMock(spec=Client)
    client.get_item = AsyncMock()
    client.get_list = MagicMock()
    client.post = AsyncMock()
    client.delete = AsyncMock()
    return client


# Fails if open() wouldn't use ASCII in the new interpreter after all, so that tests can't pass without testing anything
_CHECK_ASCII_LOCALE: Final = """
import codecs, locale, sys
assert not sys.flags.utf8_mode, "UTF-8 mode is on, so open() would use UTF-8 instead of ASCII"
assert codecs.lookup(locale.getencoding()).name == "ascii", "open() would use " + locale.getencoding() + " instead of ASCII"
"""


def run_python_with_ascii_locale(code: str, cwd: Path) -> str:
    """Runs the Python code in a new interpreter in the C locale without UTF-8 mode, in the working directory cwd. There, open()
    reads and writes text files as ASCII unless it gets an explicit encoding. (Patching the locale module doesn't change the encoding
    that open() uses, so this needs a new interpreter.) The interpreter decodes its command line as ASCII as well, so the code must be
    ASCII: embed strings with the !a conversion, e.g. f"{text!a}". It also can't open a path with non-ASCII characters given as a
    string, so refer to files by their paths relative to cwd, whose own path may have any characters. Returns what the code printed."""
    if sys.platform == "win32":
        pytest.skip("LC_ALL doesn't change the encoding that open() uses on Windows")
    if not all(path.isascii() for path in sys.path):
        # e.g. the new interpreter reads .pth files as ASCII, so with a non-ASCII path of an editable install of codaio_exporter, Python
        # 3.11 exits at startup and newer versions skip the path. Python 3.12+ also can't load extension modules from non-ASCII paths.
        pytest.skip("The new interpreter can't import from the non-ASCII paths in sys.path")
    result = subprocess.run(
        [sys.executable, "-c", _CHECK_ASCII_LOCALE + textwrap.dedent(code)],
        cwd=cwd,
        env={**os.environ, "LC_ALL": "C", "PYTHONUTF8": "0"},
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr.decode(errors="replace")
    return result.stdout.decode("ascii")
