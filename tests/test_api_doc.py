from __future__ import annotations

from codaio_exporter.api.doc import DocAPI
from codaio_exporter.api.table import TableAPI

from .conftest import async_generator_from_list, make_doc_api_data, make_mock_client, make_table_api_data


def test_doc_id() -> None:
    doc = DocAPI(make_mock_client(), make_doc_api_data(id="d-1"))
    assert doc.id() == "d-1"


def test_doc_name() -> None:
    doc = DocAPI(make_mock_client(), make_doc_api_data(name="My Doc"))
    assert doc.name() == "My Doc"


def test_doc_folder_id() -> None:
    doc = DocAPI(make_mock_client(), make_doc_api_data(folder_id="f-1"))
    assert doc.folder_id() == "f-1"


def test_doc_folder_name_present() -> None:
    doc = DocAPI(make_mock_client(), make_doc_api_data(folder_name="Work"))
    assert doc.folder_name() == "Work"


def test_doc_folder_name_missing() -> None:
    doc = DocAPI(make_mock_client(), make_doc_api_data(folder_name=None))
    assert doc.folder_name() is None


def test_doc_raw_data() -> None:
    data = make_doc_api_data()
    doc = DocAPI(make_mock_client(), data)
    assert doc.raw_data() is data


async def test_doc_get_all_tables() -> None:
    client = make_mock_client()
    table_data_1 = make_table_api_data(id="t-1", name="Table 1")
    table_data_2 = make_table_api_data(id="t-2", name="Table 2")
    client.get_list.return_value = async_generator_from_list([table_data_1, table_data_2])

    doc = DocAPI(client, make_doc_api_data(id="d-1"))
    tables: list[TableAPI] = []
    async for table in doc.get_all_tables():
        tables.append(table)

    assert len(tables) == 2
    assert tables[0].id() == "t-1"
    assert tables[1].id() == "t-2"


async def test_doc_get_all_tables_empty() -> None:
    client = make_mock_client()
    client.get_list.return_value = async_generator_from_list([])

    doc = DocAPI(client, make_doc_api_data())
    tables: list[TableAPI] = []
    async for table in doc.get_all_tables():
        tables.append(table)

    assert tables == []


async def test_doc_get_table() -> None:
    client = make_mock_client()
    client.get_item.return_value = make_table_api_data(id="t-1", name="Table 1")

    doc = DocAPI(client, make_doc_api_data(id="d-1"))
    table = await doc.get_table("t-1")

    assert isinstance(table, TableAPI)
    assert table.id() == "t-1"
    client.get_item.assert_called_once_with("/docs/d-1/tables/t-1")
