from __future__ import annotations

import pytest

from codaio_exporter.errors import CodaExporterError, DataFormatError, SchemaValidationError


def test_coda_exporter_error_is_exception() -> None:
    assert issubclass(CodaExporterError, Exception)


def test_schema_validation_error_is_coda_exporter_error() -> None:
    assert issubclass(SchemaValidationError, CodaExporterError)


def test_data_format_error_is_coda_exporter_error() -> None:
    assert issubclass(DataFormatError, CodaExporterError)


def test_schema_validation_error_caught_as_parent() -> None:
    with pytest.raises(CodaExporterError):
        raise SchemaValidationError("test")


def test_data_format_error_caught_as_parent() -> None:
    with pytest.raises(CodaExporterError):
        raise DataFormatError("test")
