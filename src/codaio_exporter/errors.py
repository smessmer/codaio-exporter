from __future__ import annotations


class CodaExporterError(Exception):
    pass


class SchemaValidationError(CodaExporterError):
    pass


class DataFormatError(CodaExporterError):
    pass
