# AGENTS.md — codaio-exporter

## Project overview

Async Python CLI tool that exports tables from Coda.io documents to local files (CSV, HTML, JSON, YAML) and can reimport previously exported tables back into Coda.io. Uses asyncio throughout with adaptive rate limiting, retries, and concurrency control.

**Version:** 0.3.4 | **Python:** 3.10+ | **Package manager:** uv

## Quick reference

```bash
uv sync                                 # Install dependencies
uv run codaio-exporter                  # Run the CLI
uv run pyright                          # Type check (strict mode)
uv run pytest                           # Run tests
uv run ruff check src/ tests/           # Lint
uv run ruff format --check src/ tests/  # Format check
```

CLI usage:
```bash
# Export all docs
codaio-exporter --api-token <TOKEN> export --dest-dir ./out

# Export single doc
codaio-exporter --api-token <TOKEN> export --dest-dir ./out --src-doc-id <ID>

# Reimport
codaio-exporter --api-token <TOKEN> reimport --src-dir ./out --dest-doc-id <ID>
```

## Repository structure

```
src/codaio_exporter/
├── __main__.py              # Entry point, CLI arg parsing (argparse)
├── export.py                # Export orchestration, file writing
├── reimport.py              # Reimport orchestration, schema validation
├── table.py                 # Data models: Table, Row, Column (dataclasses-json)
├── progress.py              # Rich-based progress bars
├── api/
│   ├── __init__.py          # make_api() factory (async context manager)
│   ├── client.py            # HTTP client: auth, pagination, rate limit, retry
│   ├── doc.py               # DocAPI wrapper
│   ├── table.py             # TableAPI wrapper (TableType enum: table/view)
│   ├── column.py            # ColumnAPI wrapper
│   ├── row.py               # RowAPI wrapper
│   └── parse.py             # Type-safe response parsing helpers
└── utils/
    ├── ratelimit.py         # AdaptiveRateLimit decorator (3-state machine)
    ├── retry.py             # @retry decorator with exponential backoff
    ├── concurrencylimit.py  # ConcurrencyLimit decorator (semaphore)
    ├── gather.py            # Async gather variants (cancel-on-error, raise-first)
    ├── generator.py         # collect(), concurrent_async_for()
    └── async_app.py         # AsyncApp abstract base class

tests/
└── test_codaio_exporter.py  # Test suite (pytest)

pyproject.toml               # Project config, dependencies, entry point (PEP 621 + uv)
```

## Architecture

```
__main__.py  →  export.py / reimport.py  →  api/  →  utils/
   CLI            Business logic           Coda.io     Rate limit,
   argparse       File I/O                 REST API    retry,
                  Progress bars            wrappers    concurrency
```

**Data flow (export):** CLI parses args → `make_api()` creates HTTP client → enumerate docs/tables → fetch columns+rows concurrently → write CSV/HTML/JSON/YAML files.

**Data flow (reimport):** CLI parses args → load JSON from disk → validate schema against live Coda.io tables → delete existing rows → insert new rows.

**API endpoint:** `https://coda.io/apis/v1` with Bearer token auth.

## Code conventions

### Type safety

pyright strict mode is mandatory. Every function needs full type annotations. Key patterns:

- `@final` decorator on classes to prevent inheritance
- `Final` annotation on immutable instance variables
- `NoReturn` for error-exit functions
- `NewType` for domain types (e.g., `RequestId`)

### Async patterns

All I/O is async. Never use blocking calls.

- File I/O: `aiofiles` with semaphore (512 concurrent max)
- HTTP: `aiohttp` with concurrency limit (50 concurrent max)
- Parallel work: `gather_raise_first_error_after_all_tasks_complete()` or `gather_cancel_on_first_error()`
- Async iteration: `collect()` to materialize async generators, `concurrent_async_for()` for concurrent processing

### Decorator stacking on API methods

API methods compose three decorators in this order:

```python
@_concurrency_limit   # Outermost: limits total concurrent calls
@retry(10)            # Middle: retries on any exception
@_request_limit       # Innermost: adaptive rate limiting
async def _get_page(self, ...): ...
```

### API wrapper pattern

Each Coda.io resource (Doc, Table, Column, Row) has a corresponding `*API` class that:
1. Wraps the raw `Dict[str, Any]` response
2. Provides typed accessor methods (e.g., `.id()`, `.name()`)
3. Exposes `.raw_data()` for full JSON access
4. Uses `parse_*()` helpers from `api/parse.py` for type-safe field extraction

### Error handling

Custom exception hierarchy rooted at `CodaError`:
- `NotFound` (404), `TooManyRequests` (429), `ContentTypeError`, `StatusCodeError`, `ResponseFormatError`

`TooManyRequests` triggers the adaptive rate limiter's backoff state.

### Naming

- Classes: `PascalCase` (e.g., `ProgressDisplay`, `TableAPI`)
- Functions/methods: `snake_case` (e.g., `export_all_docs`)
- Private: leading underscore (e.g., `_export_doc`)
- Constants: `_UPPER_SNAKE_CASE` (e.g., `_MAX_PAGE_SIZE`)
- Imports: explicit, grouped as stdlib → third-party → local

### File path safety

- `/` in names replaced with `_`
- Row names > 100 chars become `ROWNAME_TOO_LONG`
- Indices zero-filled for filesystem sort order
- Docs without folders use `NO_FOLDER_NAME`

## Export file structure

```
{dest_dir}/{folder_name} {folder_id}/{doc_name} {doc_id}/
├── api_object.json/.yaml
└── tables/{table|view}/{table_name} {table_id}/
    ├── api_object.json/.yaml
    ├── table.csv
    ├── table.html
    ├── table.json/.yaml
    ├── columns/{index}-{col_id}-{col_name}.json/.yaml
    └── rows/{index}-{row_id}-{row_name}.json/.yaml
```

## Dependencies

| Package | Purpose |
|---------|---------|
| aiohttp | Async HTTP client for Coda.io API |
| dataclasses-json | JSON/dataclass serialization (`DataClassJsonMixin`) |
| rich | Terminal progress bars |
| aiofiles | Async file I/O |
| ensure | Runtime type assertions in `parse.py` |
| PyYAML | YAML output format |

Dev (via dependency group): `pytest`, `pyright`, `ruff`, `types-aiofiles`, `types-PyYAML`

## Common tasks

### Adding a new export format

1. Add generation method to `Table` in `src/codaio_exporter/table.py`
2. Call it from `_export_rows()` in `src/codaio_exporter/export.py`
3. Write output via `_write_file()` (respects the file-write semaphore)

### Adding a new API resource

1. Create `src/codaio_exporter/api/new_resource.py` with a `@final` wrapper class
2. Add typed accessors using `parse_*()` helpers
3. Add list/get methods to the parent resource's API class
4. Use `client.get_list()` for paginated endpoints, `client.get_item()` for single items

### Modifying rate limiting / retry behavior

- Rate limit config: `AdaptiveRateLimit(TooManyRequests, backoff_seconds)` in `api/client.py`
- Retry config: `@retry(max_num_retries)` in `api/client.py`
- Concurrency config: `ConcurrencyLimit(max_concurrent)` in `api/client.py`
- File I/O concurrency: `asyncio.Semaphore(512)` in `export.py` and `reimport.py`
