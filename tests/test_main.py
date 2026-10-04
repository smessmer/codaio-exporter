from __future__ import annotations

import io
import re
import sys
from typing import Final
from unittest.mock import patch

import pytest

from codaio_exporter.__main__ import error_exit, main
from codaio_exporter.progress import with_progress_display


def test_error_exit_raises_system_exit() -> None:
    with pytest.raises(SystemExit) as exc_info:
        error_exit("test message")
    assert exc_info.value.code == 1


def test_error_exit_prints_message() -> None:
    with patch("builtins.print") as mock_print, pytest.raises(SystemExit):
        error_exit("please specify --api-token")
    mock_print.assert_called_once_with("please specify --api-token")


# --- main(): characters that the encoding of stdout can't represent ---

_DOTS_SPINNER: Final = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"  # rich's default spinner
_LINE_SPINNER: Final = "-\\|/"
_HIDE_CURSOR: Final = "\x1b[?25l"
_SHOW_CURSOR: Final = "\x1b[?25h"
_DOC_NAME: Final = "Café ☕ Plan"
# So long that rich has to truncate the other columns of its row with "…" in 80 columns
_LONG_DOC_NAME: Final = "Quarterly planning: goals, owners and milestones of all teams"
_LOG_MESSAGE: Final = "Logged while the progress display is active: Grüße ☕"


def _use_stdout(monkeypatch: pytest.MonkeyPatch, encoding: str, errors: str = "strict", *, isatty: bool = True) -> io.BytesIO:
    """Replaces sys.stdout with a stream like the one Python sets up with the given encoding and error handler. With "strict" or
    "surrogateescape", it raises UnicodeEncodeError for characters the encoding can't represent. Returns the buffer that receives
    the encoded output."""
    output = io.BytesIO()
    stdout = io.TextIOWrapper(output, encoding=encoding, errors=errors, write_through=True)
    monkeypatch.setattr(stdout, "isatty", lambda: isatty)
    monkeypatch.setattr(sys, "stdout", stdout)
    # The CLI uses rich's global console, which rich creates on first use. Let rich create a new one for this stdout.
    monkeypatch.setattr("rich._console", None)
    # An 80x25 terminal, without the environment variables that would override rich's terminal detection
    monkeypatch.setenv("COLUMNS", "80")
    monkeypatch.setenv("LINES", "25")
    monkeypatch.setenv("TERM", "xterm-256color")
    for name in ("FORCE_COLOR", "TTY_COMPATIBLE", "TTY_INTERACTIVE"):
        monkeypatch.delenv(name, raising=False)
    return output


def _run_export(monkeypatch: pytest.MonkeyPatch) -> None:
    """Runs main() with an export that shows its progress like the real one, but doesn't need the Coda API."""

    async def export() -> None:
        with with_progress_display() as progress_display:
            progress_display.add_task(_DOC_NAME)
            progress_display.add_task(_LONG_DOC_NAME, total=3)
            # Log messages take this path: while the display is active, rich redirects stderr into it
            print(_LOG_MESSAGE, file=sys.stderr)
            print("Export successfully finished")

    monkeypatch.setattr("codaio_exporter.__main__.async_main", export)
    main()


def _rows(output: str) -> list[str]:
    """The lines of the output, without colors and cursor movements."""
    return re.split(r"[\r\n]+", re.sub(r"\x1b\[[0-9;?]*[A-Za-z]", "", output))


def _spinner_frames(rows: list[str], description: str) -> set[str]:
    """The spinner frames that were shown in front of the task with the given (rendered) description."""
    return {row[0] for row in rows if row[2:].startswith(description)}


def _long_doc_name_rows(rows: list[str]) -> list[str]:
    return [row for row in rows if row[2:].startswith(_LONG_DOC_NAME)]


@pytest.mark.parametrize(
    ("encoding", "errors", "rendered_doc_name", "rendered_log_message"),
    [
        # e.g. PYTHONIOENCODING=ascii
        pytest.param("ascii", "strict", "Caf? ? Plan", "Logged while the progress display is active: Gr??e ?", id="ascii-strict"),
        # e.g. a latin-1 locale
        pytest.param("latin-1", "strict", "Café ? Plan", "Logged while the progress display is active: Grüße ?", id="latin-1-strict"),
        # the C/POSIX locale without UTF-8 mode
        pytest.param("ascii", "surrogateescape", "Caf? ? Plan", "Logged while the progress display is active: Gr??e ?", id="ascii-surrogateescape"),
    ],
)
def test_main_on_non_utf8_terminal_replaces_unencodable_characters(
    monkeypatch: pytest.MonkeyPatch, encoding: str, errors: str, rendered_doc_name: str, rendered_log_message: str
) -> None:
    output = _use_stdout(monkeypatch, encoding, errors)

    _run_export(monkeypatch)

    written = output.getvalue().decode(encoding)
    rows = _rows(written)
    frames = _spinner_frames(rows, rendered_doc_name)
    assert frames
    assert frames <= set(_LINE_SPINNER)
    # The long doc name is ASCII, so a "?" in its row is the "…" with which rich truncated the other columns
    long_doc_name_rows = _long_doc_name_rows(rows)
    assert long_doc_name_rows
    assert all("?" in row for row in long_doc_name_rows)
    assert rendered_log_message in written
    assert "Export successfully finished" in written
    # The cursor that rich hid while the display was active is visible again
    assert written.rfind(_SHOW_CURSOR) > written.rfind(_HIDE_CURSOR) >= 0


@pytest.mark.parametrize(
    ("encoding", "errors", "rendered_doc_name", "rendered_ellipsis"),
    [
        pytest.param("ascii", "strict", "Caf? ? Plan", "?", id="ascii-strict"),
        # Python's default for redirected output on Windows: the ANSI code page, e.g. cp1252 (which has "…"), and "surrogateescape"
        pytest.param("cp1252", "surrogateescape", "Café ? Plan", "…", id="cp1252-surrogateescape"),
    ],
)
def test_main_with_redirected_non_utf8_output_replaces_unencodable_characters(
    monkeypatch: pytest.MonkeyPatch, encoding: str, errors: str, rendered_doc_name: str, rendered_ellipsis: str
) -> None:
    output = _use_stdout(monkeypatch, encoding, errors, isatty=False)

    _run_export(monkeypatch)

    # Without a terminal, rich writes the display only once, when it ends
    written = output.getvalue().decode(encoding)
    rows = _rows(written)
    frames = _spinner_frames(rows, rendered_doc_name)
    assert frames
    assert frames <= set(_LINE_SPINNER)
    long_doc_name_rows = _long_doc_name_rows(rows)
    assert long_doc_name_rows
    assert all(rendered_ellipsis in row for row in long_doc_name_rows)
    assert "Export successfully finished" in written


def test_main_on_utf8_terminal_shows_all_characters(monkeypatch: pytest.MonkeyPatch) -> None:
    output = _use_stdout(monkeypatch, "utf-8")

    _run_export(monkeypatch)

    written = output.getvalue().decode("utf-8")
    rows = _rows(written)
    frames = _spinner_frames(rows, _DOC_NAME)
    assert frames
    assert frames <= set(_DOTS_SPINNER)
    long_doc_name_rows = _long_doc_name_rows(rows)
    assert long_doc_name_rows
    assert all("…" in row for row in long_doc_name_rows)
    assert _LOG_MESSAGE in written


@pytest.mark.parametrize(
    ("encoding", "errors"),
    [
        # UTF-8 can represent every character. Python uses "strict" for it with a UTF-8 locale, and "surrogateescape" (which writes
        # undecodable bytes, e.g. of file names, back unchanged) in UTF-8 mode, with the C.UTF-8 locale and for the Windows console.
        ("utf-8", "strict"),
        ("utf-8", "surrogateescape"),
        # A handler that doesn't raise, e.g. one set with PYTHONIOENCODING=latin-1:backslashreplace
        ("latin-1", "backslashreplace"),
    ],
)
def test_main_keeps_error_handler_if_stdout_can_write_every_character(monkeypatch: pytest.MonkeyPatch, encoding: str, errors: str) -> None:
    stdout = io.TextIOWrapper(io.BytesIO(), encoding=encoding, errors=errors)
    monkeypatch.setattr(sys, "stdout", stdout)

    async def do_nothing() -> None:
        pass

    monkeypatch.setattr("codaio_exporter.__main__.async_main", do_nothing)
    main()

    assert stdout.errors == errors
