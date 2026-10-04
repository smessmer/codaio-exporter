from __future__ import annotations

import io

from rich.console import Console
from rich.theme import Theme

from codaio_exporter.progress import with_progress_display


def _render(*names: str, theme: Theme | None = None) -> str:
    """Show one progress bar per name on an in-memory terminal and return everything that was written to it.

    Colors are only enabled when a theme is given, so that plain-text assertions don't have to deal with style codes.
    """
    output = io.StringIO()
    console = Console(file=output, force_terminal=True, width=120, height=25, color_system="standard" if theme is not None else None, theme=theme)
    with with_progress_display(console=console) as progress_display:
        for name in names:
            progress_display.add_task(name, total=1)
    return output.getvalue()


# --- task names are user data (e.g. coda.io doc names) and are shown literally, not parsed as rich markup ---


def test_task_name_with_brackets_is_shown_literally() -> None:
    assert "[draft] Plan" in _render("[draft] Plan")


def test_task_name_with_style_tags_is_shown_literally() -> None:
    assert "Q3 [b]budget[/b]" in _render("Q3 [b]budget[/b]")


def test_task_name_with_unmatched_closing_tag_is_shown_literally() -> None:
    assert "Notes [/x]" in _render("Notes [/x]")


def test_task_name_with_emoji_code_is_shown_literally() -> None:
    assert ":smile: Retro" in _render(":smile: Retro")


# --- styling ---


def test_task_name_keeps_progress_description_style() -> None:
    output = _render("Comparing Schemas", theme=Theme({"progress.description": "bold"}))
    assert "\x1b[1mComparing Schemas" in output
