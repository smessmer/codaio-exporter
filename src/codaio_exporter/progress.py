from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Final, final

from rich.progress import BarColumn, MofNCompleteColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn, TimeRemainingColumn


@final
class ProgressBar:
    def __init__(self, progress: Progress, name: str, total: int | None = None):
        self._current = 0
        self._total = total
        self._progress: Final = progress
        self._task_id: Final = self._progress.add_task(name, total=total)
        self._update()

    def set_total(self, total: int) -> None:
        self._total = total
        self._update()

    def increment_progress(self) -> None:
        self._current += 1
        self._update()

    def increment_total(self, update: bool = True) -> None:
        if self._total is None:
            self._total = 0
        self._total += 1
        if update:
            self._update()

    def _update(self) -> None:
        self._progress.update(self._task_id, completed=self._current, total=self._total)


class ProgressDisplay:
    def __init__(self, progress: Progress):
        super().__init__()
        self._progress = progress
        if progress.console.options.ascii_only:
            # The output encoding isn't UTF (e.g. it is latin-1). rich then draws its bars in ASCII, but not the Braille dots of its default spinner.
            for column in progress.columns:
                if isinstance(column, SpinnerColumn):
                    column.set_spinner("line")

    def add_task(self, name: str, total: int | None = None) -> ProgressBar:
        return ProgressBar(self._progress, name, total=total)


@contextmanager
def with_progress_display() -> Generator[ProgressDisplay, None, None]:
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
    ) as progress:
        yield ProgressDisplay(progress)
