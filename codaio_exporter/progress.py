from typing import Generator, Optional
from rich.progress import Progress, TextColumn, BarColumn, MofNCompleteColumn, SpinnerColumn, TimeRemainingColumn, TaskProgressColumn
from contextlib import contextmanager


class ProgressBar:
    def __init__(self, progress: Progress, name: str, total: Optional[int] = None):
        self._current = 0
        self._total = total
        self._progress = progress
        self._task_id = self._progress.add_task(name, total=total)
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
        self._progress = progress

    def add_task(self, name: str, total: Optional[int] = None) -> ProgressBar:
        return ProgressBar(self._progress, name, total=total)

@contextmanager
def with_progress_display() -> Generator[ProgressDisplay, None, None]:
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), MofNCompleteColumn(), TaskProgressColumn(), TimeRemainingColumn()) as progress:
        yield ProgressDisplay(progress)
