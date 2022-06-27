import argparse
import asyncio
import logging
import curses
from typing import Dict

from codaio_exporter.export import export_all_docs

class ProgressDisplay:
    def __init__(self) -> None:
        self._progress: Dict[str, str] = {}
        self._screen = curses.initscr()

    def progress_callback(self, doc_name: str, progress: int, total: int) -> None:
        self._progress[doc_name] = f"{progress}/{total}"
        self._display()
    
    def _display(self) -> None:
        line = 0
        self._screen.addstr(0, 0, "Exporting tables from documents:")
        self._screen.addstr(1, 0, "--------------------------------")
        line += 3
        for key, value in self._progress.items():
            self._screen.addstr(line, 0, f"{key}: {value}")
            line += 1
        self._screen.refresh()
    

async def async_main() -> None:
    logging.getLogger('backoff').addHandler(logging.StreamHandler())
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.WARN)

    parser = argparse.ArgumentParser(description='Export tables from coda.io')
    parser.add_argument('api_token', type=str)
    parser.add_argument('dest_dir', type=str)
    args = parser.parse_args()

    progress_display = ProgressDisplay()

    await export_all_docs(args.api_token, args.dest_dir, progress_display.progress_callback)

def main() -> None:
    asyncio.run(async_main())

if __name__ == "__main__":
    curses.wrapper(main)
