import argparse
import asyncio
import logging
# import curses
from typing import Dict, Optional, NoReturn

from codaio_exporter.export import export_all_docs, export_doc
from codaio_exporter.reimport import reimport_doc


class ProgressDisplay:
    def __init__(self) -> None:
        self._progress: Dict[str, str] = {}
        # self._screen = curses.initscr()

    def progress_callback(self, doc_name: str, progress: int, total: int) -> None:
        self._progress[doc_name] = f"{progress}/{total}"
        self._display()
    
    def _display(self) -> None:
        print("------------------------\nProgress: \n" + "\n".join([f"{key}: {value}" for key, value in self._progress.items()]) + "\n------------------------\n")
        # line = 0
        # self._screen.addstr(0, 0, "Exporting tables from documents:")
        # self._screen.addstr(1, 0, "--------------------------------")
        # line += 3
        # for key, value in self._progress.items():
        #     self._screen.addstr(line, 0, f"{key}: {value}")
        #     line += 1
        # self._screen.refresh()
    

async def async_main() -> None:
    logging.getLogger('backoff').addHandler(logging.StreamHandler())
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.WARN)

    parser = argparse.ArgumentParser(description='Export tables from coda.io')
    parser.add_argument('--api-token', type=str)
    subparsers = parser.add_subparsers(title='subcommands')

    parser_export = subparsers.add_parser('export', help="Export tables from one or more coda.io documents")
    parser_export.add_argument('--dest-dir', type=str, help="Destination directory to export to")
    parser_export.add_argument('--src-doc-id', type=str, help="Limit export to the tables in the document with the given id")
    parser_export.set_defaults(func=main_export)

    parser_reimport = subparsers.add_parser('reimport', help="Reimport previously exported tables into a coda.io document")
    parser_reimport.add_argument('--src-dir', type=str, help="Directory with the exported document")
    parser_reimport.add_argument('--dest-doc-id', type=str, help="The coda.io id of the document to import tables into")
    parser_reimport.set_defaults(func=main_reimport)

    args = parser.parse_args()

    if args.api_token is None:
        error_exit("Please specify the --api-token parameter")

    await args.func(args)

async def main_export(args: argparse.Namespace) -> None:
    if args.dest_dir is None:
        error_exit("Please specify the --dest-dir parameter")

    progress_display = ProgressDisplay()
    
    if args.src_doc_id is None:
        await export_all_docs(args.api_token, args.dest_dir, progress_display.progress_callback)
    else:
        await export_doc(args.api_token, args.dest_dir, args.src_doc_id, progress_display.progress_callback)

    print("Export successfully finished")

async def main_reimport(args: argparse.Namespace) -> None:
    if args.src_dir is None:
        error_exit("Please specify the --src-dir argument")

    if args.dest_doc_id is None:
        error_exit("Please specify the --dest-doc-id argument")

    progress_display = ProgressDisplay()
    
    await reimport_doc(args.api_token, args.src_dir, args.dest_doc_id, progress_display.progress_callback)

    print("Reimport successfully finished")
    
    
def error_exit(msg: str) -> NoReturn:
    print(msg)
    exit(1)

def main() -> None:
    asyncio.run(async_main())

if __name__ == "__main__":
    # curses.wrapper(main)
    main()
