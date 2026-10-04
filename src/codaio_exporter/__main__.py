from __future__ import annotations

import argparse
import asyncio
import io
import logging
import sys
from typing import NoReturn

from codaio_exporter.export import export_all_docs, export_doc
from codaio_exporter.progress import ProgressDisplay, with_progress_display
from codaio_exporter.reimport import reimport_doc


async def async_main() -> None:
    # Progress display needs to be initialized before the loggers so that stdout/stderr is displayed correctly
    # above the progress bars and doesn't interfere with them
    with with_progress_display() as progress_display:
        logging.getLogger("backoff").addHandler(logging.StreamHandler())
        logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.WARNING)

        parser = argparse.ArgumentParser(description="Export tables from coda.io")
        parser.add_argument("--api-token", type=str)
        subparsers = parser.add_subparsers(title="subcommands")

        parser_export = subparsers.add_parser("export", help="Export tables from one or more coda.io documents")
        parser_export.add_argument("--dest-dir", type=str, help="Destination directory to export to")
        parser_export.add_argument("--src-doc-id", type=str, help="Limit export to the tables in the document with the given id")
        parser_export.set_defaults(func=main_export)

        parser_reimport = subparsers.add_parser("reimport", help="Reimport previously exported tables into a coda.io document")
        parser_reimport.add_argument("--src-dir", type=str, help="Directory with the exported document")
        parser_reimport.add_argument("--dest-doc-id", type=str, help="The coda.io id of the document to import tables into")
        parser_reimport.set_defaults(func=main_reimport)

        args = parser.parse_args()

        if args.api_token is None:
            error_exit("Please specify the --api-token parameter")

        await args.func(args, progress_display)


async def main_export(args: argparse.Namespace, progress_display: ProgressDisplay) -> None:
    if args.dest_dir is None:
        error_exit("Please specify the --dest-dir parameter")

    if args.src_doc_id is None:
        await export_all_docs(args.api_token, args.dest_dir, progress_display)
    else:
        await export_doc(args.api_token, args.dest_dir, args.src_doc_id, progress_display)

    print("Export successfully finished")


async def main_reimport(args: argparse.Namespace, progress_display: ProgressDisplay) -> None:
    if args.src_dir is None:
        error_exit("Please specify the --src-dir argument")

    if args.dest_doc_id is None:
        error_exit("Please specify the --dest-doc-id argument")

    await reimport_doc(args.api_token, args.src_dir, args.dest_doc_id, progress_display)

    print("Reimport successfully finished")


def error_exit(msg: str) -> NoReturn:
    print(msg)
    exit(1)


def main() -> None:
    # The progress display writes doc names and rich's own symbols (e.g. "…" for truncated text) to stdout, on a terminal also log
    # messages. An encoding other than UTF (e.g. latin-1 on a terminal, or the ANSI code page of redirected output on Windows) may
    # not be able to represent all of their characters, and the error handlers that Python uses for stdout by default then raise
    # UnicodeEncodeError, which aborts the run: "strict", and "surrogateescape" (on Windows, and in the C/POSIX locale without
    # UTF-8 mode), which only covers the lone surrogates that stand for undecodable bytes. Write such characters as "?" instead.
    stdout = sys.stdout
    if isinstance(stdout, io.TextIOWrapper) and not stdout.encoding.lower().startswith("utf") and stdout.errors in ("strict", "surrogateescape"):
        stdout.reconfigure(errors="replace")
    asyncio.run(async_main())


if __name__ == "__main__":
    # curses.wrapper(main)
    main()
