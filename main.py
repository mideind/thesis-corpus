#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

import thesis_scraper
import config
import sync
import segment_skemman
import clean_segments


def print_status():
    # TODO: Look for useful info in the DB
    print("this is the current status")
    sync.fetch_status()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Download and extract text from pdf files.")

    parser.add_argument(
        "--status",
        dest="status",
        action="store_true",
        required=False,
        help="Print some info about what's currently in the database.",
    )

    parser.add_argument(
        "--data-dir",
        dest="data_dir",
        required=False,
        default="./data/",
        type=Path,
        help="Base directory to store data.",
    )

    parser.add_argument(
        "--max-documents",
        dest="max_docs",
        required=False,
        default=10,
        type=int,
        help="Maximum amount of documents to fetch. Set to -1 to fetch all found documents.",
    )

    parser.add_argument(
        "--scrape-delay",
        dest="scrape_delay",
        required=False,
        default=1.0,
        type=float,
        help="Number of seconds to wait between concurrent requests when scraping data.",
    )

    parser.add_argument(
        "--actions",
        dest="actions",
        required=False,
        default=None,
        nargs="*",
        choices=["scrape", "download", "extract", "clean", "abstracts"],
        help="What actions to perform. Default is to run all actions. Be aware that some action combinations may not make sense depending on what has been done before.",
    )

    args = parser.parse_args()
    print(args)

    config.data_dir = args.data_dir
    config.scrape_delay = args.scrape_delay

    if args.status:
        print_status()
        sys.exit(0)

    do_all = args.actions is None

    if do_all or "scrape" in args.actions:
        thesis_scraper.scrape_skemman(args.max_docs)

    if do_all or "download" in args.actions:
        sync.download_files()

    if do_all or "extract" in args.actions:
        segment_skemman.gen_pdf()

    if do_all or "clean" in args.actions:
        clean_segments.clean_current_db()

    if do_all or "abstracts" in args.actions:
        # TODO
        pass
