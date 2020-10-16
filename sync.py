import os
import time
from pathlib import Path
import traceback

try:
    from icecream import ic

    ic.configureOutput(includeContext=True)
except ImportError:  # Silently ignore if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

from skemman_db import SkemmanDb
from utils import get_open_access_article_pdfs
import config



def clean_dirty_state(skemman_file):
    pass


def download_files():
    fetch_status()
    print(f"Downloading files to: {config.pdf_dir()}")
    files_out = get_open_access_article_pdfs()
    db = SkemmanDb()
    remaining_files = [item for item in files_out if not item.is_local]
    try:
        for item in remaining_files:
            try:
                item.sync_to_disk(db, verbose=True)
            except KeyboardInterrupt as e:
                return
                print("Exiting")
            except Exception as e:
                traceback.print_exc()
                pass
            time.sleep(config.scrape_delay)
    except KeyboardInterrupt as e:
        print("Exiting")
        return


def fetch_status():
    db = SkemmanDb()
    files_out = get_open_access_article_pdfs()
    local_files = [item for item in files_out if item.is_local]
    rem_files = [item for item in files_out if not item.is_local]
    local_mb = sum([item.size for item in local_files])
    rem_mb = sum([item.size for item in rem_files])
    # files_on_disk = [item for item in files_out if item.is_on_disk]
    total_mb = local_mb + rem_mb
    done_pct = round(100 * (local_mb / total_mb), 1)

    print(
        (
            f"Thesis corpus sync {done_pct:>5.1f}%\n"
            f"    files downloaded: {len(local_files):>6d}  ({local_mb:>8.1f} MB)\n"
            f"    files remaining:  {len(rem_files):>6d}  ({rem_mb:>8.1f} MB)\n"
        )
    )

def is_dir(string):
    path = Path(string)
    return path if path.is_dir() else False

def main():
    import argparse

    parser = argparse.ArgumentParser("Downloader/syncer for Skemman pdf files")

    parser.add_argument(
        "--status",
        dest="status",
        action="store_true",
        required=False,
        help="Get status of sync",
    )
    parser.add_argument(
        "--sync",
        dest="sync",
        action="store_true",
        required=False,
        help="Do a sync",
    )
    parser.add_argument(
        "--dir",
        dest="dir",
        type=is_dir,
        required=False,
        default=None,
        help="Base directory for storing downloads",
    )

    args = parser.parse_args()
    if args.status:
        fetch_status()
    elif args.sync:
        download_files(download_dir=args.dir)
    else:
        print("No argument supplied, type -h for help.")


if __name__ == "__main__":
    main()
