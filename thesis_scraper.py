import logging
from pprint import pprint
import urllib.parse
from collections import namedtuple
import os
from pathlib import Path
import json
import unicodedata
import base64

from bs4 import BeautifulSoup as bs

try:
    from icecream import ic

    ic.configureOutput(includeContext=True)
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

from fetcher import Fetcher, download_file
from skemman_db import SkemmanDb
from skemman import SkemmanDocument, SkemmanFile, Skemman
import utils

"""https://skemman.is/simple-search?query=%2A&sort_by=score&order=desc&rpp=25&etal=0&start=1000"""

test_url = "https://skemman.is/simple-search?query=*&sort_by=score&order=desc&rpp=25&etal=0&start=25"
BASE_URL = "https://skemman.is/simple-search?query=*"
MAX_PAGE_IDX = 1300

logging.basicConfig()
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)

_PROJECT_DIR = Path(os.path.realpath("__file__")).parent
DATA_DIR = _PROJECT_DIR / "data"


def main():
    db = SkemmanDb()
    finished_pages = db.get_pages()
    finished_hrefs = db.get_hrefs()
    ic(finished_pages)

    docs = []
    for page_idx in range(1, MAX_PAGE_IDX):
        if page_idx in finished_pages:
            continue
        docs = Skemman.get_results_from_page_idx(page_idx)
        for doc in docs:
            if doc.href not in finished_hrefs:
                doc.get_id_or_store_document(db)
                finished_hrefs.add(doc.href)
                try:
                    doc.fetch()
                    doc.parse()
                    doc.store_all(db)
                except AttributeError as e:
                    logger.warning(f"Could not parse {doc.href}")
        db.insert_page(page_idx)


if __name__ == "__main__":
    main()
