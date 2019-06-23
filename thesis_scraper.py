"""
root table is here:
    https://skemman.is/simple-search?query=*

Breadcrumbs show taxonomy of paper e.g.
    BSc thesis, MSc thesis, conference paper
    store taxonomy in csv

Table has a list of available files
    also has file type
    grab largest pdf file in table

"""

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


def breadcrumbs_to_path(breadcrumbs):
    breadcrumbs = [item.replace("/", "-") for item in breadcrumbs]
    return utils.transliterate_path("/".join(breadcrumbs)).lower()


class SkemmanDocument:
    def __init__(self, href, title=None, author=None, accepted=None):
        self.href = href
        self.title = title
        self.author = author
        self.accepted = accepted
        self.html = None
        self.attrs = None
        self.filelist = None
        self.document_id = None

    def fetch(self):
        logger.info(f"Getting document page for {self.href}")
        query_str, fragment = "", ""
        url_tup = (Skemman._scheme, Skemman._netloc, self.href, query_str, fragment)
        url = urllib.parse.urlunsplit(url_tup)

        self.html = Fetcher.fetch_with_retry(url)

    def parse(self):
        logger.info(f"Parsing document page {self.href}")
        soup = bs(self.html, "html.parser")
        main_content = soup.find("div", id="index_page")
        header = main_content.find("h1")

        # Content type
        header_h1 = header.contents[0]
        header_h1 = header_h1.strip()
        logger.debug("Header: %s", header_h1)

        # Bread crumbs, document taxonomy
        trail = main_content.find("span", class_="trail")
        breadcrumbs = []
        for elem in trail.find_all("a", class_="trailInstitution"):
            breadcrumbs.append(elem.text.strip())
        logger.debug("Breadcrumbs: %s", str(breadcrumbs))

        # Document metadata attributes
        content_metadata = main_content.find("div", class_="attrList")
        document_attrs = []
        for attr in content_metadata.find_all("div", class_="attr"):
            label_elem = attr.find("span", class_="attrLabel")
            label = (
                label_elem.text.strip() if hasattr(label_elem, "text") else label_elem
            )

            content_elem = attr.find("div", class_="attrContent")
            content = (
                content_elem.text.strip()
                if hasattr(content_elem, "text")
                else content_elem
            )

            if label is None or content is None:
                continue

            document_attrs.append([label, content])
            logger.debug("Found attribute: %s", str([label, content]))
        self.attrs = {key: val for (key, val) in document_attrs}
        self.attrs["taxonomy"] = json.dumps(breadcrumbs, ensure_ascii=False)

        table = content_metadata.find("table", class_="t-data-grid")
        filelist = []
        for entry in table.find_all("tr"):
            row = []
            for col in entry.find_all("td"):
                row.append(col)
                logger.debug("Found column data: %s", str(col.text))
            if not row:
                continue
            fname, size, access, descr, ftype, link = row
            href = link.find("a")
            href = href.get("href") if href is not None else href

            file_entry = SkemmanFile(
                self,
                fname=fname.text,
                href=href,
                size=size.text,
                access=access.text,
                descr=descr.text,
                ftype=ftype.text,
            )

            filelist.append(file_entry)
            logger.debug("Found file data: %s", str(file_entry))
        self.filelist = filelist

    def __repr__(self):
        title_fmt = self.title
        dieresis = "..."
        max_title_len = 25
        abbrev_len = max_title_len - len(dieresis)
        if len(title_fmt) > max_title_len:
            title_fmt = f"{title_fmt[:abbrev_len]}{dieresis}"
        return f"<SkemmanDocument {self.href}: {title_fmt}>"

    def get_id(self, db):
        self.document_id = db.get_document_id_by_href(self.href)

    def get_id_or_store_document(self, db):
        self.get_id(db)
        if self.document_id is None:
            cursor = db.insert_document(self)
            self.document_id = cursor.lastrowid

    def store_filelist(self, db):
        if self.document_id is not None:
            self.get_id_or_store_document(db)
        rel_dir = breadcrumbs_to_path(json.loads(self.attrs["taxonomy"]))
        cursor = db.insert_filelist(self.filelist, rel_dir, self.document_id)

    def store_attrs(self, db):
        if self.document_id is not None:
            self.get_id_or_store_document(db)
        cursor = db.insert_map(self.attrs, self.document_id)

    def store_all(self, db):
        self.get_id_or_store_document(db)
        self.store_filelist(db)
        self.store_attrs(db)


class SkemmanFile:
    def __init__(
        self,
        document,
        href,
        fname=None,
        size=None,
        access=None,
        descr=None,
        ftype=None,
        is_local=None,
        rel_dir=None,
        doc_id=None,
        base_dir=DATA_DIR,
    ):
        self.document = document
        self.fname = fname
        self.size = size  # in MB
        self.access = access
        self.descr = descr
        self.ftype = ftype
        self.href = href
        self.is_local = is_local
        self.rel_dir = rel_dir
        self.base_dir = base_dir
        self.doc_id = doc_id

    @property
    def url(self):
        url = Skemman.make_url(self.href)
        return url

    def sync_to_disk(self, db, verbose=False):
        if self.is_on_disk and not self.is_local:
            db.update_file_status(self.href, True)
            if verbose:
                print(f"Already on disk:     {self.href}")
                print(f"File synced to db:   {self.href}", flush=True)
            return self.local_path
        elif self.is_on_disk:
            if verbose:
                print(f"Already synced file: {self.href}", flush=True)
            return self.local_path
        suffixes = self.local_path.suffixes + [".tmp"]
        tmp_path = self.local_path.with_suffix("".join(suffixes))
        if verbose:
            print(f"Downloading file ({self.size} MB): {self.href} ...", end=" ", flush=True)
        fpath = download_file(self.url, self.local_path, tmp_path, make_dirs=True)

        db.update_file_status(self.href, True)
        if verbose:
            print(f"done")
        return fpath

    @property
    def local_path(self):
        path = self.base_dir / self.rel_dir / self.local_filename
        return path

    @property
    def local_filename(self):
        translit_fname = utils.transliterate_path(self.fname)
        doc_id = str(self.doc_id)
        return f"{doc_id}.{translit_fname}"

    @property
    def is_on_disk(self):
        return self.local_path.is_file()

    def __repr__(self):
        return f"<SkemmanFile:{self.fname} at {self.href}>"


class Skemman:
    _scheme = "https"
    _netloc = "skemman.is"
    _search_path = "/simple-search"

    @classmethod
    def get_page_url(cls, page_idx):
        """Get page url of all results of page_idx.
            Note page_idx is one-index based. """

        start_idx = 0
        results_per_page = 25
        start_idx = (page_idx - 1) * results_per_page

        query_dict = {
            "query": "*",
            "sort_by": "score",
            "order": "desc",
            "rpp": str(results_per_page),
            "etal": "0",  # what is this?
            "start": str(start_idx),
        }
        fragment = ""
        new_query_str = urllib.parse.urlencode(query_dict, doseq=True)
        url = cls.make_url(cls._search_path, new_query_str)
        return url

    @classmethod
    def make_url(cls, path, query_str="", fragment=""):
        url_tup = (cls._scheme, cls._netloc, path, query_str, fragment)
        url = urllib.parse.urlunsplit(url_tup)
        return url

    @classmethod
    def get_results_from_page_idx(cls, page_idx):
        url = cls.get_page_url(page_idx)
        logger.info("Fetching page url: %s", url)
        result = Fetcher.fetch_with_retry(url)
        logger.info("Fetched page url")
        results = cls.parse_results_page(result)
        return results

    @classmethod
    def parse_results_page(cls, html):
        """Parse search results page, extracting links to new document pages"""
        logger.info("Attempting to parse results page")
        soup = bs(html, "html.parser")
        logger.info("Soup is made")
        results = []
        for table in soup.find_all("table"):
            caption = table.find("caption")
            if not caption or caption.text != "Niðurstöður":
                continue
            for row in table.find_all("tr"):
                data = []
                for col in row.find_all("td"):
                    data.append(col)
                if not data:
                    # skip header
                    continue

                date_accepted, title, author = data
                href = title.find("a").attrs["href"]
                logger.debug(f"Found entry: {href}")
                doc = SkemmanDocument(
                    href,
                    title=title.text,
                    author=author.text,
                    accepted=date_accepted.text,
                )
                results.append(doc)
            logger.info(f"Found {len(results)} entries")
        return results


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
