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
from bs4 import BeautifulSoup as bs
from collections import namedtuple
import os
from pathlib import Path
import json
import unicodedata
from icecream import ic

from fetcher import Fetcher, download_file
from skemman_db import SkemmanDb

"""https://skemman.is/simple-search?query=%2A&sort_by=score&order=desc&rpp=25&etal=0&start=1000"""

test_url = "https://skemman.is/simple-search?query=*&sort_by=score&order=desc&rpp=25&etal=0&start=25"
BASE_URL = "https://skemman.is/simple-search?query=*"

logging.basicConfig()
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)

_PROJECT_DIR = Path(os.path.realpath("__file__")).parent
_DATA_DIR = _PROJECT_DIR / "data"


def breadcrumbs_to_path(breadcrumbs):
    string = "/".join(breadcrumbs).replace(" ", "_").lower()
    return unicodedata.normalize("NFKD", string).encode("ascii", "ignore").decode("ascii")


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
        rel_dir_path = breadcrumbs_to_path(json.loads(self.attrs["taxonomy"]))
        cursor = db.insert_filelist(self.filelist, rel_dir_path, self.document_id)

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
        self, document, href, fname=None, size=None, access=None, descr=None, ftype=None
    ):
        self.document = document
        self.fname = fname
        self.size = size
        self.access = access
        self.descr = descr
        self.ftype = ftype
        self.href = href

    @property
    def url(self):
        url = Skemman.make_url(self.href)
        return url

    def download(self):
        if self.is_local:
            return self.local_path
        fpath = download_file(self.url, fname=self.fname, dir_=self._dir)
        return fpath

    @property
    def is_local(self):
        """Doc string"""
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
        table = soup.find("table")
        results = []
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
                href, title=title.text, author=author.text, accepted=date_accepted.text
            )
            results.append(doc)
        logger.info(f"Found {len(results)} entries")
        return results

def main():
    search_page_idx = 1
    db = SkemmanDb()
    finished_pages = db.get_pages()
    finished_hrefs = db.get_hrefs()

    enter = True
    docs = []
    while enter or docs:
        enter = False
        while search_page_idx in finished_pages:
            search_page_idx += 1
        docs = Skemman.get_results_from_page_idx(search_page_idx)
        for doc in docs:
            if doc.href not in finished_hrefs:
                doc.fetch()
                doc.parse()
                doc.store_all(db)
                finished_hrefs.add(doc.href)
        db.insert_page(search_page_idx)
        finished_pages.add(search_page_idx)
        search_page_idx += 1

if __name__ == '__main__':
    main()
