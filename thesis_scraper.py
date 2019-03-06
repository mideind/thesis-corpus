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

from fetcher import Fetcher

"""https://skemman.is/simple-search?query=%2A&sort_by=score&order=desc&rpp=25&etal=0&start=1000"""

test_url = "https://skemman.is/simple-search?query=*&sort_by=score&order=desc&rpp=25&etal=0&start=25"
BASE_URL = "https://skemman.is/simple-search?query=*"

logging.basicConfig()
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)


def get_url_info(url, verbose=0):
    parsed_url = urllib.parse.urlparse(url)
    urlsplit = urllib.parse.urlsplit(url)
    query_dict = urllib.parse.parse_qs(urlsplit.query)

    scheme, netloc, path, query, fragment = urlsplit

    logger.debug("url: %s", str(url))
    logger.debug("parsed_url: %s", str(parsed_url))
    logger.debug("query_dict: %s", str(query_dict))

    # query_dict["rpp"] = str(int(50))
    # # Generate new url with changed query parameters
    # new_query_str = urllib.parse.urlencode(query_dict, doseq=True)
    # newTup = (scheme, netloc, path, new_query_str, fragment)
    # pprint(new_query_str)
    # newUrl = urllib.parse.urlunsplit(newTup)
    # pprint(newUrl)

    return query_dict


SearchResultsEntry = namedtuple("SearchResultsEntry", "accepted, title, author, href")
Document = namedtuple("Document", "metadata, attrs, filelist")
FileTableEntry = namedtuple(
    "FileTableEntry", "filename, size, access, description, filetype, href"
)


class Skemman:
    _scheme = "https"
    _netloc = "skemman.is"
    _path = "/simple-search"

    @classmethod
    def get_page_url(cls, page_idx):
        """ Get page url of all results of page_idx.
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
        url_tup = (cls._scheme, cls._netloc, cls._path, new_query_str, fragment)
        url = urllib.parse.urlunsplit(url_tup)
        return url

    @classmethod
    def get_results_from_page(cls, page_idx):
        url = cls.get_page_url(page_idx)
        logger.info("Fetching page url: %s", url)
        result = Fetcher.fetch_with_retry(url)
        logger.info("Fetched page url")
        results = cls.parse_results_page(result)
        return results

    @classmethod
    def parse_results_page(cls, html):
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
            table_entry = SearchResultsEntry(
                accepted=date_accepted.text,
                title=title.text,
                author=author.text,
                href=href,
            )
            logger.debug("Found entry: %s", str(table_entry))
            results.append(table_entry)
        logger.info("Parsed %s entries", str(len(results)))
        return results

    @classmethod
    def get_document_page(cls, search_item):
        logger.info("Getting document page for %s", search_item)
        query_str, fragment = "", ""
        url_tup = (cls._scheme, cls._netloc, search_item.href, query_str, fragment)
        url = urllib.parse.urlunsplit(url_tup)
        logger.info("Getting document page from %s", url)

        html = Fetcher.fetch_with_retry(url)
        doc = cls.parse_document_page(search_item, html)
        return doc

    @classmethod
    def parse_document_page(cls, search_item, html):
        logger.info("Parsing document page")
        soup = bs(html, "html.parser")
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

        # Document metadata
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

        table = content_metadata.find("table", class_="t-data-grid")
        filelist = []
        for entry in table.find_all("tr"):
            row = []
            for col in entry.find_all("td"):
                row.append(col)
                logger.debug("Found column data: %s", str(col.text))
            if not row:
                continue
            filename, size, access, descr, filetype, link = row
            href = link.find("a").get("href")

            file_entry = FileTableEntry(
                filename=filename.text,
                size=size.text,
                access=access.text,
                description=descr.text,
                filetype=filetype.text,
                href=href,
            )

            filelist.append(file_entry)
            logger.debug("Found file data: %s", str(file_entry))
        document = Document(
            metadata=search_item,
            filelist=filelist,
            attrs=document_attrs,
        )
        return document


table_res = Skemman.get_results_from_page(1)
for entry in table_res:
    document = Skemman.get_document_page(entry)
    pprint(document)
    break

