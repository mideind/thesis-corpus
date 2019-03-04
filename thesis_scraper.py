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
logger.setLevel(logging.DEBUG)


def get_url_info(url, verbose=0):
    parsed_url = urllib.parse.urlparse(url)
    urlsplit = urllib.parse.urlsplit(url)
    query_dict = urllib.parse.parse_qs(urlsplit.query)

    scheme, netloc, path, query, fragment = urlsplit

    logger.debug("url: %s", str(url))
    logger.debug("parsed_url: %s", str(parsed_url))
    logger.debug("query_dict: %s", str(query_dict))

    query_dict["rpp"] = str(int(50))
    # Generate new url with changed query parameters
    new_query_str = urllib.parse.urlencode(query_dict, doseq=True)
    newTup = (scheme, netloc, path, new_query_str, fragment)
    print()
    pprint(new_query_str)
    newUrl = urllib.parse.urlunsplit(newTup)
    print()
    pprint(newUrl)

    return query_dict


# # info = get_url_info(test_url)
# info = get_url_info(BASE_URL)u
# pprint(info)

SearchResultsTable = namedtuple("SearchResultsTable", "accepted, title, author, href")
FileEntry = namedtuple(
    "FileEntry", "filename, size, access, description, filetype, href"
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
        # result = Fetcher.fetch_with_retry(url)
        result, _ = Fetcher.fetch_maybe(url, "test_table.html", save=True)
        logger.info("Fetched page url")
        # if result:
        results = cls.parse_results_page(result)
        return results
        # else:
        #     logger.info("no results")

    @classmethod
    def parse_results_page(cls, html):
        logger.info("Attempting to parse results page")
        soup = bs(html, "html.parser")
        logger.info("Soup is made")
        table = soup.find("table")
        results = []
        i = 0
        for row in table.find_all("tr"):
            data = []
            for col in row.find_all("td"):
                data.append(col)
            if not data:
                # skip header
                continue

            date_accepted, title, author = data
            href = title.find("a").attrs["href"]
            table_entry = SearchResultsTable(
                accepted=date_accepted.text,
                title=title.text,
                author=author.text,
                href=href,
            )
            logger.debug("Found entry: %s", str(table_entry))
            results.append(table_entry)
            i += 1
            if i > 0:
                break
        logger.info("Parsed %s entries", str(len(results)))
        return results

    @classmethod
    def get_document_page(cls, href):
        logger.info("Getting document page for %s", href)
        query_str, fragment = "", ""
        url_tup = (cls._scheme, cls._netloc, href, query_str, fragment)
        url = urllib.parse.urlunsplit(url_tup)
        logger.info("Getting document page from %s", url)

        html, _ = Fetcher.fetch_maybe(url, "test_doc.html", save=True)
        doc_info = cls.parse_document_page(html)
        return doc_info

    @classmethod
    def parse_document_page(cls, html):
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
        content_meta = main_content.find("div", class_="attrList")
        attrs = []
        for attr in content_meta.find_all("div", class_="attr"):
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

            attrs.append([label, content])
            logger.debug("Found attribute: %s", str([label, content]))

        table = content_meta.find("table", class_="t-data-grid")
        total = []
        for entry in table.find_all("tr"):
            row = []
            for col in entry.find_all("td"):
                row.append(col)
                logger.debug("Found column data: %s", str(col.text))
            if not row:
                continue
            filename, size, access, descr, filetype, link = row
            href = link.find("a").get("href")

            file_entry = FileEntry(
                filename=filename.text,
                size=size.text,
                access=access.text,
                description=descr.text,
                filetype=filetype.text,
                href=href,
            )

            total.append(file_entry)
            # logger.debug("Found file data: %s", str(data))
            logger.debug("Found file data: %s", str(file_entry))
        return total


table_res = Skemman.get_results_from_page(1)
pprint(table_res)
entry = table_res[0]
# for entry in table_res:
document = Skemman.get_document_page(entry.href)
pprint(document)
