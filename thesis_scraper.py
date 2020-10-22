import time

from skemman_db import SkemmanDb
from skemman import Skemman
import config
import utils

MAX_PAGE_IDX = 1441  # TODO: Get rid of this


def scrape_skemman(max_documents: int):
    current_documents = utils.get_open_access_article_pdfs()

    remaining_count = max_documents - len(current_documents)
    print("remaining documents to scrape:", remaining_count)
    if remaining_count <= 0 and max_documents > 0:
        print("already know enough docs")
        return

    db = SkemmanDb()
    finished_pages = db.get_pages()
    finished_hrefs = db.get_hrefs()

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

                    # TODO: Not accurate due to some documents not
                    # being open access. Good enough for testing though.
                    remaining_count -= 1
                    if remaining_count <= 0:
                        break
                except AttributeError as e:
                    print(f"Could not parse {doc.href}")

            time.sleep(config.scrape_delay)

        if remaining_count <= 0:
            break
        db.insert_page(page_idx)


if __name__ == "__main__":
    scrape_skemman(10)
