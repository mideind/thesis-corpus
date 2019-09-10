import os
import time
import random
from pathlib import Path
from collections import Counter, namedtuple
import subprocess
import traceback
import uuid
import shutil

try:
    from icecream import ic

    ic.configureOutput(includeContext=True)
except ImportError:  # Silently ignore if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

import sqlalchemy
import sqlalchemy.sql
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Sequence

from reynir import bintokenizer
from tokenizer import paragraphs, mark_paragraphs
from tokenizer import correct_spaces

from skemman_db import SkemmanDb
from utils import get_open_access_article_pdfs

"""
# number of files in db
select count(num) from (select count(*) as num from segments group by document_id) where num > 1;
16_564

# number of lines
select sum(num) from (select count(*) as num from segments group by document_id) where num > 1;
16_063_945

# number of lines, naive
select count(*) from segments;
16_064_079

# number of characters
select sum(len) from (select length(text) as len from segments);
1_750_937_023

# number of words/tokens
sqlite3 segment.db "select text from segments;" | wc -w
277_426_436
"""

_PDFBOX_PATH = "/home/haukur/Downloads/pdfbox-app-2.0.16.jar"
DATA_DIR = Path("/mnt/windows/thesiscorpus/data")
TMP_DIR = Path("/tmp") / "thesiscorpus"
SAMPLE = DATA_DIR / Path(
    "/haskoli_islands/felagsvisindasvid/meistaraprofsritgerdir_-_felagsvisindasvid/1235-0_383.txt"
)
OUT_DIR = Path("/home/haukur/Projects/thesiscorpus/data")
NOISY_SAMPLE_TXT = Path("samples/sample_noisy.pdfbox.txt")

Segment = namedtuple("Segment", "index text")


class SegmentDb:
    db_url = "sqlite:///segment.db"

    def __init__(self):
        self.engine = sqlalchemy.create_engine(self.db_url)
        metadata = sqlalchemy.MetaData()
        self.documents = sqlalchemy.Table(
            "documents",
            metadata,
            Column("id", sqlalchemy.Integer, autoincrement=True, primary_key=True),
            Column("skemman_id", sqlalchemy.String, unique=True, nullable=False),
        )
        self.segments = Table(
            "segments",
            metadata,
            Column("id", sqlalchemy.Integer, autoincrement=True, primary_key=True),
            sqlalchemy.Column("sentence_index", sqlalchemy.String),
            sqlalchemy.Column("text", sqlalchemy.String),
            sqlalchemy.Column(
                "document_id", sqlalchemy.Integer, sqlalchemy.ForeignKey("documents.id")
            ),
        )
        metadata.create_all(self.engine)

    def insert_segments(self, segments, skemman_id):
        with self.engine.begin() as connection:
            result = connection.execute(
                self.documents.insert(), skemman_id=skemman_id
            )
            key = result.inserted_primary_key[0]
            segs = [
                {"sentence_index": segment.index, "text": segment.text, "document_id": key}
                    for segment in segments
            ]
            connection.execute(self.segments.insert(), segs)

    def get_completed(self):
        with self.engine.begin() as connection:
            sel = sqlalchemy.sql.select([self.documents.c.skemman_id])
            result = connection.execute(sel)
            return set(row.skemman_id for row in result.fetchall())


def gen_pdf():
    segment_db = SegmentDb()
    completed_files = segment_db.get_completed()
    rem_files = [
        item
        for item in get_open_access_article_pdfs(download_dir=DATA_DIR)
        if item.is_on_disk
        and item.language == "icelandic"
        and item.url not in completed_files
    ]
    for (idx, item) in enumerate(rem_files):
        try:
            skemman_id = item.url
            process_pdf(segment_db, item)
            time.sleep(3)
        except KeyboardInterrupt:
            remain = len(rem_files) - idx - 1
            print()
            print(f"Exiting... {remain} remaining")
            return
        except Exception:
            continue


def process_pdf(db, item):
    tmp_fname = str(uuid.uuid4())
    tmp_path = TMP_DIR / tmp_fname
    success = pdfbox_to_text(item.local_path, tmp_path)
    if success:
        with open(tmp_path, "r") as fh:
            text = fh.read()
        segments = segment_text(text)
        db.insert_segments(segments, item.url)
    if tmp_path.exists():
        os.remove(tmp_path)


def toks_to_text(tokstream):
    return correct_spaces(" ".join([tok.txt for tok in tokstream if tok.txt]))


def segment_text(text):
    sent_offset = 1
    par_offset = 1
    # text = mark_paragraphs(text)
    # output from skemman+pdfbox seems better without marking paragraphs
    toks = bintokenizer.tokenize(text)
    paragraph_stream = paragraphs(toks)
    for (par_idx, paragraph) in enumerate(paragraph_stream):
        par_idx += par_offset
        for (rel_sent_idx, (offset, sentence)) in enumerate(paragraph):
            sent_idx = sent_offset + rel_sent_idx
            # yield Segment(index=f"{par_idx}.{sent_idx}", text=toks_to_text(sentence))
            yield Segment(index=str(sent_idx), text=toks_to_text(sentence))


def test_segment():
    files_on_disk = get_open_access_article_pdfs(download_dir=DATA_DIR)
    files_on_disk = [item for item in files_on_disk if item.is_on_disk]
    files_on_disk = [item for item in files_on_disk if item.language == "icelandic"]
    random.seed(1337)
    random.shuffle(files_on_disk)
    sample = files_on_disk[0]
    sample_path = sample.local_path.with_suffix(".txt")
    print("item_path:", sample_path)
    # print("NOISY_SAMPLE_TXT:", NOISY_SAMPLE_TXT)
    # with open(sample_path, "r") as fh:
    #     # text = fh.read()[: 10 ** 5]
    #     # print(text)
    #     text = fh.read()
    # for (idxs, sent) in segment_text(text):
    #     print(idxs, sent)


def pdfbox_to_text(pdf_path, txt_path):
    try:
        ret = subprocess.run(
            [
                "java",
                "-jar",
                str(_PDFBOX_PATH),
                "ExtractText",
                str(pdf_path),
                str(txt_path),
            ]
        )
        return ret.returncode == 0
    except KeyboardInterrupt:
        raise KeyboardInterrupt
    except Exception:
        print(ret)
        traceback.print_exc()
        return False
    return True


def tmp_dir_maintenance():
    os.makedirs(TMP_DIR, exist_ok=True)
    for fname in os.listdir(TMP_DIR):
        abs_path = TMP_DIR / fname
        os.remove(abs_path)


def main():
    tmp_dir_maintenance()
    gen_pdf()


if __name__ == "__main__":
    main()
