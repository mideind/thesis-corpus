import os
import time
import random
from pathlib import Path
from collections import Counter

# from shutil import copy
import shutil

try:
    from icecream import ic

    ic.configureOutput(includeContext=True)
except ImportError:  # Silently ignore if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

from reynir import bintokenizer
from tokenizer import paragraphs, mark_paragraphs
from tokenizer import correct_spaces

from skemman_db import SkemmanDb
from utils import get_open_access_article_pdfs

DATA_DIR = Path("/mnt/windows/thesiscorpus/data")
SAMPLE = DATA_DIR / Path(
    "/haskoli_islands/felagsvisindasvid/meistaraprofsritgerdir_-_felagsvisindasvid/1235-0_383.txt"
)
OUT_DIR = Path("/home/haukur/Projects/thesiscorpus/data")
NOISY_SAMPLE = Path("samples/sample_noisy.pdfbox.txt")

def toks_to_text(tokstream):
    return correct_spaces(" ".join([tok.txt for tok in tokstream if tok.txt]))


def segment_text(text):
    sent_offset = 1
    par_offset = 1
    # toks = bintokenizer.tokenize(mark_paragraphs(text))
    toks = bintokenizer.tokenize(text)
    paragraph_stream = paragraphs(toks)
    for (par_idx, paragraph) in enumerate(paragraph_stream):
        par_idx += par_offset
        for (rel_sent_idx, (offset, sentence)) in enumerate(paragraph):
            sent_idx = sent_offset + rel_sent_idx
            yield (par_idx, sent_idx), toks_to_text(sentence)


def copy_open_access_skemman_text_files():
    files_on_disk = get_open_access_article_pdfs(download_dir=DATA_DIR)
    files_on_disk = [item for item in files_on_disk if item.is_on_disk]
    files_on_disk = [item for item in files_on_disk if item.language == "icelandic"]
    for item in files_on_disk:
        item_path = Path(item.local_path.with_suffix(".txt"))
        out_path = OUT_DIR / item_path.relative_to(DATA_DIR)
        if item_path.exists():
            out_path.parent.mkdir(exist_ok=True, parents=True)
            shutil.copy(item_path, out_path)


def test_segment():
    # files_on_disk = get_open_access_article_pdfs(download_dir=DATA_DIR)
    # files_on_disk = [item for item in files_on_disk if item.is_on_disk]
    # files_on_disk = [item for item in files_on_disk if item.language == "icelandic"]
    # random.seed(1337)
    # random.shuffle(files_on_disk)
    # sample = files_on_disk[0]
    # sample_path = sample.local_path.with_suffix(".txt")
    sample_path = NOISY_SAMPLE
    with open(sample_path, "r") as fh:
        # text = fh.read()[: 10 ** 5]
        # print(text)
        text = fh.read()
    for (idxs, sent) in segment_text(text):
        print(idxs, sent)


def main():
    test_segment()


if __name__ == "__main__":
    main()
