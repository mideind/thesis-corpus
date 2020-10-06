import os
import time
from pathlib import Path
import subprocess
import traceback
from collections import Counter

try:
    from icecream import ic

    ic.configureOutput(includeContext=True)
except ImportError:  # Silently ignore if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

from skemman_db import SkemmanDb
from utils import get_open_access_article_pdfs

DATA_DIR = Path("/mnt/windows/thesiscorpus/data")


def convert_pdf_update_language_all():
    import random

    db = SkemmanDb()
    files_on_disk = get_open_access_article_pdfs(download_dir=DATA_DIR)
    files_on_disk = [item for item in files_on_disk if item.is_on_disk]
    random.shuffle(files_on_disk)
    total = len(files_on_disk)
    last_printed = -1
    for (idx, item) in enumerate(files_on_disk):
        pct = int(100 * idx / total)
        if pct % 5 == 0 and last_printed != pct:
            print(f"{pct:>3d}%")
            last_printed = pct
        path = item.local_path
        out_path = path.with_suffix(".txt")
        try:
            if pdf_to_text(path, out_path):
                language = predict_language(out_path)
                db.update_file_language(item.href, language)
        except KeyboardInterrupt:
            return
        except Exception:
            traceback.print_exc()


def predict_language(path):
    counter = Counter()
    with open(path, "r") as f:
        try:
            counter.update(f.read())
        except Exception:
            counter.update([""])
    exclude = ("", " ", "\n", ".", ",")
    for c in exclude:
        if c in counter:
            counter.pop(c)
    most_common = [
        char for (char, count) in counter.most_common(len(counter)) if char.isalpha()
    ]
    if "e" not in most_common:
        return "unknown"
    elif most_common.index("e") > 2:
        return "icelandic"
    return "english"


def pdf_to_text(path, out_path):
    try:
        ret = subprocess.run(["pdftotext", path, out_path])
    except KeyboardInterrupt:
        return
    except Exception:
        print(ret)
        traceback.print_exc()
        return False
    return True


def main():
    convert_pdf_update_language_all()


if __name__ == "__main__":
    main()
