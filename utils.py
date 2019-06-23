from pprint import pprint
from collections import namedtuple
import os
from pathlib import Path
import re

try:
    from icecream import ic

    ic.configureOutput(includeContext=True)
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

# from thesis_scraper import SkemmanFile
import thesis_scraper
from skemman_db import SkemmanDb


_PROJECT_DIR = Path(os.path.realpath("__file__")).parent
DATA_DIR = _PROJECT_DIR / "data"

SUBS = {
    "á": "a",
    "ð": "d",
    "é": "e",
    "í": "i",
    "ó": "o",
    "ú": "u",
    "ý": "y",
    "þ": "th",
    "æ": "ae",
    "ö": "o",
    "Á": "A",
    "Ð": "D",
    "É": "E",
    "Í": "I",
    "Ó": "O",
    "Ú": "U",
    "Ý": "Y",
    "Þ": "TH",
    "Æ": "AE",
    "Ö": "O",
    " ": "_",
    ",": ".",
}
SUBS = tuple(SUBS.items())

B_IN_KB = 10 ** 3
B_IN_MB = 10 ** 6
B_IN_GB = 10 ** 9

UNITS = {" B": 1, "kB": B_IN_KB, "MB": B_IN_MB, "GB": B_IN_GB}
UNIT_NAMES = list(UNITS.keys())

MAX_SIZE_IN_MB = 75


def size_to_mb(size_str):
    unit = list(filter(lambda key: key in size_str, UNIT_NAMES)).pop()
    factor = UNITS[unit]
    size_in_b = float(size_str.replace(unit, "").strip()) * factor
    return round(size_in_b / B_IN_MB, 3)


def transliterate_path(text):
    out = text
    for sub in SUBS:
        out = out.replace(*sub)
    return out


def filter_open_access_main_pdfs(
    item_list, limit_file_size=True, verbose=False, dump=False
):
    accum = 0
    unfiltered = []
    files_out = []
    files_blocked = []
    investigate = []
    descr_blacklist = [
        "forsíða",
        "forsida",
        "útdráttur",
        "úrdráttur",
        "ágrip",
        "efnisyfirlit",
        "efnisskrá",
        "heimildaskrá",
        "heimildarskrá",
        "heimildir",
        # "samantekt",
        "yfirlýsing",
        "viðtal",
        "kápa",
        "titilsíða",
        "abstract",
        "beiðni um lokun",
        "samþykki",
        "leyfisbr",
        "teikning",
        # handle maybe
        "þakkir",
        "þakkar",
        "thakkir",
        "thakkar",
        # handle later
        "fylgiskjal",
        "fylgiskjöl",
        "fylgirit",
        "viðauki",
        "viðaukar",
        "lokun",
    ]
    whitelist = [
        "heild",
        "ritgerð",
        "ritgerd",
        "greinagerð",
        "greinargerð",
        "lokaverkefni",
        "handrit",
        "handbok",
        "handbók",
        "lokaverkefni",
        "bækling",
        "skýrsla",
        "skyrsla",
        "meginmál",
        "meginmal",
    ]
    lname_pat = re.compile(  # lower name pattern
        r"""
        (\b|_)
        (ba|b\.a|bs|b\.s|bsc|b\.sc|b\.ed|ma|m\.a|ms|m\.s|msc|m\.sc)
        (\b|_)
    """,
        re.VERBOSE,
    )

    for (
        file_href,
        fname,
        size,
        descr,
        access,
        rel_dir,
        doc_href,
        title,
        is_local,
            doc_id,
    ) in item_list:
        lname = fname.lower()
        ldescr = descr.lower().strip()
        size_in_mb = size_to_mb(size)
        file_tup = (doc_href, size_in_mb, ldescr, fname, title)
        item_tup = (
            file_href,
            fname,
            size_in_mb,
            descr,
            access,
            rel_dir,
            doc_href,
            title,
            is_local,
            doc_id,
        )

        if limit_file_size and size_in_mb > MAX_SIZE_IN_MB:
            files_blocked.append(file_tup)
            continue

        if (
            ".pdf" in lname
            and "Opinn" in access
            and (
                any(word in ldescr for word in whitelist)
                or any(word in lname for word in whitelist)
                or lname_pat.search(lname)
            )
        ):
            # whitelist
            accum += size_in_mb
            files_out.append(item_tup)
        elif (
            "Opinn" not in access
            or ".pdf" not in lname
            or any(word in ldescr for word in descr_blacklist)
        ):
            # blacklist
            files_blocked.append(file_tup)
        elif not ldescr:
            investigate.append(file_tup)
        else:
            unfiltered.append(file_tup)

    del item_list
    files_out = sorted(files_out)

    if verbose:
        ic(len(files_out))
        ic(len(files_blocked))
        ic(len(investigate))
        ic(len(unfiltered))
        ic(round(accum, 1))

    if dump:
        with open("dump.tmp", "w") as f:
            for row in files_out:
                item_tup = (
                    file_href,
                    fname,
                    size_in_mb,
                    descr,
                    access,
                    rel_dir,
                    doc_href,
                    title,
                ) = row
                f.write(
                    f"href:  {file_href}\n"
                    f"size:  {size} MB\n"
                    f"descr: {descr}\n"
                    f"fname: {fname}\n"
                    f"title: {title}\n"
                    "\n"
                )
    return files_out


def get_open_access_article_pdfs(download_dir=None):
    db = SkemmanDb()
    files = db.get_filedocs()
    files_out = [
        thesis_scraper.SkemmanFile(
            doc_href,
            file_href,
            fname=fname,
            size=size,
            access=access,
            descr=descr,
            is_local=is_local,
            ftype=".pdf",
            rel_dir=rel_dir,
            base_dir=download_dir,
            doc_id=doc_id,
        )
        for (
            file_href,
            fname,
            size,
            descr,
            access,
            rel_dir,
            doc_href,
            title,
            is_local,
            doc_id,
        ) in filter_open_access_main_pdfs(files)
    ]
    return files_out


def stats_open_access_article_pdfs():
    db = SkemmanDb()
    files = db.get_filedocs()
    files_out = filter_open_access_main_pdfs(files, verbose=True, dump=True)


def main():
    stats_open_access_article_pdfs()


if __name__ == "__main__":
    main()
