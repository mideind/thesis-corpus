import sys, subprocess
import traceback
import tempfile
import random
import re
import time

from alternative_extractions import extract_with_pdfbox

try:
    from icecream import ic

    ic.configureOutput(includeContext=True)
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

import tokenizer

import sqlalchemy
import sqlalchemy.sql
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Sequence

import config

"""
.mode lines
select count(document_id) from segments where text like "%Abstract%";
select count(document_id) from segments where text like "%Ágrip%";


document_id 4381, Anna Dís, Endurhæfing og eftirfylgd
haskoli akureyrar
abstract without section heading (icelandic and english abstracts)

should also look at
Lykilorð -- Lykilhugtök -- Keywords -- Key words


(Abstrakt|ABSTRAKT|Ágrip|ÁGRIP|Útdráttur|ÚTDRÁTTUR)  -- 18603
(Lykilorð:)  -- 2495
(Lykilorð:)  -- exclude (Abstrakt|...) -- 117
(Lykilorð)   -- 3123
(Lykilorð)   -- exclude (Abstrakt|...) -- 143


comm -12 (Abstrakt|ABSTRAKT|...) (Abstract|ABSTRACT)  -- 8853
comm -13 (Abstrakt|ABSTRAKT|...) (Abstract|ABSTRACT)  -- 9750
comm -23 (Abstrakt|ABSTRAKT|...) (Abstract|ABSTRACT)  -- 2185


Find first mention of
(Abstrakt|ABSTRAKT|Ágrip|ÁGRIP|Útdráttur|ÚTDRÁTTUR)
grab til next formfeed?

Find first mention of
(Abstract|ABSTRACT)
grab til next formfeed?

Require minimum line count
Some theses have abstracts that are longer than one page
Some theses have abstracts on pages that are not numbered
Some theses have abstracts on pages in english-icelandic order instead of icelandic-english, despite thesis language being icelandic
Sometimes there isnt an empty line separating the two abstracts
Require that first abstract/abstrakt occurrence is within 10 pages of start
Some papers have "ÁGRIP Á ÍSLENSKU"
Some paper have abstracts preceding the table of content and the abstracts are not mentioned in the table

Some papers have flow-issues inside the abstracts, such that for parts of it, only a single word appears on each line
    and they are separated by empty lines
    e.g. Kítósanafleiður með bakteríudrepandi verkun
Some papers have abstracts that are multiple (proper, line-separated) paragraphs

Some papers have weird encoding errors like "Sjálfboðaliðaferðamennska hvatar, væntingar og áhrif"


Heuristic: document has keywords/lykilorð/efnisorð that follows abstract/abstrakt then do not read further than that
Heuristic: read until sentence end


preprocessing:
remove lines which seem to be page numbers, e.g. they start with
    vii
    _ _ _ _ _ vii
    7
line splits due to text reflow
double ff ligature in some documents have been converted to the control code ^[

minimum char count, maximum char count
minimum line count, maximum line count
minimum average word length (over whole abstract)
maximum word length
max count of '!'

expect capital letter at start of abstract
expect punctuation at end of abstract

post processing:


######## Alignment

try aligning with bleualign: https://github.com/rsennrich/Bleualign/blob/master/bleualign/align.py

also possible to use noisy channel score of 1-1 and 2-1/1-2 matching i.e. translation score is
    1/T' log_prob p(s|t) + 1/T log_prob p(t|s)
    also possible to use length penalty for normalized scores (instead of average token log probs)
    score(p(s|t))/2 + score(s|t)/2
not clear if relative scoring of score(t|s) and score(t'|s) where t' is from beam decoding is better than regular noisy channel
    it would probably be more accurate to instead use beam_size=20 (or more) and pick the beam that has highest bleu with ground truth targets
    and use that as relative measure

how does hunalign/gale-church/bleualign handle adjacent swapping e.g. true alignment is (1a 2b 3c) but input is (123 bac)
    what about more distant ones like (12345 adbce) where ground-truth is (1a 2b ...)

"""

PATHS_FILE = config.data_dir / "both_abstracts.paths"


def read_file(path):
    lines = []
    with open(path) as fp:
        for line in fp:
            lines.append(line.rstrip())
    return lines


PATHS = [config.data_dir / path for path in read_file(PATHS_FILE)]
SAMPLE_INDEX = 1
SAMPLE_PATH = PATHS[SAMPLE_INDEX]
ENG_START_PATS = ("Abstract", "ABSTRACT")
ISL_START_PATS = ("Abstrakt", "ABSTRAKT", "Ágrip", "ÁGRIP", "Útdráttur", "ÚTDRÁTTUR")
ISL_END_KWORDS = (
    "Lykilhugtök",
    "Lykil hugtök",
    "Lykilorð",
    "Lykil orð",
    "Efnisorð",
    "Efnis orð",
    ".....",
    "Formáli",
    "Efnisyfirlit",
)
ENG_END_KWORDS = ("Keywords", "Key words", ".....", "Table of contents", "Key terms")

ISL_END_PATS = ISL_END_KWORDS + ENG_START_PATS
ENG_END_PATS = ENG_START_PATS + ISL_START_PATS + ISL_END_PATS

NUMERAL_RX = re.compile("^([iIvVxX0-9])+$")
HLINE = 88 * "#"


def as_pages(stream):
    page = []
    for line in stream:
        if not line.startswith("\x0c"):
            page.append(line)
            continue
        if page:
            yield page
            page = [line.strip()]


def render_file(path):
    try:
        pager = subprocess.Popen(
            ["less", "-F", "-R", "-S", "-X", "-K"],
            stdin=subprocess.PIPE,
            stdout=sys.stdout,
        )
        line_idx = -1
        for idx, page in enumerate(as_pages(read_file(path))):
            pager.stdin.write(
                b"----------------------------------------------------------------------------------------\n"
            )
            for line in page:
                line_idx += 1
                pager.stdin.write(line.encode("utf8"))
                pager.stdin.write(b"\n")
        pager.stdin.flush()
        pager.stdin.close()
        pager.wait()
    except KeyboardInterrupt:
        pass
    except BrokenPipeError:
        pass


def paged_render(text):
    try:
        pager = subprocess.Popen(
            ["less", "-F", "-R", "-S", "-X", "-K"],
            stdin=subprocess.PIPE,
            stdout=sys.stdout,
        )
        pager.stdin.write(text.encode("utf8"))
        pager.stdin.write(b"\n")
        pager.stdin.flush()
        pager.stdin.close()
        pager.wait()
    except KeyboardInterrupt:
        pass
    except BrokenPipeError:
        pass


def get_abstracts_from_text(text):
    lines = text.split("\n")

    def get_contiguous_text_at_pat(lines, patterns):
        start_idx = None
        for line_idx, line in enumerate(lines):
            for substring in patterns:
                if substring in line and not "....." in line:
                    start_idx = line_idx
        if start_idx == None:
            return None
        return_lines = []
        for offset, line in enumerate(lines[start_idx:]):
            if line.strip():
                return_lines.append(line)
            else:
                break
        return return_lines

    eng = get_contiguous_text_at_pat(lines, ENG_START_PATS)
    isl = get_contiguous_text_at_pat(lines, ISL_START_PATS)
    return eng, isl


class Document:
    def __init__(self, path):
        """docstring"""
        self.path = path
        self.pages = None

    def _maybe_get_pages(self):
        if self.pages is None:
            self.pages = list(as_pages(read_file(self.path)))

    def find_abstracts(self):
        self._maybe_get_pages()
        # (abstrakt|abstrakt|ágrip|ágrip|útdráttur|útdráttur)  -- 18603
        # (abstract|abstract)
        eng_occurr = set()
        isl_occurr = set()
        for page_idx, page in enumerate(self.pages):
            for substring in ENG_START_PATS:
                if any(substring in line for line in page):
                    eng_occurr.add(page_idx)
            for substring in ISL_START_PATS:
                if any(substring in line for line in page):
                    isl_occurr.add(page_idx)
        return sorted(list(eng_occurr)), sorted(list(isl_occurr))

    def render_page(self, page_idx):
        self._maybe_get_pages()
        return "\n".join(self.pages[page_idx])

    def draw(self):
        self._maybe_get_pages()
        try:
            pager = subprocess.Popen(
                ["less", "-F", "-R", "-S", "-X", "-K"],
                stdin=subprocess.PIPE,
                stdout=sys.stdout,
            )
            line_idx = -1
            for idx, page in enumerate(self.pages):
                pager.stdin.write(
                    b"----------------------------------------------------------------------------------------\n"
                )
                for line in page:
                    line_idx += 1
                    pager.stdin.write(line.encode("utf8"))
                    pager.stdin.write(b"\n")
            pager.stdin.flush()
            pager.stdin.close()
            pager.wait()
        except KeyboardInterrupt:
            pass
        except BrokenPipeError:
            pass

    def extract_pdfbox(self):
        if self.path is not None:
            tfile = tempfile.NamedTemporaryFile(suffix="txt")
            extract_with_pdfbox(self.path.with_suffix(".pdf"), tfile.name)
            text = tfile.read().decode("utf8")
            return text
        raise ValueError


def line_empty_or_numeral(line):
    line = line.strip()
    if line == "":
        return True
    return NUMERAL_RX.match(line)


def line_has_pat(line, pats):
    for substring in pats:
        if substring.lower() in line.lower():
            # ic(substring)
            return True
    return False


class PdfBoxOutput:
    def __init__(self, pdfbox_output):
        """docstring"""
        self.text = pdfbox_output
        self.lines = self.text.split("\n")
        if self.lines == [""]:
            self.lines = []
        self.eng_offsets = []
        self.isl_offsets = []
        self.max_abstract_len = 100  # lines
        self.max_num_empty = 2  # lines

    def has_text(self):
        return not not self.lines

    def render_all(self):
        paged_render(self.text)

    def find_abstracts_starts(self):
        # (abstrakt|abstrakt|ágrip|ágrip|útdráttur|útdráttur)  -- 18603
        # (abstract|abstract)
        def find_pattern_occurrences(lines, patterns):
            idxs = []
            for line_idx, line in enumerate(lines):
                for substring in patterns:
                    if substring in line and not "....." in line:
                        idxs.append(line_idx)
            return idxs

        eng = find_pattern_occurrences(self.lines, ENG_START_PATS)
        if not eng:
            return None
        isl = find_pattern_occurrences(self.lines, ISL_START_PATS)
        if not isl:
            return None
        self.eng_offsets = eng
        self.isl_offsets = isl
        return eng, isl

    def render_at_offsets(self):
        self.find_abstracts_starts()
        if not self.isl_offsets:
            raise ValueError("")
        isl_start = self.isl_offsets[0]
        eng_start = self.eng_offsets[0]
        output = [
            f"isl: {self.isl_offsets}",
            f"eng: {self.eng_offsets}",
        ]
        output.extend([HLINE] * 3)
        output.extend(self.lines[isl_start : isl_start + self.max_abstract_len])
        output.extend([HLINE] * 3)
        output.extend(self.lines[eng_start : eng_start + self.max_abstract_len])
        paged_render("\n".join(output))

    def find_abstract_end_isl(self):
        if not self.isl_offsets:
            raise ValueError("")
        num_empty = 0
        start = self.isl_offsets[0]
        end = min(start + self.max_abstract_len, len(self.lines) - start)
        # ic(end)
        for line_idx, line in enumerate(self.lines[start + 1 : end], start + 1):
            # ic(line)
            curr_is_empty = line.strip() == ""
            # num_empty = num_empty + 1 if line_empty_or_numeral(line) else 0
            num_empty = num_empty + 1 if line.strip() == "" else 0
            # if NUMERAL_RX.match(line.strip()):
            #     num_empty = 0
            if (num_empty >= self.max_num_empty) or line_has_pat(line, ISL_END_PATS):
                end = line_idx
                break
        # ic(end)
        return end

    def find_abstract_end_eng(self):
        if not self.eng_offsets:
            raise ValueError("")
        start = self.eng_offsets[0]
        end = min(start + self.max_abstract_len, len(self.lines) - start)
        num_empty = 0
        # ic(end)
        for line_idx, line in enumerate(self.lines[start + 1 : end], start + 1):
            # ic(line)
            # num_empty = num_empty + 1 if line_empty_or_numeral(line) else 0
            num_empty = num_empty + 1 if line.strip() == "" else 0
            # if NUMERAL_RX.match(line.strip()):
            #     num_empty = 0
            if (num_empty >= self.max_num_empty) or line_has_pat(line, ENG_END_PATS):
                end = line_idx
                break
        # ic(end)
        return end

    def render_abstracts_v1(self):
        self.find_abstracts_starts()
        if not self.isl_offsets:
            raise ValueError("")
        isl_start = self.isl_offsets[0]
        isl_end = self.find_abstract_end_isl()
        eng_start = self.eng_offsets[0]
        eng_end = self.find_abstract_end_eng()
        output = [
            f"isl: {self.isl_offsets}   end: {isl_end}",
            f"eng: {self.eng_offsets}   end: {eng_end}",
        ]
        output.extend([HLINE] * 3)
        output.extend(self.lines[isl_start:isl_end])
        output.extend([HLINE] * 3)
        output.extend(self.lines[eng_start:eng_end])
        # input("Press enter to render...")
        paged_render("\n".join(output))

    def get_abstracts(self):
        self.find_abstracts_starts()
        if not self.isl_offsets or not self.eng_offsets:
            raise ValueError("")
        isl_start = self.isl_offsets[0]
        isl_end = self.find_abstract_end_isl()
        eng_start = self.eng_offsets[0]
        eng_end = self.find_abstract_end_eng()
        isl_abs = self.lines[isl_start:isl_end]
        eng_abs = self.lines[eng_start:eng_end]
        return "\n".join(eng_abs), "\n".join(isl_abs)


class AbstractsDb:
    def __init__(self):
        self.db_file = config.db_dir() / "abstracts.db"
        self.db_url = "sqlite:///" + str(self.db_file)

        self.engine = sqlalchemy.create_engine(self.db_url)
        metadata = sqlalchemy.MetaData()
        self.abstracts = sqlalchemy.Table(
            "abstracts",
            metadata,
            # Column("id", sqlalchemy.Integer, start=1, autoincrement=True, unique=True),
            Column(
                "path", sqlalchemy.String, unique=True, nullable=False, primary_key=True
            ),
            Column("isl", sqlalchemy.String, unique=True, nullable=False),
            Column("eng", sqlalchemy.String, unique=True, nullable=False),
        )
        self.segmented = sqlalchemy.Table(
            "segmented",
            metadata,
            Column("id", sqlalchemy.Integer, autoincrement=True, primary_key=True),
            Column("path", sqlalchemy.String, unique=True, nullable=False),
            Column("eng", sqlalchemy.String, unique=True, nullable=False),
            Column("isl", sqlalchemy.String, unique=True, nullable=False),
        )
        metadata.create_all(self.engine)

    def insert(self, path, eng=None, isl=None):
        if eng is None or isl is None:
            raise ValueError("Value eng or isl cannot be None")
        path = str(path)
        with self.engine.begin() as connection:
            result = connection.execute(
                self.abstracts.insert(),
                path=path,
                isl=isl,
                eng=eng,
            )

    def insert_segmented(self, path, eng=None, isl=None):
        if eng is None or isl is None:
            raise ValueError("Value eng or isl cannot be None")
        path = str(path)
        with self.engine.begin() as connection:
            result = connection.execute(
                self.segmented.insert(),
                path=path,
                isl=isl,
                eng=eng,
            )

    def contains_path(self, path):
        path = str(path)
        abs_table = self.abstracts
        path_col = abs_table.c.path
        with self.engine.begin() as connection:
            result = connection.execute(
                abs_table.select().where(
                    path_col == path,
                ),
            )
            return result.fetchone() is not None

    def get_all_items(self):
        abs_table = self.abstracts
        path_col = abs_table.c.path
        with self.engine.begin() as connection:
            result = connection.execute(abs_table.select())
            return list(result.fetchall())


def segment_abstract(text):
    # text = tokenizer.mark_paragraphs(text)
    text = text.split("\n")[1:]
    if not text:
        return ""
    if not text[0].strip():
        text = text[1:]
    text = "\n".join(text)
    # text = "[[ " + " ]] [[ ".join(text.split("\n\n")) + " ]]"
    # print(text)
    text = text.replace("\n", " ")
    toks = list(tokenizer.tokenize(text))
    pgs = list(tokenizer.paragraphs(toks))
    sents = []
    for pg in pgs:
        for (tok_offset, sent) in pg:
            sent = list(sent)
            sent = tokenizer.detokenize(sent)
            sents.append(sent)
            # print(tok_offset, sent)
    text = "\n".join(sents)
    return text


# doc.draw()
# res = doc.extract_pdfbox()
# paged_render(res)

db = AbstractsDb()
items = db.get_all_items()
random.seed(12345)
random.shuffle(items)
no_punct = 0
eng_out = open("debug.eng", "w")
isl_out = open("debug.isl", "w")
for (path, isl, eng) in items:
    if len(isl) < 50 or len(eng) < 50:
        print("skipped chars")
        # no abstract under 50 chars allowed
        continue
    isl = segment_abstract(isl)
    eng = segment_abstract(eng)
    if not isl.strip() or not eng.strip():
        continue
    isl_line_count = isl.count("\n")
    eng_line_count = eng.count("\n")
    if (isl_line_count < 3 or 150 < isl_line_count) or (
        eng_line_count < 3 or 150 < eng_line_count
    ):
        print("skipped line count")
        continue
    PUNCTS = "!?."
    last_isl = isl.split("\n")[-1]
    last_eng = eng.split("\n")[-1]
    last = no_punct
    if last_isl[-1] not in PUNCTS and not (len(last_isl.split(" ")) < 3):
        # ic(path)
        # ic(last_isl)
        # no_punct += 1
        print("isl punct")
        continue
    elif last_eng[-1] not in PUNCTS and not (len(last_eng.split(" ")) < 3):
        # ic(path)
        # ic(last_eng)
        # no_punct += 1
        print("eng punct")
        continue
    # ic(no_punct)

    eng_out.write(eng)
    eng_out.write("\n\n")
    isl_out.write(isl)
    isl_out.write("\n\n")

    # output = [path]
    # output.extend([HLINE] * 3)
    # output.extend([isl])
    # output.extend([HLINE] * 3)
    # output.extend([eng])
    # paged_render("\n".join(output))

    # input("Next?")

    # if last != no_punct:
    #     ic(no_punct)
    #     input("Next?")

    # break
    # pass
eng_out.close()
isl_out.close()

# try:
#     db.insert("mypath", "myisl", "myeng")
# except sqlalchemy.exc.IntegrityError as e:
#     pass
# print(db.contains_path("mypath"))
# print(db.contains_path("mypath1"))
# sys.exit(0)


def do_extract_all():
    random.seed(1234567)
    paths = list(PATHS)
    random.shuffle(paths)
    for sample_idx, doc_path in enumerate(paths):
        if db.contains_path(doc_path):
            ic("skipping:", doc_path)
            continue
        ic("processing:", doc_path)
        doc = Document(doc_path)
        text_pdfbox = doc.extract_pdfbox()
        obj = PdfBoxOutput(text_pdfbox)
        found = obj.find_abstracts_starts()

        if not obj.has_text() or not found:
            ic("could not find text or abstract")
            continue

        # # if not found:
        # #     with open("not_found_log.txt", "a") as fp:
        # #         fp.write(f"could not find abstracts but has text '{docpath}'\n")
        # #     print()
        # #     input(f"Abstract not found: rendering {len(obj.lines)} lines")
        # #     import pdb; pdb.set_trace()
        # #     obj.render_all()
        # obj.render_abstracts_v1()

        eng_abs, isl_abs = obj.get_abstracts()
        try:
            db.insert(doc_path, eng=eng_abs, isl=isl_abs)
        except sqlalchemy.exc.IntegrityError as e:
            print(e)
            continue

        with open("extraction_abstracts.log", "a") as fp:
            ic("inserted")
            fp.write(f"inserted: '{doc_path}'\n")

        # input()
        # prevent overheating
        time.sleep(3)
