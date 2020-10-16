
import os
from pathlib import Path

# This will be set by main.py
# (but maybe not in tests? bah.)
data_dir = Path("./data/")
scrape_delay = 1.0

pdfbox_path = "/home/petur/Downloads/pdfbox-app-2.0.21.jar"



def db_dir():
    _d = data_dir / "db"
    os.makedirs(_d, exist_ok = True)
    return _d

def pdf_dir():
    _d = data_dir / "pdf"
    os.makedirs(_d, exist_ok = True)
    return _d

def tmp_dir():
    # TODO: use OS tmpdir mechanisms
    _d = Path("/tmp") / "thesiscorpus"
    os.makedirs(_d, exist_ok = True)
    return _d

