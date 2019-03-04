import os
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("start of script")


_DB_FNAME = "database.tsv"
try:
    _DB_DIR = os.path.dirname(os.path.realpath(__file__))
except NameError as e:
    _DB_DIR = "/tmp/thesiscorpus"
    if not os.path.isdir(_DB_DIR):
        os.mkdir(_DB_DIR)

_DB_PATH = os.path.join(_DB_DIR, _DB_FNAME)

class Database:

    def __init__(self):
        """docstring"""
        self._db = None
        self._data = None
        self._modified = False
        logger.debug("initializing db")

    def _read_db(self):
        logger.debug("reading db from file")
        with open(_DB_PATH, "r") as fh:
            lines = [line.strip("\n").split("\t") for line in fh]
            keys, *_ = zip(*lines)
            self._data = dict(zip(keys, lines))

    def _save_to_file(self):
        logger.debug("saving db to file")
        with open(_DB_PATH, "w") as fh:
            if self._data is not None:
                for tup in self._data.values():
                    fh.write("\t".join(tup) + "\n")

    def store(self, tup):
        key, *rest = tup
        if key not in self._data:
            logger.info(f"storing {key}")
            self._data[key] = tup
            self._modified = True

    def __enter__(self):
        logger.debug("entering")
        if self._db is None:
            self._read_db()
        return self

    def __exit__(self, type, value, traceback):
        logger.debug("exiting")
        if self._modified:
            logger.debug("db has been modified")
            self._save_to_file()
        return False
