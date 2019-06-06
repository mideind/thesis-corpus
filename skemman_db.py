import sqlite3
import datetime


class SkemmanDb:
    _DB_NAME = "skemman.db"

    _SQL_CREATE_SKEMMAN_DOCUMENTS = """CREATE TABLE IF NOT EXISTS skemman_documents (
            id INTEGER PRIMARY KEY,
            href TEXT,
            title TEXT,
            author TEXT,
            accepted TEXT,
            inserted TEXT,
            UNIQUE(href)
    )"""

    _SQL_CREATE_SKEMMAN_FILES = """CREATE TABLE IF NOT EXISTS skemman_files (
            id INTEGER PRIMARY KEY,
            href TEXT UNIQUE,
            fname TEXT,
            size TEXT,
            access TEXT,
            descr TEXT,
            ftype TEXT,
            is_local BOOL,
            dir TEXT,
            inserted TEXT,
            document_id INTEGER,
            UNIQUE(fname, document_id),
            FOREIGN KEY (document_id) REFERENCES skemman_documents (id)
    )"""

    _SQL_CREATE_SKEMMAN_MAP = """CREATE TABLE IF NOT EXISTS skemman_maps (
            id INTEGER PRIMARY KEY,
            key TEXT,
            value TEXT,
            document_id INTEGER,
            FOREIGN KEY (document_id) REFERENCES skemman_documents (id)
    )"""

    _SQL_CREATE_SKEMMAN_PAGES = """CREATE TABLE IF NOT EXISTS skemman_pages (
            page INTEGER NOT NULL,
            UNIQUE(page)
    )"""

    def __init__(self):
        conn = sqlite3.connect(self._DB_NAME)
        try:
            SkemmanDb._create_tables(conn)
        except Exception as e:
            print(e)
            print("Could not create tables")
            return
        self.conn = conn

    @classmethod
    def _create_tables(cls, conn):
        with conn as c:
            c.execute(cls._SQL_CREATE_SKEMMAN_DOCUMENTS)
            c.execute(cls._SQL_CREATE_SKEMMAN_MAP)
            c.execute(cls._SQL_CREATE_SKEMMAN_FILES)
            c.execute(cls._SQL_CREATE_SKEMMAN_PAGES)

    def insert_document(self, doc):
        sql = """INSERT INTO
            skemman_documents (href, title, author, accepted, inserted)
            VALUES (?, ?, ?, ?, ?)"""
        date_inserted = str(datetime.date.today())
        params = (doc.href, doc.title, doc.author, doc.accepted, date_inserted)
        try:
            with self.conn as c:
                return c.execute(sql, params)
        except Exception as e:
            print(e)
            print(f"Could not insert document {doc.href}")

    def insert_map(self, attr_map, document_id):
        sql = """INSERT INTO
            skemman_maps (key, value, document_id)
            VALUES (?, ?, ?)"""
        params = [(key, val, document_id) for (key, val) in attr_map.items()]
        try:
            with self.conn as c:
                return c.executemany(sql, params)
        except Exception as e:
            print(e)
            print(f"Could not insert map for {document_id}")

    def insert_filelist(self, filelist, rel_dir_path, document_id):
        sql = """INSERT INTO
            skemman_files (href, fname, size,
                           access, descr, ftype,
                           is_local, dir, inserted,
                           document_id)
            VALUES (?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?)"""

        date_inserted = str(datetime.date.today())
        params = [(
            sfile.href,
            sfile.fname,
            sfile.size,
            sfile.access,
            sfile.descr,
            sfile.ftype,
            False,
            rel_dir_path,
            date_inserted,
            document_id,
        ) for sfile in filelist]
        try:
            with self.conn as c:
                return c.executemany(sql, params)
        except Exception as e:
            print(e)

    def get_document_id_by_href(self, href):
        sql = """SELECT id FROM skemman_documents WHERE href = ?"""
        params = (href,)
        try:
            with self.conn as c:
                cursor = c.execute(sql, params)
                res = cursor.fetchone()
                return res[0] if res is not None else res
        except Exception as e:
            print(e)

    def insert_page(self, idx):
        sql = """INSERT INTO skemman_pages (page) VALUES (?)"""
        params = (idx,)
        try:
            with self.conn as c:
                return c.execute(sql, params)
        except Exception as e:
            print(e)

    def get_pages(self):
        sql = """SELECT page FROM skemman_pages"""
        try:
            with self.conn as c:
                cursor = c.execute(sql)
                pages = set([tup[0] for tup in cursor.fetchall()])
                return pages
        except Exception as e:
            print(e)

    def get_hrefs(self):
        sql = """SELECT href FROM skemman_documents"""
        try:
            with self.conn as c:
                cursor = c.execute(sql)
                pages = set([tup[0] for tup in cursor.fetchall()])
                return pages
        except Exception as e:
            print(e)

