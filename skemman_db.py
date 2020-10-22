import sqlite3
import datetime

import config


class SkemmanDb:

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
            language TEXT,
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

    _SQL_CREATE_VIEW_OPEN_ACCESS_PDFS = """
    CREATE VIEW IF NOT EXISTS open_access_pdfs AS
        SELECT
            f.href AS file_href
            , fname
            , size
            , descr
            , dir
            , d.href AS document_href
            , title
            , is_local
        FROM skemman_files AS f INNER JOIN skemman_documents AS d
            ON f.document_id = d.id
        WHERE access = "Opinn" AND ftype = "PDF"
    """

    def __init__(self):
        self._DB_NAME = config.db_dir() / "skemman.db"
        conn = sqlite3.connect(self._DB_NAME)
        try:
            SkemmanDb._create_tables_views(conn)
        except Exception as e:
            print(e)
            print("Could not create tables")
            return
        self.conn = conn

    @classmethod
    def _create_tables_views(cls, conn):
        with conn as c:
            c.execute(cls._SQL_CREATE_SKEMMAN_DOCUMENTS)
            c.execute(cls._SQL_CREATE_SKEMMAN_MAP)
            c.execute(cls._SQL_CREATE_SKEMMAN_FILES)
            c.execute(cls._SQL_CREATE_SKEMMAN_PAGES)
            c.execute(cls._SQL_CREATE_VIEW_OPEN_ACCESS_PDFS)

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

    def insert_filelist(self, filelist, rel_dir, document_id):
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
        params = [
            (
                sfile.href,
                sfile.fname,
                sfile.size,
                sfile.access,
                sfile.descr,
                sfile.ftype,
                False,
                rel_dir,
                date_inserted,
                document_id,
            )
            for sfile in filelist
        ]
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

    def get_documents(self):
        sql = """SELECT * FROM skemman_documents"""
        try:
            with self.conn as c:
                cursor = c.execute(sql)
                docs = cursor.fetchall()
                return docs
        except Exception as e:
            print(e)

    def get_files(self):
        sql = """SELECT fname, size, access FROM skemman_files"""
        try:
            with self.conn as c:
                cursor = c.execute(sql)
                files = cursor.fetchall()
                return files
        except Exception as e:
            print(e)

    def get_open_pdfs(self):
        sql = """SELECT fname, size, FROM open_access_pdfs"""
        try:
            with self.conn as c:
                cursor = c.execute(sql)
                files = cursor.fetchall()
                return files
        except Exception as e:
            print(e)

    def get_filedocs(self):
        sql = """
            SELECT
                f.href AS file_href
                , fname
                , size
                , descr
                , access
                , dir
                , d.href AS document_href
                , title
                , is_local
                , document_id
                , language
            FROM skemman_files AS f
            INNER JOIN skemman_documents AS d ON f.document_id = d.id
        """
        try:
            with self.conn as c:
                cursor = c.execute(sql)
                files = cursor.fetchall()
                return files
        except Exception as e:
            print(e)

    def update_file_status(self, href, status):
        sql = """UPDATE skemman_files
            SET is_local = ? WHERE href = ?"""
        try:
            with self.conn as c:
                cursor = c.execute(sql, (status, href))
                return cursor
        except Exception as e:
            print(e)

    def update_file_language(self, href, language):
        sql = """UPDATE skemman_files
            SET language = ? WHERE href = ?"""
        try:
            with self.conn as c:
                cursor = c.execute(sql, (language, href))
                return cursor
        except Exception as e:
            print(e)
