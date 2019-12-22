import os
import sqlite3
import hashlib


class Router:
    def __init__(self, dbpath, factor=3):
        self.factor = factor
        self.dbpath = dbpath

    def connection(self, key: bytes):
        # TODO pooling
        db = hashlib.sha1(key.encode('utf-8')).hexdigest()[:self.factor]
        conn = sqlite3.connect(
            os.path.join(self.dbpath, f'{db}.db'),
            check_same_thread=False
        )
        conn.execute('pragma journal_mode=wal')
        return conn
