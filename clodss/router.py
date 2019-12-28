'''
router.py: provides the Router class to map keys to a corresponding database.
routing is configured by a sharding factor
'''

import os
import sqlite3
import hashlib
import time
import uuid
from ilock import ILock


class DBConnection:
    'a database connection used within a pool'
    def __init__(self, fname):
        self.id = uuid.uuid1().hex
        self.free = True
        conn = sqlite3.connect(fname, check_same_thread=False)
        conn.execute('PRAGMA SYNCHRONOUS=OFF')
        conn.execute('PRAGMA CACHE_SIZE=500')
        self._conn = conn

    def acquire(self):
        'acquires this connection'
        self.free = False

    def cursor(self):
        'sqlite3 cursor'
        return self._conn.cursor()

    def execute(self, *args, **kwargs):
        'sqlite3 execute'
        return self._conn.execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        'sqlite3 executemany'
        return self._conn.executemany(*args, **kwargs)

    def executescript(self, *args, **kwargs):
        'sqlite3 executescript'
        return self._conn.executescript(*args, **kwargs)

    def commit(self):
        'sqlite3 commit'
        self._conn.commit()

    def close(self):
        'frees this connection'
        self.free = True


class Router:
    'main routing class, maps keys to a db based on a sharding factor'
    def __init__(self, dbpath, factor=3):
        self.factor = factor
        self.dbpath = dbpath
        self.connections = {}

    def connection(self, key: bytes):
        'gets a new connection'
        db = hashlib.sha1(key.encode('utf-8')).hexdigest()[:self.factor]
        fname = os.path.join(self.dbpath, f'{db}.db')

        with ILock(f'clodss-{db}-lock', timeout=5):
            conns = self.connections.get(db)
            if not conns:
                conns = [DBConnection(fname) for _ in range(50)]
                self.connections[db] = conns

            for _ in range(1024):
                for conn in conns:
                    if not conn.free:
                        continue
                    conn.acquire()
                    return conn
                time.sleep(0.5)

    def poolstatus(self):
        'gets pooling status'
        return {k: [conn.free for conn in conns]
                for k, conns in self.connections.items()}
