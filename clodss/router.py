import os
import sqlite3
import hashlib
import time
import uuid
from cuttlepool import CuttlePool
from ilock import ILock

class DBConnection:
    def __init__(self, fname):
        self.id = uuid.uuid1().hex
        self.free = True
        conn = sqlite3.connect(fname, check_same_thread=False)
        conn.execute('PRAGMA SYNCHRONOUS=OFF')
        conn.execute('PRAGMA CACHE_SIZE=500')
        self._conn = conn

    def acquire(self):
        self.free = False

    def cursor(self):
        return self._conn.cursor()

    def execute(self, *args, **kwargs):
        return self._conn.execute(*args, **kwargs)

    def commit(self):
        self._conn.commit()

    def close(self):
        self.free = True


class SQLitePool(CuttlePool):

    def normalize_resource(self, conn):
        conn.row_factory = None
        conn.execute('pragma journal_mode=wal')
        conn.execute('PRAGMA CACHE_SIZE=500')
        #conn.execute('PRAGMA LOCKING_MODE=EXCLUSIVE')

    def ping(self, conn):
        try:
            rv = conn.execute('SELECT 1').fetchall()
            return (1,) in rv
        except sqlite3.Error:
            return False


class Router:
    def __init__(self, dbpath, factor=3, pooling='default'):
        self.factor = factor
        self.dbpath = dbpath
        self.pooling = pooling
        self.connections = {}

    def connection(self, key: bytes):
        db = hashlib.sha1(key.encode('utf-8')).hexdigest()[:self.factor]
        fname = os.path.join(self.dbpath, f'{db}.db')

        if self.pooling == 'cuttlepool':
            pool = self.connections.get(db)
            if not pool:
                pool = SQLitePool(factory=sqlite3.connect, database=fname, capacity=50)
                self.connections[db] = pool
            return pool.get_resource()

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
