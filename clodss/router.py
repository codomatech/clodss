# -*- coding: utf-8 -*-

'''
router.py: provides the Router class to map keys to a corresponding database.
routing is configured by a spread factor
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
        query = (
            'CREATE TABLE IF NOT EXISTS `﹁expiredkeys﹁` '
            '(key TEXT PRIMARY KEY, time REAL)'
        )
        conn.execute(query)
        conn.commit()
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
    'main routing class, maps keys to a db based on a spread factor'
    EXT = 'clodssdb'

    def __init__(self, dbpath, factor=2, poolsize=5):
        '''
        dbpath: where to store the data files
        factor: partitioning factor, the higher it is, the more spread your
        data will be. this improves concurrency but also increases the number
        of open files. defaults to 2
        poolsize: pool size per data file, defaults to 5
        '''
        self.factor = factor
        self.dbpath = dbpath
        self.poolsize = poolsize
        self.connections = {}

    def _alldbs(self):
        return sorted([
            os.path.join(self.dbpath, f)[:-len(Router.EXT)-1]
            for f in os.listdir(self.dbpath)
            if f.endswith(Router.EXT)
        ])

    def _acquireconnection(self, db):
        'given an encoded db name, returns a connection'
        fname = os.path.join(self.dbpath, f'{db}.{Router.EXT}')
        with ILock(f'clodss-{db}-lock', timeout=5):
            conns = self.connections.get(db)
            if not conns:
                conns = [DBConnection(fname) for _ in range(self.poolsize)]
                self.connections[db] = conns

            for _ in range(1024):
                for conn in conns:
                    if not conn.free:
                        continue
                    conn.acquire()
                    return conn
                time.sleep(0.5)

    def reset(self):
        'clears the database and closes all connections'
        for db in self._alldbs():
            try:
                [conn.close() for conn in self.connections[db]]
            except KeyError:
                pass
            fname = os.path.join(self.dbpath, f'{db}.{Router.EXT}')
            os.unlink(fname)
        self.connections = {}

    def connection(self, key: bytes):
        'gets a new connection'
        db = hashlib.sha1(key.encode('utf-8')).hexdigest()[:self.factor]
        return self._acquireconnection(db)

    def allconnections(self, offset: int = 0):
        '''
        generator of connections to all available dbs
        offset: start offset in the alphabetically sorted list of dbs
        '''
        dbs = self._alldbs()[offset:]
        for db in dbs:
            yield self._acquireconnection(db)

    def poolstatus(self):
        'gets pooling status'
        return {k: [conn.free for conn in conns]
                for k, conns in self.connections.items()}
