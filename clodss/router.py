# -*- coding: utf-8 -*-

'''
router.py: provides the Router class to map keys to a corresponding database.
routing is configured by a spread factor
'''

import hashlib
import os
import random
import uuid
import lsm

class DBConnection:
    'a database connection used within a pool'
    def __init__(self, fname):
        self._id = uuid.uuid1().hex
        self.free = True
        self._db = lsm.LSM(fname)
        self.fname = fname

    def db(self):
        'db object'
        return self._db

    def id(self):
        'connection id'
        return self._id


class Router:
    'main routing class, maps keys to a db based on a spread factor'
    EXT = 'clodssdb'

    def __init__(self, dbpath, factor=2, poolsize=3):
        '''
        dbpath: where to store the data files
        factor: partitioning factor, the higher it is, the more spread your
        data will be. this improves concurrency but also increases the number
        of open files. defaults to 2
        poolsize: pool size per data file, defaults to 3
        '''
        self.factor = factor
        self.dbpath = dbpath
        self.poolsize = poolsize
        self.pool = {}

    def _alldbs(self):
        return sorted([
            os.path.join(self.dbpath, f)
            for f in os.listdir(self.dbpath)
            if f.endswith(Router.EXT)
        ])

    def reset(self):
        'clears the database and closes all connections'
        for db in self._alldbs():
            os.unlink(db)
        self.pool = {}

    def connection(self, key: bytes):
        'gets a new connection'
        db = hashlib.sha1(key.encode('utf-8')).hexdigest()[:self.factor]
        dbs = self.pool.get(db)
        if not dbs:
            self.pool[db] = [
                DBConnection(os.path.join(self.dbpath, f'{db}.{Router.EXT}'))
                for _ in range(self.poolsize)
            ]
            dbs = self.pool[db]
        return random.choice(dbs)

    def allconnections(self, offset: int = 0):
        '''
        generator of connections to all available dbs
        offset: start offset in the alphabetically sorted list of dbs
        '''
        dbs = self._alldbs()[offset:]
        pooldbs = {dbs[0].fname: dbs[0] for dbs in self.pool.values()}
        for db in dbs:
            try:
                yield pooldbs[db]
            except KeyError:
                yield DBConnection(db)

    def poolstatus(self):
        'gets pooling status'
        return {k: [conn.free for conn in conns]
                for k, conns in self.pool.items()}
