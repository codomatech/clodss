'''
clodss is a data-structures on-disk store with an API largly compatible with
redis. The goal is to develop a store with the simplicity of the redis API
which scales beyond memory capacity, allows harnessing multi-core processors,
and does not burden accesses with network latency.
'''
# pylint: disable=E0402

import time
import os
from ilock import ILock
from .router import Router
from . import hashmaps
from . import lists
from . import keys


def wrapmethod(method, stats=None):
    '''
    - guards all clodss methods with a key-scoped lock
    - performs sanity checks on key
    - enures the key has not expired
    '''
    def wrapper(*args, **kwargs):
        if len(args) < 2:
            raise TypeError('too few parameters, `key` is required')
        instance = args[0]
        key = args[1]
        if '`' in key or '﹁' in key:
            raise ValueError('`key` contains invalid character(s)')
        if key.startswith('sqlite_'):
            key = f'﹁{key}'

        if stats is not None:
            t1 = time.perf_counter()
        with ILock(f'clodss-{key}', timeout=5):
            instance.checkexpired(key, enforce=True)
            result = method(*args, **kwargs)
        if stats is not None:
            t = time.perf_counter() - t1
            avg, n = stats.get(method.__name__, (0, 0))
            avg = (avg * n + t) / (n + 1)
            stats[method.__name__] = (avg, n + 1)
        return result
    wrapper.__name__ = method.__name__
    return wrapper


class StrictRedis:
    'main clodss class'
    def __init__(
            self, db: int = 0, sharding_factor: int = 3, base: str = 'data',
            decode_responses: bool = False, benchmark: bool = True) -> None:
        self.decode = decode_responses
        dbpath = os.path.join(os.getcwd(), base, '%02d' % db)
        os.makedirs(dbpath, exist_ok=True)
        self._dbpath = dbpath
        self.router = Router(dbpath, sharding_factor)
        self.knownkeys = {}
        self.keystoexpire = {}
        self._stats = {} if benchmark else None

        modules = [keys, lists, hashmaps]
        for module in modules:
            for attr in dir(module):
                if attr.startswith('_'):
                    continue
                method = getattr(module, attr)
                if not callable(method):
                    continue
                if attr == 'sēt':
                    # `set` is a reserved keyword
                    attr = 'set'
                setattr(StrictRedis, attr, wrapmethod(method, self._stats))

    def stats(self):
        'gets benchmarking statistics'
        return self._stats

    def dbpath(self):
        'gets base path for database files'
        return self.dbpath

    def checkexpired(self, key, enforce=False):
        'checks if a key expired and removes it if so'
        db = self.router.connection(key)
        try:
            exp = [self.keystoexpire.get(key)]
            if not exp[0]:
                query = 'SELECT time FROM `﹁expiredkeys﹁` WHERE key=?'
                exp = db.execute(query, (key,)).fetchone()
            if exp:
                t = exp[0]
                now = time.time()
                if now > t:
                    if not enforce:
                        return True
                    ekey = key.replace('"', '""')
                    tables = db.execute(
                        'SELECT name FROM sqlite_master WHERE '
                        f'type="table" AND name LIKE "{ekey}%"').fetchall()
                    queries = ';\n'.join([f'DROP TABLE `{table[0]}`'
                                          for table in tables])
                    db.executescript(queries)
                    db.commit()
                    if key in self.knownkeys:
                        del self.knownkeys[key]

                    query = 'DELETE FROM `﹁expiredkeys﹁` WHERE key=?'
                    db.execute(query, (key,))
                    db.commit()
                    return True
                return 'scheduled'
            return False
        finally:
            db.close()
