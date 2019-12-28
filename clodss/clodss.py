'''
clodss is a data-structures on-disk store with an API largly compatible with
redis. The goal is to develop a store with the simplicity of the redis API
which scales beyond memory capacity, allows harnessing multi-core processors,
and does not burden accesses with network latency.
'''

import time
import os
from ilock import ILock
from router import Router
import lists
import keys


def wrapmethod(method, stats=None):
    'guards all clodss methods with a key-scoped lock'
    def wrapper(*args, **kwargs):
        key = args[1]
        if stats is not None:
            t1 = time.perf_counter()
        with ILock(f'clodss-{key}', timeout=5):
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
        self.knownkeys = set()
        self._stats = {} if benchmark else None

        modules = [keys, lists]
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
