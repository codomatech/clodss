import time
import os
from router import Router
from ilock import ILock, ILockException
import lists


def wrapmethod(method, stats=None):
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
    def __init__(
            self, db: int = 0, sharding_factor: int = 3, base: str = 'data',
            decode_responses: bool = False, benchmark: bool = True) -> None:
        self.decode = decode_responses
        dbpath = os.path.join(os.getcwd(), base, '%02d' % db)
        os.makedirs(dbpath, exist_ok=True)
        self.router = Router(dbpath, sharding_factor)
        self._knownkeys = set()
        self._stats = {} if benchmark else None

        modules = [lists]
        for module in modules:
            for attr in dir(module):
                if attr.startswith('_'):
                    continue
                method = getattr(module, attr)
                if not callable(method):
                    continue
                setattr(StrictRedis, attr, wrapmethod(method, self._stats))


if __name__ == '__main__':

    # rudimentary tests

    db = StrictRedis(db=0)
    key = 'varname'

    def resetlist(key):
        db.ltrim(key, 1, 0)
        for i in range(10):
            db.rpush(key, f'some value #+{i+1:02d}')
        for i in range(10):
            db.lpush(key, f'some value #-{i+1:02d}')

    def displaylist(key):
        for i in range(db.llen(key)):
            print(f'lindex({i:02d})=', db.lindex(key, i))

    resetlist(key)

    print('* llen & lindex\n*\n')
    print('llen', db.llen(key))
    displaylist(key)

    print('* lrem\n*\n')
    val = 'some value #+01'
    print(f'lrem({val})')
    db.lrem(key, 1, val)
    displaylist(key)

    print('* lrange & ltrim\n*\n')
    ranges = (
        (7, 14),
        (2, 9),
        (15, 20),
        (14, -2),
        (17, 17),
        (17, -3),
        (17, 999999)
    )
    for rng in ranges:
        resetlist(key)
        print(f'lrange({rng})=\n\t' + '\n\t'.join(db.lrange(key, *rng)))
        print(f'ltrim {rng}')
        db.ltrim(key, *rng)
        for i in range(db.llen(key)):
            print(f'lindex({i:02d})=', db.lindex(key, i))
        print('---')

    print('* rpop & lpop\n*\n')
    resetlist(key)
    for i in range(10):
        print('rpop', db.rpop(key))
        print('lpop', db.lpop(key))
    print('llen', db.llen(key))

    print('* statistics\n*\n')
    for method, (t, n) in db._stats.items():
        t = t * 1000
        print(f'{method}: {t:.2f}ms')
