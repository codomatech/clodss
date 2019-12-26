import time
import os
from router import Router
from ilock import ILock, ILockException
import lists, keys


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

        modules = [keys, lists]
        for module in modules:
            for attr in dir(module):
                if attr.startswith('_'):
                    continue
                method = getattr(module, attr)
                if not callable(method):
                    continue
                setattr(StrictRedis, attr, wrapmethod(method, self._stats))
