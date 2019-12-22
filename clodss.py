import os
from router import Router
import lists


class StrictRedis:
    def __init__(
            self, db: int = 0, sharding_factor: int = 3, base: str = 'data',
            decode_responses: bool = False) -> None:
        self.decode = decode_responses
        dbpath = os.path.join(os.getcwd(), base, '%02d' % db)
        os.makedirs(dbpath, exist_ok=True)
        self.router = Router(dbpath, sharding_factor)
        self._knownkeys = set()

        modules = [lists]
        for module in modules:
            for attr in dir(module):
                if attr.startswith('_'):
                    continue
                obj = getattr(module, attr)
                if not callable(obj):
                    continue
                setattr(StrictRedis, attr, obj)


if __name__ == '__main__':
    # rudimentary tests
    db = StrictRedis(db=0)
    key = 'varname'
    for i in range(10):
        db.rpush(key, f'some value #+{i}')
    for i in range(10):
        db.lpush(key, f'some value #-{i}')
    print(db.llen(key))
    for i in range(10):
        print(db.rpop(key))
        print(db.lpop(key))
    print(db.llen(key))
