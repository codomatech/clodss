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
        conn = sqlite3.connect(os.path.join(self.dbpath, f'{db}.db'))
        conn.execute('pragma journal_mode=wal')
        return conn


class StrictRedis:
    def __init__(self, db: int = 0, sharding_factor: int = 3, base: str = 'data', decode_responses: bool = False) -> None:
        self.decode = decode_responses
        dbpath = os.path.join(os.getcwd(), base, '%02d' % db)
        os.makedirs(dbpath, exist_ok=True)
        self.router = Router(dbpath, sharding_factor)
        self._knownkeys = set()

    # TODO IOC dtype to handlers

    def _ensure_exists(self, db, key, dtype):
        if key in self._knownkeys:
            return
        TABLES = {
            'list': ['<key>-l', '<key>-r']
        }

        tables = [tname.replace('<key>', key) for tname in TABLES[dtype]]
        x = db.execute('SELECT 1 FROM sqlite_master WHERE '
                       'type="table" AND name=?', (tables[0],)).fetchone()
        if x is None:
            for table in tables:
                db.execute(f'CREATE TABLE `{table}` (value TEXT)')

        self._knownkeys.add(key)

    def llen(self, key) -> int:
        db = self.router.connection(key)
        try:
            l = db.execute(f'SELECT COUNT(*) FROM `{key}-l`').fetchone()
            r = db.execute(f'SELECT COUNT(*) FROM `{key}-r`').fetchone()
            return l[0] + r[0]
        except sqlite3.OperationalError:
            return 0
        finally:
            db.close()

    def rpush(self, key, val) -> int:
        db = self.router.connection(key)
        self._ensure_exists(db, key, 'list')
        try:
            db.execute(f'INSERT INTO `{key}-r` VALUES(?)', (val,)).fetchone()
            db.commit()
        finally:
            db.close()

    def lpush(self, key, val) -> int:
        db = self.router.connection(key)
        self._ensure_exists(db, key, 'list')
        try:
            db.execute(f'INSERT INTO `{key}-l` VALUES(?)', (val,)).fetchone()
            db.commit()
        finally:
            db.close()

    def rpop(self, key) -> int:
        db = self.router.connection(key)
        self._ensure_exists(db, key, 'list')
        try:
            table = f'{key}-r'
            res = db.execute(
                f'SELECT ROWID, value FROM `{table}` ORDER BY ROWID DESC LIMIT 1').fetchone()
            if not res:
                table = f'{key}-l'
                res = db.execute(
                    f'SELECT ROWID, value FROM `{table}` ORDER BY ROWID ASC LIMIT 1').fetchone()
                if not res:
                    return None
            rowid, val = res
            db.execute(f'DELETE FROM `{table}` WHERE ROWID=?', (rowid,))
            db.commit()
            return val
        except Exception as e:
            raise
        finally:
            db.close()

    def lpop(self, key) -> int:
        db = self.router.connection(key)
        self._ensure_exists(db, key, 'list')
        try:
            table = f'{key}-l'
            res = db.execute(
                f'SELECT ROWID, value FROM `{table}` ORDER BY ROWID DESC LIMIT 1'
                ).fetchone()
            if not res:
                table = f'{key}-r'
                res = db.execute(
                    f'SELECT ROWID, value FROM `{table}` ORDER BY ROWID ASC LIMIT 1'
                ).fetchone()
                if not res:
                    return None
            rowid, val = res
            db.execute(f'DELETE FROM `{table}` WHERE ROWID=?', (rowid,))
            db.commit()
            return val
        except Exception as e:
            raise
        finally:
            db.close()


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
