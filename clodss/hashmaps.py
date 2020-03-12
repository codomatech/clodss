'''
clodss: hashmap data-structure
'''

from .common import keyexists


def _mapexists(instance, db, hkey, create=True):
    'checks if a hashmap exists, optionally creates one if not'
    tables = [f'{hkey}-hash']
    columns = 'key TEXT UNIQUE, value TEXT'
    return keyexists('hash', tables, instance, db,
                     hkey, create, columns=columns)


def hset(instance, hkey, key, val):
    'https://redis.io/commands/hset'
    db = instance.router.connection(hkey)
    _mapexists(instance, db, hkey)
    try:
        db.execute(f'INSERT OR REPLACE INTO `{hkey}-hash` VALUES(?, ?)',
                   (key, val))
        db.commit()
        return 1
    finally:
        db.close()


def hget(instance, hkey, key):
    'https://redis.io/commands/hget'
    db = instance.router.connection(hkey)
    _mapexists(instance, db, hkey)
    try:
        val = db.execute(
            f'SELECT value FROM `{hkey}-hash` WHERE key=?', (key,)).fetchone()
        return None if val is None else val[0]
    finally:
        db.close()
