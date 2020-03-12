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


def hdel(instance, hkey, key):
    'https://redis.io/commands/hdel'
    db = instance.router.connection(hkey)
    if not _mapexists(instance, db, hkey, create=False):
        return 0
    try:
        cur = db.cursor()
        cur.execute(f'DELETE FROM `{hkey}-hash` WHERE key=?', (key,))
        count = cur.rowcount
        cur.close()
        db.commit()
        return count
    finally:
        db.close()


def hkeys(instance, hkey):
    'https://redis.io/commands/hkeys'
    db = instance.router.connection(hkey)
    if not _mapexists(instance, db, hkey, create=False):
        return []
    try:
        return [record[0]
                for record in db.execute(f'SELECT key FROM `{hkey}-hash`')]
    finally:
        db.close()


def hvalues(instance, hkey):
    'https://redis.io/commands/hvalues'
    db = instance.router.connection(hkey)
    if not _mapexists(instance, db, hkey, create=False):
        return []
    try:
        return [record[0]
                for record in db.execute(f'SELECT value FROM `{hkey}-hash`')]
    finally:
        db.close()


def hgetall(instance, hkey):
    'https://redis.io/commands/hgetall'
    db = instance.router.connection(hkey)
    if not _mapexists(instance, db, hkey, create=False):
        return {}
    try:
        return {record[0]: record[1] for record in
                db.execute(f'SELECT key, value FROM `{hkey}-hash`')}
    finally:
        db.close()
