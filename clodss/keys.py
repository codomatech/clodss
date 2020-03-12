'''
clodss: keys-related functions
'''

from .common import keyexists


def _keyexists(instance, db, key, create=True):
    tables = [key]
    initquery = f'INSERT INTO `{key}` VALUES(0)'
    return keyexists('bytes', tables, instance, db,
                     key, create, initquery=initquery)


def get(instance, key) -> int:
    'https://redis.io/commands/get'
    db = instance.router.connection(key)
    exists = _keyexists(instance, db, key, create=False)
    try:
        if not exists:
            return None
        res = db.execute(
            f'SELECT value FROM `{key}` LIMIT 1'
        ).fetchone()
        return res[0]
    finally:
        db.close()


def sēt(instance, key, value) -> int:
    'https://redis.io/commands/set'
    db = instance.router.connection(key)
    _keyexists(instance, db, key, create=True)
    try:
        db.execute(f'UPDATE `{key}` SET value=? LIMIT 1', (value, ))
        db.commit()
    finally:
        db.close()


def delete(instance, key) -> int:
    'https://redis.io/commands/del'
    db = instance.router.connection(key)
    try:
        tables = db.execute(
            'SELECT name FROM sqlite_master WHERE '
            f'type="table" AND name LIKE "{key}%"').fetchall()
        for table in tables:
            db.execute(f'DROP TABLE `{table[0]}`')
        db.commit()
        del instance.knownkeys[key]
    finally:
        db.close()
