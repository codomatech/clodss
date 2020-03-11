'''
clodss: hashmap data-structure
'''


def _mapexists(instance, db, hkey, create=True):
    'checks if a hashmap exists, optionally creates one if not'
    if hkey in instance.knownkeys:
        return True
    table = hkey
    x = db.execute('SELECT 1 FROM sqlite_master WHERE '
                   'type="table" AND name=?', (table,)).fetchone()
    if x is None:
        if not create:
            return False
        db.execute(f'CREATE TABLE `{table}` (key TEXT UNIQUE, value TEXT)')

    instance.knownkeys.add(hkey)
    return True


def hset(instance, hkey, key, val):
    'https://redis.io/commands/hset'
    db = instance.router.connection(hkey)
    _mapexists(instance, db, hkey)
    try:
        db.execute(f'INSERT OR REPLACE INTO `{hkey}` VALUES(?, ?)',
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
            f'SELECT value FROM `{hkey}` WHERE key=?', (key,)).fetchone()
        return None if val is None else val[0]
    finally:
        db.close()
