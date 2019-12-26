'''
clodss: keys-related functions
'''


def _keyexists(instance, db, key, create=True):
    if key in instance._knownkeys:
        return True
    tables = [f'{key}']
    x = db.execute('SELECT 1 FROM sqlite_master WHERE '
                   'type="table" AND name=?', (tables[0],)).fetchone()
    if x is None:
        if not create:
            return False
        for table in tables:
            db.execute(f'CREATE TABLE `{table}` (value TEXT)')
            db.execute(f'INSERT INTO `{table}` VALUES("")')

    instance._knownkeys.add(key)
    return True


def get(instance, key) -> int:
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


def set(instance, key, value) -> int:
    db = instance.router.connection(key)
    _keyexists(instance, db, key, create=True)
    try:
        db.execute(f'UPDATE `{key}` SET value=? LIMIT 1', (value, ))
        db.commit()
    finally:
        db.close()


def delete(instance, key) -> int:
    db = instance.router.connection(key)
    exists = _keyexists(instance, db, key, create=False)
    try:
        if not exists:
            return
        db.execute(f'DROP TABLE `{key}`')
        db.commit()
        instance._knownkeys.remove(key)
    finally:
        db.close()
