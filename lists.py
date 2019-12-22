'''
clodss: list data-structure
'''


def _ensure_exists(instance, db, key, dtype):
    if key in instance._knownkeys:
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

    instance._knownkeys.add(key)


def llen(instance, key) -> int:
    db = instance.router.connection(key)
    try:
        l = db.execute(f'SELECT COUNT(*) FROM `{key}-l`').fetchone()
        r = db.execute(f'SELECT COUNT(*) FROM `{key}-r`').fetchone()
        return l[0] + r[0]
    except sqlite3.OperationalError:
        return 0
    finally:
        db.close()


def rpush(instance, key, val):
    db = instance.router.connection(key)
    _ensure_exists(instance, db, key, 'list')
    try:
        db.execute(f'INSERT INTO `{key}-r` VALUES(?)', (val,)).fetchone()
        db.commit()
    finally:
        db.close()


def lpush(instance, key, val):
    db = instance.router.connection(key)
    _ensure_exists(instance, db, key, 'list')
    try:
        db.execute(f'INSERT INTO `{key}-l` VALUES(?)', (val,)).fetchone()
        db.commit()
    finally:
        db.close()


def rpop(instance, key):
    db = instance.router.connection(key)
    _ensure_exists(instance, db, key, 'list')
    try:
        table = f'{key}-r'
        res = db.execute(
            f'SELECT ROWID, value FROM `{table}` ORDER BY ROWID DESC LIMIT 1'
        ).fetchone()
        if not res:
            table = f'{key}-l'
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


def lpop(instance, key):
    db = instance.router.connection(key)
    _ensure_exists(instance, db, key, 'list')
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


def lindex(instance, key, index: int) -> int:
    db = instance.router.connection(key)
    _ensure_exists(instance, db, key, 'list')
    try:
        table = f'{key}-l'
        res = db.execute(f'SELECT COUNT(*) FROM `{table}`').fetchone()
        lcount = res[0]
        if index < lcount:
            query =  f'''SELECT value FROM `{table}`
                         ORDER BY ROWID DESC LIMIT 1 OFFSET {index}'''
        else:
            index -= lcount
            table = f'{key}-r'
            query =  f'''SELECT value FROM `{table}`
                         ORDER BY ROWID ASC LIMIT 1 OFFSET {index}'''
        res = db.execute(query).fetchone()
        return res[0]
    finally:
        db.close()
