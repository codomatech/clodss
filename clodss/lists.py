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
        return 1
    finally:
        db.close()


def lpush(instance, key, val):
    db = instance.router.connection(key)
    _ensure_exists(instance, db, key, 'list')
    try:
        db.execute(f'INSERT INTO `{key}-l` VALUES(?)', (val,)).fetchone()
        db.commit()
        return 1
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


def _locateindex(db, key, index: int):
    table = f'{key}-l'
    res = db.execute(f'SELECT COUNT(*) FROM `{table}`').fetchone()
    lcount = res[0]
    if index < lcount:
        order = 'DESC'
    else:
        index -= lcount
        table = f'{key}-r'
        order = 'ASC'
    return index, table, order


def _normalize_index(i: int, size: int, allowoverflow=True) -> int:
    if i < 0:
        i += (abs(i)//size + 1) * size
        return i % size
    else:
        if i >= size and not allowoverflow:
            return None
        return min(i, size - 1)


def lindex(instance, key, index: int) -> int:
    db = instance.router.connection(key)
    _ensure_exists(instance, db, key, 'list')
    try:
        size = llen(instance, key)
        index = _normalize_index(index, size, False)
        if index is None:
            return None
        index, table, order = _locateindex(db, key, index)
        query = f'''SELECT value FROM `{table}`
                    ORDER BY ROWID {order} LIMIT 1 OFFSET {index}'''
        res = db.execute(query).fetchone()
        return res[0]
    finally:
        db.close()


def lrange(instance, key, start: int, end: int):
    # TODO generators?
    db = instance.router.connection(key)
    cursor = db.cursor()
    cursor2 = db.cursor()
    _ensure_exists(instance, db, key, 'list')
    try:
        size = llen(instance, key)
        start = _normalize_index(start, size)
        end = _normalize_index(end, size)

        if start > end:
            raise ValueError('end should be >= start')

        sindex, stable, sorder = _locateindex(db, key, start)
        eindex, etable, eorder = _locateindex(db, key, end)

        if stable == etable:
            n = eindex - sindex + 1
            query = f'''SELECT value FROM `{stable}`
                        ORDER BY ROWID {sorder} LIMIT {n} OFFSET {sindex}'''
            cursor.execute(query)
            return [record[0] for record in cursor]

        cursor.execute(f'''SELECT value FROM `{stable}`
                    ORDER BY ROWID {sorder} LIMIT -1 OFFSET {sindex}''')

        cursor2.execute(f'''SELECT value FROM `{etable}`
                    ORDER BY ROWID {eorder} LIMIT {eindex + 1} OFFSET 0''')
        return [record[0] for record in cursor] + [record[0] for record in cursor2]
    finally:
        cursor.close()
        cursor2.close()
        db.close()
