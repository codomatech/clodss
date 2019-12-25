'''
clodss: list data-structure
'''


def _listexists(instance, db, key, create=True):
    if key in instance._knownkeys:
        return
    tables = [f'{key}-l', f'{key}-r']
    x = db.execute('SELECT 1 FROM sqlite_master WHERE '
                   'type="table" AND name=?', (tables[0],)).fetchone()
    if x is None:
        if not create:
            return False
        for table in tables:
            db.execute(f'CREATE TABLE `{table}` (value TEXT)')

    instance._knownkeys.add(key)
    return True


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
    _listexists(instance, db, key)
    try:
        db.execute(f'INSERT INTO `{key}-r` VALUES(?)', (val,)).fetchone()
        db.commit()
        return 1
    finally:
        db.close()


def lpush(instance, key, val):
    db = instance.router.connection(key)
    _listexists(instance, db, key)
    try:
        db.execute(f'INSERT INTO `{key}-l` VALUES(?)', (val,)).fetchone()
        db.commit()
        return 1
    finally:
        db.close()


def rpop(instance, key):
    db = instance.router.connection(key)
    _listexists(instance, db, key)
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
    _listexists(instance, db, key)
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


def lindex(instance, key, index: int):
    db = instance.router.connection(key)
    _listexists(instance, db, key)
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
    _listexists(instance, db, key)
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


def ltrim(instance, key, start: int, end: int) -> None:
    db = instance.router.connection(key)
    _listexists(instance, db, key)
    try:
        size = llen(instance, key)
        start = _normalize_index(start, size)
        end = _normalize_index(end, size)

        if start > end or start >= size:
            db.execute(f'DELETE FROM `{key}-l`')
            db.execute(f'DELETE FROM `{key}-r`')
            db.commit()
            return

        sindex, stable, sorder = _locateindex(db, key, start)
        eindex, etable, eorder = _locateindex(db, key, end)

        if stable != etable:
            rlen = db.execute(f'SELECT COUNT(*) FROM `{stable}`').fetchone()[0]
            sindex = rlen - sindex
            eindex += 1
            db.execute(f'DELETE FROM `{stable}` LIMIT -1 OFFSET {sindex}')
            db.execute(f'DELETE FROM `{etable}` LIMIT -1 OFFSET {eindex}')
        else:
            if sorder == 'ASC':
                db.execute(f'DELETE FROM `{key}-l`')
                l = eindex - sindex + 1
                db.execute(f'DELETE FROM `{stable}` LIMIT {sindex} OFFSET 0')
                db.execute(f'DELETE FROM `{stable}` LIMIT -1 OFFSET {l}')
            else:
                db.execute(f'DELETE FROM `{key}-r`')
                rlen = db.execute(
                    f'SELECT COUNT(*) FROM `{stable}`').fetchone()[0]
                l = eindex - sindex + 1
                sindex = rlen - eindex - 1
                db.execute(f'DELETE FROM `{stable}` LIMIT {sindex} OFFSET 0')
                db.execute(f'DELETE FROM `{stable}` LIMIT -1 OFFSET {l}')
        db.commit()
    finally:
        db.close()


def lrem(instance, key, count: int, value) -> None:
    db = instance.router.connection(key)
    _listexists(instance, db, key)
    try:
        if count == 0:
            db.executescript(f'''
                            DELETE FROM `{key}-l`;
                            DELETE FROM `{key}-r`;
                            ''')
            db.commit()
            return None

        if count < 0:
            tables = [(f'{key}-r', 'DESC'), (f'{key}-l', 'ASC')]
            limit = -count
        else:
            tables = [(f'{key}-l', 'DESC'), (f'{key}-r', 'ASC')]
            limit = count

        nremoved = 0
        for table, order in tables:
            l = limit - nremoved
            res = db.execute(f'DELETE FROM `{table}` WHERE value=? ORDER BY ROWID {order} LIMIT {l}', (value, ))
            nremoved += res.rowcount
            if nremoved == limit:
                break
        db.commit()
    finally:
        db.close()


def linsert(instance, key, where, refvalue, value) -> int:
    db = instance.router.connection(key)
    exists = _listexists(instance, db, key, False)
    try:
        if not exists:
            return 0

        size = llen(instance, key)

        table = f'{key}-l'
        res = db.execute(
            f'SELECT ROWID FROM `{table}` WHERE value=? ORDER BY ROWID DESC LIMIT 1',
            (refvalue, )
        ).fetchone()
        if res is not None:
            rowid = res.fetchone()[0]
        db.commit()
    finally:
        db.close()
