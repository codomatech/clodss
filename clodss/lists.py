'''
clodss: list data-structure
'''

from .common import keyexists


def _listexists(instance, db, key, create=True):
    tables = [f'{key}-l', f'{key}-r']
    return keyexists('list', tables, instance, db, key, create)


def llen(instance, key) -> int:
    'https://redis.io/commands/llen'
    db = instance.router.connection(key)
    try:
        left = db.execute(f'SELECT COUNT(*) FROM `{key}-l`').fetchone()
        right = db.execute(f'SELECT COUNT(*) FROM `{key}-r`').fetchone()
        return left[0] + right[0]
    finally:
        db.close()


def rpush(instance, key, val):
    'https://redis.io/commands/rpush'
    db = instance.router.connection(key)
    _listexists(instance, db, key)
    try:
        db.execute(f'INSERT INTO `{key}-r` VALUES(?)', (val,))
        db.commit()
        return 1
    finally:
        db.close()


def lpush(instance, key, val):
    'https://redis.io/commands/lpush'
    db = instance.router.connection(key)
    _listexists(instance, db, key)
    try:
        db.execute(f'INSERT INTO `{key}-l` VALUES(?)', (val,))
        db.commit()
        return 1
    finally:
        db.close()


def rpop(instance, key):
    'https://redis.io/commands/rpop'
    db = instance.router.connection(key)
    _listexists(instance, db, key)
    try:
        table = f'{key}-r'
        res = db.execute(
            f'SELECT ROWID, value FROM `{table}` ORDER BY ROWID DESC LIMIT 1'
        ).fetchone()
        if not res:
            table = f'{key}-l'
            res = db.execute(f'''SELECT ROWID, value FROM `{table}`
                             ORDER BY ROWID ASC LIMIT 1''').fetchone()
            if not res:
                return None
        rowid, val = res
        db.execute(f'DELETE FROM `{table}` WHERE ROWID=?', (rowid,))
        db.commit()
        return val
    finally:
        db.close()


def lpop(instance, key):
    'https://redis.io/commands/lpop'
    db = instance.router.connection(key)
    _listexists(instance, db, key)
    try:
        table = f'{key}-l'
        res = db.execute(
            f'SELECT ROWID, value FROM `{table}` ORDER BY ROWID DESC LIMIT 1'
        ).fetchone()
        if not res:
            table = f'{key}-r'
            res = db.execute(f'''SELECT ROWID, value FROM `{table}`
                             ORDER BY ROWID ASC LIMIT 1''').fetchone()
            if not res:
                return None
        rowid, val = res
        db.execute(f'DELETE FROM `{table}` WHERE ROWID=?', (rowid,))
        db.commit()
        return val
    finally:
        db.close()


def _locateindex(db, key, index: int):
    'finds the table and offset for a given index'
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
    'normalizes negative and oob indices'
    if i < 0:
        i += (abs(i)//size + 1) * size
        return i % size

    if i >= size and not allowoverflow:
        return None
    return min(i, size - 1)


def lindex(instance, key, index: int):
    'https://redis.io/commands/lindex'
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


def lset(instance, key, index: int, value):
    'https://redis.io/commands/lset'
    db = instance.router.connection(key)
    _listexists(instance, db, key)
    try:
        size = llen(instance, key)
        index = _normalize_index(index, size, False)
        if index is None:
            raise ValueError('out of bound index')
        index, table, order = _locateindex(db, key, index)
        query = f'''SELECT ROWID FROM `{table}`
                    ORDER BY ROWID {order} LIMIT 1 OFFSET {index}'''
        rowid = db.execute(query).fetchone()[0]
        db.execute(f'UPDATE `{table}` SET VALUE=? WHERE ROWID=?',
                   (value, rowid))
        db.commit()
    finally:
        db.close()


def lrange(instance, key, start: int, end: int):
    'https://redis.io/commands/lrange'
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
        return [record[0] for record in cursor] + [
            record[0] for record in cursor2]
    finally:
        cursor.close()
        cursor2.close()
        db.close()


def ltrim(instance, key, start: int, end: int) -> None:
    'https://redis.io/commands/ltrim'
    db = instance.router.connection(key)
    _listexists(instance, db, key)
    try:
        size = llen(instance, key)
        s, e = start, end
        start = _normalize_index(start, size)
        end = _normalize_index(end, size)

        truncate = s > e >= 0
        if truncate or start > end or start >= size:
            db.executescript('\n'.join([
                f'DELETE FROM `{key}-l`;',
                f'DELETE FROM `{key}-r`;'
                ]))
            db.commit()
            return None

        sindex, stable, sorder = _locateindex(db, key, start)
        eindex, etable, _ = _locateindex(db, key, end)

        if stable != etable:
            rlen = db.execute(f'SELECT COUNT(*) FROM `{stable}`').fetchone()[0]
            sindex = rlen - sindex
            eindex += 1
            db.execute(f'DELETE FROM `{stable}` LIMIT -1 OFFSET {sindex}')
            db.execute(f'DELETE FROM `{etable}` LIMIT -1 OFFSET {eindex}')
        else:
            if sorder == 'ASC':
                db.execute(f'DELETE FROM `{key}-l`')
                rsize = eindex - sindex + 1
                db.execute(f'DELETE FROM `{stable}` LIMIT {sindex} OFFSET 0')
                db.execute(f'DELETE FROM `{stable}` LIMIT -1 OFFSET {rsize}')
            else:
                db.execute(f'DELETE FROM `{key}-r`')
                rlen = db.execute(
                    f'SELECT COUNT(*) FROM `{stable}`').fetchone()[0]
                rsize = eindex - sindex + 1
                sindex = rlen - eindex - 1
                db.execute(f'DELETE FROM `{stable}` LIMIT {sindex} OFFSET 0')
                db.execute(f'DELETE FROM `{stable}` LIMIT -1 OFFSET {rsize}')
        db.commit()
        return None
    finally:
        db.close()


def lrem(instance, key, count: int, value) -> None:
    'https://redis.io/commands/lrem'
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
            size = limit - nremoved
            res = db.execute(f'''DELETE FROM `{table}` WHERE value=?
                             ORDER BY ROWID {order} LIMIT {size}''',
                             (value, ))
            nremoved += res.rowcount
            if nremoved == limit:
                break
        db.commit()
        return None
    finally:
        db.close()


def linsert(instance, key, where, refvalue, value) -> int:
    'https://redis.io/commands/linsert'
    db = instance.router.connection(key)
    cursor = db.cursor()
    exists = _listexists(instance, db, key, False)
    try:
        if not exists:
            return 0

        size = llen(instance, key)
        where = where.lower()

        table = f'{key}-l'
        res = db.execute(f'''SELECT ROWID FROM `{table}` WHERE value=?
                         ORDER BY ROWID DESC LIMIT 1''',
                         (refvalue, )).fetchone()
        if res is not None:
            rowid = res[0]
            db.execute(f'INSERT INTO `{table}` VALUES("")')
            db.commit()
            op = '>' if where == 'before' else '>='
            cursor.execute(f'''SELECT ROWID, value FROM `{table}`
                           WHERE ROWID {op} {rowid}''')
            prv = value
            params = []
            for rowid, val in cursor:
                params.append((prv, rowid))
                prv = val
            db.executemany(f'UPDATE `{table}` SET value=? WHERE ROWID=?',
                           params)
            db.commit()
            return size + 1

        table = f'{key}-r'
        query = f'''SELECT ROWID FROM `{table}` WHERE value=?
                    ORDER BY ROWID ASC LIMIT 1'''
        res = db.execute(query, (refvalue, )).fetchone()
        if res is not None:
            rowid = res[0]
            db.execute(f'INSERT INTO `{table}` VALUES("")')
            db.commit()
            op = '>=' if where == 'before' else '>'
            cursor.execute(f'''SELECT ROWID, value FROM `{table}`
                           WHERE ROWID {op} {rowid}''')
            prv = value
            params = []
            for rowid, val in cursor:
                params.append((prv, rowid))
                prv = val
            db.executemany(f'UPDATE `{table}` SET value=? WHERE ROWID=?',
                           params)
            db.commit()
            return size + 1

        return size
    finally:
        cursor.close()
        db.close()
