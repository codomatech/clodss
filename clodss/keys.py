# -*- coding: utf-8 -*-

'''
clodss: keys-related functions
'''

import time

from .common import ProblematicSymbols, keyexists, _clearexpired


def _keyexists(instance, db, key, create=True):
    tables = [f'{key}﹁bytes']
    initquery = f'INSERT INTO `{key}﹁bytes` VALUES(0)'
    return keyexists('bytes', tables, instance, db,
                     key, create, initquery=initquery)


def get(instance, key):
    'https://redis.io/commands/get'
    db = instance.router.connection(key)
    exists = _keyexists(instance, db, key, create=False)
    try:
        if not exists:
            return None
        res = db.execute(f'SELECT value FROM `{key}﹁bytes` LIMIT 1').fetchone()
        return res[0]
    finally:
        db.close()


def sēt(instance, key, value):
    'https://redis.io/commands/set'
    db = instance.router.connection(key)
    _keyexists(instance, db, key, create=True)
    try:
        db.execute(f'UPDATE `{key}﹁bytes` SET value=? LIMIT 1', (value, ))
        db.commit()
        _clearexpired(instance, db, key)
    finally:
        db.close()


def delete(instance, key):
    'https://redis.io/commands/del'
    db = instance.router.connection(key)
    try:
        ekey = key.replace('"', '""')
        tables = db.execute(
            'SELECT name FROM sqlite_master WHERE '
            f'type="table" AND name LIKE "{ekey}%"').fetchall()
        queries = ';\n'.join([f'DROP TABLE `{table[0]}`' for table in tables])
        db.executescript(queries)
        db.commit()
        if key in instance.knownkeys:
            del instance.knownkeys[key]
        _clearexpired(instance, db, key)
    finally:
        db.close()


def expire(instance, key, duration: int) -> int:
    'https://redis.io/commands/expire'
    db = instance.router.connection(key)
    if not _keyexists(instance, db, key, create=False):
        return 0
    try:
        expiretime = duration + time.time()
        db.execute(
            'INSERT OR REPLACE INTO `﹁expiredkeys﹁` (key, time) VALUES(?, ?)',
            (key, expiretime)
        )
        instance.keystoexpire[key] = expiretime
        return 1
    finally:
        db.close()


def persist(instance, key) -> int:
    'https://redis.io/commands/persist'
    db = instance.router.connection(key)
    if not _keyexists(instance, db, key, create=False):
        return 0
    if instance.checkexpired(key) != 'scheduled':
        return 0
    try:
        _clearexpired(instance, db, key)
        return 1
    finally:
        db.close()


def incr(instance, key, amount=1):
    'https://redis.io/commands/incr'
    db = instance.router.connection(key)
    _keyexists(instance, db, key, create=True)
    try:
        res = db.execute(f'SELECT value FROM `{key}﹁bytes` LIMIT 1').fetchone()
        try:
            val = int('0' if res is None else res[0])
        except ValueError:
            raise TypeError(f'value stored at {key} is not an integer')
        val += amount
        db.execute(f'UPDATE `{key}﹁bytes` SET value=? LIMIT 1', (val, ))
        db.commit()
        return val
    finally:
        db.close()


def incrby(instance, key, amount):
    'https://redis.io/commands/incrby'
    return incr(instance, key, amount)


def decr(instance, key, amount=1):
    'https://redis.io/commands/decr'
    db = instance.router.connection(key)
    _keyexists(instance, db, key, create=True)
    try:
        res = db.execute(f'SELECT value FROM `{key}﹁bytes` LIMIT 1').fetchone()
        try:
            val = int('0' if res is None else res[0])
        except ValueError:
            raise TypeError(f'value stored at {key} is not an integer')
        val -= amount
        db.execute(f'UPDATE `{key}﹁bytes` SET value=? LIMIT 1', (val, ))
        db.commit()
        return val
    finally:
        db.close()


def decrby(instance, key, amount):
    'https://redis.io/commands/decrby'
    return decr(instance, key, amount)


def flushdb(instance):
    'https://redis.io/commands/flushdb'
    instance.reset()


def keys(instance, pattern='*'):
    '''
    https://redis.io/commands/keys
    NOTE: there is no guarantee the keys returned are complete or precise, i.e.
    some existing keys might be missing and some retured keys might be deleted.
    For now we favor performance for this specific command, please use hkeys as
    a reliable alternative.
    '''
    allkeys = []
    pattern = pattern.replace('*', '%')
    for db in instance.router.allconnections():
        try:
            query = ('SELECT name FROM sqlite_master WHERE '
                     f'name LIKE "{pattern}" AND '
                     'name NOT LIKE "sqlite_%"')
            for record in db.execute(query):
                table = record[0]
                if table.startswith('﹁'):
                    continue
                key = table.split('﹁')[0]
                allkeys.append(ProblematicSymbols.restore(key))
        finally:
            db.close()
    return allkeys


def scan(instance, cursor=None, match='*'):
    '''
    https://redis.io/commands/scan
    currently is the same as `keys`
    '''

    if cursor not in (None, True):
        raise ValueError('invalid cursor %r' %cursor)

    if cursor is True:
        return []

    return True, keys(instance, match)
