# -*- coding: utf-8 -*-

'''
clodss: keys-related functions
'''

import re
import time

from .common import SEP, _clearexpired


def _keyexists(db, key):
    for k, _ in db[key.encode('utf-8'):]:
        if k == key.encode('utf-8') or k.startswith(
                f'{key}{SEP}'.encode('utf-8')):
            return True
        break
    return False


def get(instance, key):
    'https://redis.io/commands/get'
    db = instance.router.connection(key).db()
    try:
        return instance.makevalue(db[key])
    except KeyError:
        return None


def sēt(instance, key, value):
    'https://redis.io/commands/set'
    db = instance.router.connection(key).db()
    db[key] = value


def delete(instance, key):
    'https://redis.io/commands/del'
    db = instance.router.connection(key).db()
    for k, _ in db[key:]:
        if k == key.encode('utf-8') or k.startswith(
                f'{key}{SEP}'.encode('utf-8')):
            del db[k]
        else:
            break


def expire(instance, key, duration: float) -> int:
    'https://redis.io/commands/expire'
    db = instance.router.connection(key).db()
    if not _keyexists(db, key):
        print('key doesnt exist', key)
        return 0
    expiretime = duration + time.time()
    db[f'{SEP}expire{SEP}{key}'] = expiretime
    instance.keystoexpire[key] = expiretime
    return 1


def persist(instance, key) -> int:
    'https://redis.io/commands/persist'
    db = instance.router.connection(key).db()
    if not _keyexists(db, key):
        return 0
    if instance.checkexpired(key) != 'scheduled':
        return 0
    _clearexpired(instance, db, key)
    return 1


def incr(instance, key, amount=1):
    'https://redis.io/commands/incr'
    db = instance.router.connection(key).db()
    try:
        val = db[key]
    except KeyError:
        val = 0
    try:
        val = int(val)
    except ValueError:
        raise TypeError('invalid numeric %s' %val)
    newval = val + amount
    db[key] = newval
    return newval


def incrby(instance, key, amount):
    'https://redis.io/commands/incrby'
    return incr(instance, key, amount)


def decr(instance, key, amount=1):
    'https://redis.io/commands/decr'
    db = instance.router.connection(key).db()
    try:
        val = db[key]
    except KeyError:
        val = 0
    try:
        val = int(val)
    except ValueError:
        raise TypeError('invalid numeric %s' %val)
    newval = int(val) - amount
    db[key] = newval
    return newval


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
    pattern = pattern.replace('*', '.*')
    regex = re.compile(pattern)
    for db in instance.router.allconnections():
        for k, _ in db.db():
            k = k.split(SEP.encode('utf-8'))[0]
            #print('adding key', k)
            if regex.match(k.decode('utf-8')):
                allkeys.append(instance.makevalue(k))
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
