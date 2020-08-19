# -*- coding: utf-8 -*-

'''
clodss: list data-structure
'''

from lsm import SEEK_LE, SEEK_GE
from .common import SEP

MAX_DIGITS = 12

MIDDLE_INDEX = int(10 ** (MAX_DIGITS / 2))

def _augkey(lkey):
    return f'{lkey}{SEP}l{SEP}'


def llen(instance, key) -> int:
    'https://redis.io/commands/llen'
    db = instance.router.connection(key).db()
    prefix = _augkey(key).encode('utf-8')
    n = 0
    for k, v in db[prefix:]:
        if not k.startswith(prefix):
            break
        n += 1
    return items


def rpush(instance, key, val):
    'https://redis.io/commands/rpush'
    db = instance.router.connection(key).db()
    prefix = _augkey(key).encode('utf-8')
    maxkey = None
    for k, _ in db[prefix:]:
        if maxkey is None or k > maxkey:
            maxkey = key
    index = MIDDLE_INDEX if maxkey is None else int(maxkey[len(prefix):])
    indexstr = f'%0{MAX_DIGITS}d' %(index+1)
    if len(indexstr) > MAX_DIGITS:
        # TODO cleanup to use deleted indices
        raise Exception('list is too big')
    db[f'{prefix}%0{indexstr}'] = val


def lpush(instance, key, val):
    'https://redis.io/commands/lpush'
    db = instance.router.connection(key).db()
    prefix = _augkey(key)
    n = 0
    for k, _ in db[prefix + '0':]:
        if not k.startswith(key.encode('utf-8')):
            break
        n += 1
    db[prefix + str(n + 1)] = val


def rpop(instance, key):
    'https://redis.io/commands/rpop'
    db = instance.router.connection(key).db()


def lpop(instance, key):
    'https://redis.io/commands/lpop'
    db = instance.router.connection(key).db()


def lindex(instance, key, index: int):
    'https://redis.io/commands/lindex'
    db = instance.router.connection(key).db()


def lset(instance, key, index: int, value):
    'https://redis.io/commands/lset'
    db = instance.router.connection(key).db()


def lrange(instance, key, start: int, end: int):
    'https://redis.io/commands/lrange'
    db = instance.router.connection(key).db()


def ltrim(instance, key, start: int, end: int) -> None:
    'https://redis.io/commands/ltrim'
    db = instance.router.connection(key).db()


def lrem(instance, key, count: int, value) -> None:
    'https://redis.io/commands/lrem'
    db = instance.router.connection(key).db()


def linsert(instance, key, where, refvalue, value) -> int:
    'https://redis.io/commands/linsert'
    db = instance.router.connection(key).db()
