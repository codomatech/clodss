# -*- coding: utf-8 -*-

'''
clodss: hashmap data-structure
'''

from .common import SEP

def _augkey(hkey, key):
    return f'{hkey}{SEP}h{SEP}{key}'


def _db(instance, hkey):
  return instance.router.connection(hkey).db()


def hset(instance, hkey, key, val):
    'https://redis.io/commands/hset'
    db = _db(instance, hkey)
    db[_augkey(hkey, key)] = val
    return 1


def hget(instance, hkey, key):
    'https://redis.io/commands/hget'
    db = _db(instance, hkey)
    try:
        return instance.makevalue(db[_augkey(hkey, key)])
    except:
        return None


def hdel(instance, hkey, key):
    'https://redis.io/commands/hdel'
    db = _db(instance, hkey)
    key = _augkey(hkey, key)
    if not key in db:
        return 0
    del db[key]
    return 1


def hkeys(instance, hkey):
    'https://redis.io/commands/hkeys'
    db = _db(instance, hkey)
    keys = []
    prefix = _augkey(hkey, '').encode('utf-8')
    for k, _ in db[prefix:]:
        if not k.startswith(prefix):
            break
        keys.append(instance.makevalue(k[len(prefix):]))
    return keys

def hvalues(instance, hkey):
    'https://redis.io/commands/hkeys'
    db = _db(instance, hkey)
    vals = []
    prefix = _augkey(hkey, '').encode('utf-8')
    for k, v in db[prefix:]:
        if not k.startswith(prefix):
            break
        vals.append(instance.makevalue(v))
    return vals

def hgetall(instance, hkey):
    'https://redis.io/commands/hgetall'
    db = _db(instance, hkey)
    items = {}
    prefix = _augkey(hkey, '').encode('utf-8')
    for k, v in db[prefix:]:
        if not k.startswith(prefix):
            break
        items[instance.makevalue(k[len(prefix):])] = instance.makevalue(v)
    return items


def hmset(instance, hkey, mapping):
    'https://redis.io/commands/hmset'
    for k, v in mapping.items():
        hset(instance, hkey, k, v)


def hmget(instance, hkey, *keys):
    'https://redis.io/commands/hmget'
    return [hget(instance, hkey, k) for k in keys]

