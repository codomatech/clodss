# -*- coding: utf-8 -*-

'''
clodss: list data-structure
'''

from .common import SEP

MAX_DIGITS = 12


MIDDLE_INDEX = int(10 ** (MAX_DIGITS / 2))


def _augkey(lkey):
    return f'{lkey}{SEP}l{SEP}'


def _maxkey(lkey, db, asint=True):
    prefix = _augkey(lkey).encode('utf-8')
    maxkey = None
    for k, _ in db[prefix::True]:
        maxkey = k
        break
    if maxkey is None or not maxkey.startswith(prefix):
        return None
    return maxkey if not asint else int(maxkey[len(prefix):])


def _minkey(lkey, db, asint=True):
    prefix = _augkey(lkey).encode('utf-8')
    minkey = None
    for k, _ in db[prefix:]:
        minkey = k
        break
    if minkey is None or not minkey.startswith(prefix):
        return None
    return minkey if not asint else int(minkey[len(prefix):])


def llen(instance, key) -> int:
    'https://redis.io/commands/llen'
    db = instance.router.connection(key).db()
    prefix = _augkey(key).encode('utf-8')
    n = 0
    for k, _ in db[prefix:]:
        if not k.startswith(prefix):
            break
        n += 1
    return n


def rpush(instance, key, val):
    'https://redis.io/commands/rpush'
    db = instance.router.connection(key).db()
    prefix = _augkey(key).encode('utf-8')
    maxkey = _maxkey(key, db)
    index = MIDDLE_INDEX if maxkey is None else maxkey
    indexstr = f'%0{MAX_DIGITS}d' % (index + 1)
    if len(indexstr) > MAX_DIGITS:
        # TO DO: cleanup to use deleted indices
        raise Exception('list is too big')
    db[prefix + indexstr.encode('utf-8')] = val


def lpush(instance, key, val):
    'https://redis.io/commands/lpush'
    db = instance.router.connection(key).db()
    prefix = _augkey(key).encode('utf-8')
    minkey = _minkey(key, db)
    index = MIDDLE_INDEX if minkey is None else minkey
    index = index - 1
    if index < 0:
        # TO DO: cleanup to use deleted indices
        raise Exception('list is too big')
    indexstr = f'%0{MAX_DIGITS}d' % (index)
    db[prefix + indexstr.encode('utf-8')] = val


def rpop(instance, key):
    'https://redis.io/commands/rpop'
    db = instance.router.connection(key).db()
    maxkey = _maxkey(key, db, False)
    if maxkey is None:
        return None
    v = db[maxkey]
    del db[maxkey]
    return instance.makevalue(v)


def lpop(instance, key):
    'https://redis.io/commands/lpop'
    db = instance.router.connection(key).db()
    minkey = _minkey(key, db, False)
    if minkey is None:
        return None
    v = db[minkey]
    del db[minkey]
    return instance.makevalue(v)


def _normalizeindex(index, lkey, instance):
    if index >= 0:
        return index
    l = llen(instance, lkey)
    if l == 0:
        return 0
    return (index + (-index // l + 1) * l) % l


def _lindex(lkey, index: int, instance):
    db = instance.router.connection(lkey).db()
    prefix = _augkey(lkey).encode('utf-8')
    index = _normalizeindex(index, lkey, instance)
    i = 0
    key = None
    for k, _ in db[prefix:]:
        if not k.startswith(prefix):
            break
        if i == index:
            key = k
            break
        i += 1
    return key


def lindex(instance, key, index: int):
    'https://redis.io/commands/lindex'
    db = instance.router.connection(key).db()
    k = _lindex(key, index, instance)
    if k is None:
        return None
    return instance.makevalue(db[k])


def lset(instance, key, index: int, value: bytes):
    'https://redis.io/commands/lset'
    db = instance.router.connection(key).db()
    k = _lindex(key, index, instance)
    if k is None:
        return
    db[k] = value


def lrange(instance, key, start: int, end: int):
    'https://redis.io/commands/lrange'
    db = instance.router.connection(key).db()
    k0 = _lindex(key, start, instance)
    k1 = _lindex(key, end, instance)
    if k0 is None:
        return None
    if k1 is None:
        k1 = _maxkey(key, db, False)
    return [instance.makevalue(v) for _, v in db[k0:k1]]


def ltrim(instance, key, start: int, end: int) -> None:
    'https://redis.io/commands/ltrim'
    db = instance.router.connection(key).db()
    prefix = _augkey(key).encode('utf-8')
    start = _normalizeindex(start, key, instance)
    end = _normalizeindex(end, key, instance)
    if start > end:
        instance.delete(key)
        return
    i = 0
    for k, _ in db[prefix:]:
        if not k.startswith(prefix):
            break
        if not start <= i <= end:
            del db[k]
        i += 1


def lrem(instance, key, count: int, value: bytes) -> None:
    'https://redis.io/commands/lrem'
    db = instance.router.connection(key).db()
    prefix = _augkey(key).encode('utf-8')
    todel = []
    iterable = db[prefix::True] if count < 0 else db[prefix:]
    limit = int(10**MAX_DIGITS) if count == 0 else abs(count)
    for k, v in iterable:
        if v == value.encode('utf-8'):
            todel.append(k)
        if len(todel) == limit:
            break
        if not k.startswith(prefix):
            break
    for k in todel:
        del db[k]
    return len(todel)


def linsert(instance, key, where, refvalue, value) -> int:
    'https://redis.io/commands/linsert'
    db = instance.router.connection(key).db()
    if instance.keydtype(key) is None:
        return 0
    prefix = _augkey(key).encode('utf-8')

    marker = SEP * 10
    n = 0
    found = False
    cache = {}
    prev = None
    for k, v in db[prefix:]:
        if not k.startswith(prefix):
            break
        n += 1
        if found:
            if len(cache) >= 42:
                for ck, cv in cache.items():
                    db[ck] = cv
                cache = {}
            cache[k] = prev
            prev = v
        else:
            if v == refvalue.encode('utf-8'):
                found = True
                instance.rpush(key, marker)
                if where == 'before':
                    prev = refvalue
                    cache[k] = value
                else:
                    prev = value
    if found:
        for ck, cv in cache.items():
            db[ck] = cv
        strprev = prev if isinstance(prev, str) else prev.decode('utf-8')
        if strprev != marker:
            lastkey = _maxkey(key, db, False)
            db[lastkey] = prev
    return -1 if not found else n
