# -*- coding: utf-8 -*-

'''
clodss is a data-structures on-disk store with an API largly compatible with
redis. The goal is to develop a store with the simplicity of the redis API
which scales beyond memory capacity, allows harnessing multi-core processors,
and does not burden accesses with network latency.
'''

import logging
import time
import os
import __main__
from .router import Router
from . import hashmaps
from . import lists
from . import keys
from .common import SEP

LOG = logging.getLogger('clodss')


def wrapmethod(method, stats=None):
    '''
    - guards all clodss methods with a key-scoped lock
    - performs sanity checks on key
    - enures the key has not expired
    '''
    def wrapper(*args, **kwargs):
        globalmethod = method.__name__ in ('keys', 'scan', 'flushdb')

        instance = args[0]
        if globalmethod:
            key = '﹁'
        else:
            if len(args) < 2:
                raise TypeError('too few parameters, `key` is required')
            key = args[1]

            if SEP in key:
                raise ValueError(f'`key` contains invalid character(s): {SEP}')
        dtype = instance.keydtype(key)
        methodtype = method.__name__[0].encode('utf-8')
        if method.__name__ in ('rpush', 'rpop'):
            methodtype = b'l'
        elif methodtype not in (b'l', b'h'):
            methodtype = ''
        LOG.debug(method.__name__, key, dtype, methodtype, globalmethod)
        if method.__name__ != 'delete' and dtype is not None \
            and not globalmethod and dtype != methodtype:
            raise ValueError('incompatible operation `%s` on %s' %(
                method.__name__, dtype))

        if stats is not None:
            t1 = time.perf_counter()
        if not globalmethod:
            instance.checkexpired(key, enforce=True)
        result = method(*args, **kwargs)
        if stats is not None:
            t = time.perf_counter() - t1
            avg, n = stats.get(method.__name__, (0, 0))
            avg = (avg * n + t) / (n + 1)
            stats[method.__name__] = (avg, n + 1)
        return result
    wrapper.__name__ = method.__name__
    return wrapper


class StrictRedis:
    'main clodss class'
    def __init__(
            self, dbpath: str = None, db: int = 0, spread_factor: int = 2,
            decode_responses: bool = False, benchmark: bool = True) -> None:
        if dbpath is None:
            try:
                fname = __main__.__file__
            except AttributeError:
                fname = '<unknown>'
            d = os.path.dirname(fname)
            if d == '':
                d = '.'
            dbpath = os.path.realpath(f'{d}/./clodss-data')
        LOG.info('clodss db path: %s', dbpath)
        self.decode = decode_responses
        dbpath = os.path.join(dbpath, '%02d' % db)
        os.makedirs(dbpath, exist_ok=True)
        self._dbpath = dbpath
        self.router = Router(dbpath, spread_factor)
        self.knownkeys = {}
        self.keystoexpire = {}
        self._stats = {} if benchmark else None

        modules = [keys, lists, hashmaps]
        for module in modules:
            for attr in dir(module):
                if attr.startswith('_'):
                    continue
                method = getattr(module, attr)
                if not callable(method):
                    continue
                if attr == 'sēt':
                    # `set` is a reserved keyword
                    attr = 'set'
                setattr(StrictRedis, attr, wrapmethod(method, self._stats))

    def stats(self):
        'gets benchmarking statistics'
        return self._stats

    def makevalue(self, v):
        'makes value respecting decode_responses setting'
        if self.decode:
            return v.decode('utf-8')
        return v

    def dbpath(self):
        'gets base path for database files'
        return self._dbpath

    def keydtype(self, key):
        'get key data type'
        db = self.router.connection(key).db()
        closestkey = b''
        for k, _ in db[key:]:
            closestkey = k
            break
        if closestkey != key.encode('utf-8') and not closestkey.startswith(
                f'{key}{SEP}'.encode('utf-8')):
            return None
        if not SEP.encode('utf-8') in closestkey:
            return ''
        return closestkey.split(SEP.encode('utf-8'))[1]

    def checkexpired(self, key, enforce=False):
        'checks if a key expired and removes it if so'
        db = self.router.connection(key).db()
        try:
            exp = self.keystoexpire.get(key)
            if not exp:
                try:
                    exp = db[f'{SEP}expire{SEP}{key}']
                except KeyError:
                    pass
            if exp:
                t = float(exp)
                now = time.time()
                if now > t:
                    LOG.debug('expiring', key, type(key))
                    if not enforce:
                        return True
                    del db[f'{SEP}expire{SEP}{key}'.encode('utf-8')]
                    for k, _ in db[key:]:
                        LOG.debug('checking', k, key, k == key)
                        if k == key.encode('utf-8') or k.startswith(
                                f'{key}{SEP}'.encode('utf-8')):
                            LOG.debug('delete', k, f'{SEP}expire{SEP}{k}')
                            del db[k]
                        else:
                            break
                    if key in self.knownkeys:
                        del self.knownkeys[key]

                    return True
                return 'scheduled'
            return False
        finally:
            pass

    def reset(self):
        'clears a database and all cached information'
        self.router.reset()
        self.knownkeys = {}
        self.keystoexpire = {}
