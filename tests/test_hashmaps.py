'''
test cases for keys functionality
'''

from clodss import clodss

db = clodss.StrictRedis(db=1, decode_responses=True)

hkey = 'some-map'

def test_hget():
    key = 'key_get'
    assert db.hget(hkey, key) is None

def test_hset():
    key = 'key_set'
    value = 'some value'
    db.hset(hkey, key, value)
    assert db.hget(hkey, key) == value


def test_hset_existing():
    key = 'key_set_existing'
    value = 'some other value'
    db.hset(hkey, key, 'random value')
    db.hset(hkey, key, value)
    assert db.hget(hkey, key) == value

def test_hdel_existing():
    key = 'key_set_existing'
    db.hset(hkey, key, 'random value')
    assert db.hdel(hkey, key) == 1
    assert db.hget(hkey, key) is None


def test_hdel_nonexisting_hash():
    key = 'key_set_nonexisting'
    assert db.hdel(hkey, key) == 0

def test_hdel_nonexisting_key():
    key = 'key_set_existing'
    db.hset(hkey, key, 'random value')
    assert db.hdel(hkey, 'nonexisting_key') == 0
