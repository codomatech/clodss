'''
test cases for keys functionality
'''
import pytest
from clodss import clodss

db = clodss.StrictRedis(db=1, decode_responses=True)


def test_get():
    key = 'key_get'
    assert db.get(key) is None

def test_set():
    key = 'key_set'
    value = 'some value'
    db.set(key, value)
    assert db.get(key) == value


def test_set_existing():
    key = 'key_set_existing'
    value = 'some other value'
    db.set(key, value)
    assert db.get(key) == value


def test_del():
    key = 'key_del'
    value = 'some other value'
    db.set(key, value)
    db.delete(key)
    assert db.get(key) is None


def test_incr_inited():
    key = 'key_incr'
    db.set(key, 5)
    db.incr(key)
    assert db.get(key) == '6'


def test_incr_uninited():
    key = 'key_incr_uninited'
    db.delete(key)
    db.incr(key)
    assert db.get(key) == '1'


def test_incr_nonint():
    key = 'key_incr_nonint'
    db.set(key, 'ab')
    with pytest.raises(TypeError):
        db.incr(key)


def test_incrby_inited():
    key = 'key_incrby'
    db.set(key, 5)
    db.incrby(key, 20)
    assert db.get(key) == '25'


def test_incrby_uninited():
    key = 'key_incrby_uninited'
    db.delete(key)
    db.incrby(key, 33)
    assert db.get(key) == '33'


def test_incrby_nonint():
    key = 'key_incrby_nonint'
    db.set(key, 'ab')
    with pytest.raises(TypeError):
        db.incrby(key, 44)
