import sys
sys.path.append('clodss')
import clodss # noqa

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
