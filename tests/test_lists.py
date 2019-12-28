'''
test cases for lists functionality
'''

import pytest
import sys
sys.path.append('clodss')
import clodss # noqa

db = clodss.StrictRedis(db=1, decode_responses=True)
key = 'key-name'


def resetlist(key):
    db.ltrim(key, 1, 0)
    for i in range(10):
        db.rpush(key, f'+{i+1:02d}')
    for i in range(10):
        db.lpush(key, f'-{i+1:02d}')


def getlist(key):
    count = db.llen(key)
    return [db.lindex(key, i) for i in range(count)]


def test_llen():
    resetlist(key)
    assert db.llen(key) == 20


def test_lindex():
    resetlist(key)
    expected = [
        "-10",
        "-09",
        "-08",
        "-07",
        "-06",
        "-05",
        "-04",
        "-03",
        "-02",
        "-01",
        "+01",
        "+02",
        "+03",
        "+04",
        "+05",
        "+06",
        "+07",
        "+08",
        "+09",
        "+10"
    ]
    assert getlist(key) == expected


def test_lrem():
    resetlist(key)
    db.lrem(key, 1, '+01')
    expected = [
        "-10",
        "-09",
        "-08",
        "-07",
        "-06",
        "-05",
        "-04",
        "-03",
        "-02",
        "-01",
        "+02",
        "+03",
        "+04",
        "+05",
        "+06",
        "+07",
        "+08",
        "+09",
        "+10"
    ]
    assert getlist(key) == expected


def test_lset():
    resetlist(key)
    for i in (0, 5, 10, 15, 19, -3, -12):
        db.lset(key, i, f'** set value @ {i} **')
    expected = [
        "** set value @ 0 **",
        "-09",
        "-08",
        "-07",
        "-06",
        "** set value @ 5 **",
        "-04",
        "-03",
        "** set value @ -12 **",
        "-01",
        "** set value @ 10 **",
        "+02",
        "+03",
        "+04",
        "+05",
        "** set value @ 15 **",
        "+07",
        "** set value @ -3 **",
        "+09",
        "** set value @ 19 **"
    ]
    assert getlist(key) == expected


def test_linsert():
    resetlist(key)
    val = '** inserted value **'
    for refvalue in ('-10', '-01', '+05', '+10'):
        db.linsert(key, 'before', refvalue, val)
        db.linsert(key, 'after', refvalue, val)
    expected = [
        "** inserted value **",
        "-10",
        "** inserted value **",
        "-09",
        "-08",
        "-07",
        "-06",
        "-05",
        "-04",
        "-03",
        "-02",
        "** inserted value **",
        "-01",
        "** inserted value **",
        "+01",
        "+02",
        "+03",
        "+04",
        "** inserted value **",
        "+05",
        "** inserted value **",
        "+06",
        "+07",
        "+08",
        "+09",
        "** inserted value **",
        "+10",
        "** inserted value **"
    ]
    assert getlist(key) == expected


lrange_data = (
    ((7, 14), ["-03", "-02", "-01", "+01", "+02", "+03", "+04", "+05"]),
    ((2, 9), ["-08", "-07", "-06", "-05", "-04", "-03", "-02", "-01"]),
    ((15, 20), ["+06", "+07", "+08", "+09", "+10"]),
    ((14, -2), ["+05", "+06", "+07", "+08", "+09"]),
    ((17, 17), ["+08"]),
    ((17, -3), ["+08"]),
    ((17, 999999), ["+08", "+09", "+10"])
)
@pytest.mark.parametrize("rng, expected", lrange_data)
def test_lrange(rng, expected):
    resetlist(key)
    assert db.lrange(key, *rng) == expected


@pytest.mark.parametrize("rng, expected", lrange_data)
def test_ltrim(rng, expected):
    resetlist(key)
    db.ltrim(key, *rng)
    assert getlist(key) == expected
