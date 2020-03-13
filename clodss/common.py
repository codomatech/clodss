# -*- coding: utf-8 -*-

'''
common utilities
'''


def keyexists(keytype, tables, instance, db, key,
              create=True, columns=None, initquery=None):
    'checks if a key of given type exists, optionally creates one if not'

    wrongtypemsg = f'key `{key}` already exists with a different type'
    existingtype = instance.knownkeys.get(key)
    if existingtype:
        if existingtype != keytype:
            raise ValueError(wrongtypemsg)
        return True
    ekey = key.replace('"', '""')
    existing = db.execute('SELECT name FROM sqlite_master WHERE '
                          f'type="table" AND name LIKE "{ekey}%"').fetchall()
    existing = {record[0] for record in existing}
    if existing and existing != set(tables):
        raise ValueError(wrongtypemsg)

    if not existing:
        if not create:
            return False
        if not columns:
            columns = 'value TEXT'
        for table in tables:
            db.execute(f'CREATE TABLE `{table}` ({columns})')
            if initquery:
                db.execute(initquery)

    instance.knownkeys[key] = keytype
    return True


def _clearexpired(instance, db, key):
    if instance.checkexpired(key) in (True, 'scheduled'):
        db.execute('DELETE FROM `﹁expiredkeys﹁` WHERE key=?', (key,))
        db.commit()
        if key in instance.keystoexpire:
            del instance.keystoexpire[key]


class ProblematicSymbols:
    'remove symbols that could cause sql injection or use reserved words'

    FWD = {
        'sqlite_': 'sqlite﹁',
        '`': "﹁'﹁",
    }

    BWD = {v: k for k, v in FWD.items()}

    @staticmethod
    def remove(text):
        'remove symbols'
        for kw, rep in ProblematicSymbols.FWD.items():
            text = text.replace(kw, rep)
        return text

    @staticmethod
    def restore(text):
        'resotre symbols'
        for kw, rep in ProblematicSymbols.BWD.items():
            text = text.replace(kw, rep)
        return text
