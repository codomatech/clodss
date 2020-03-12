"""
common utilities
"""


def keyexists(keytype, tables, instance, db, key,
              create=True, columns=None, initquery=None):
    'checks if a key of given type exists, optionally creates one if not'

    wrongtypemsg = f'key `{key}` already exists with a different type'
    existingtype = instance.knownkeys.get(key)
    if existingtype:
        if existingtype != keytype:
            raise ValueError(wrongtypemsg)
        return True
    existing = db.execute('SELECT name FROM sqlite_master WHERE '
                          f'type="table" AND name LIKE "{key}%"').fetchall()
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
