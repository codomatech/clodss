# -*- coding: utf-8 -*-

'''
common utilities
'''


SEP = chr(0x2c3)

def _clearexpired(instance, db, key):
    if instance.checkexpired(key) in (True, 'scheduled'):
        del db[f'{SEP}expire{SEP}{key}']
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
