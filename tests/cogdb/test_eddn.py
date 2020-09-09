"""
Tests for cogdb.eddn
"""
import json

import cog.util


with open(cog.util.rel_to_abs('tests', 'eddn_data', 'journal')) as fin:
    JOURNAL = json.loads(fin.read())


def test_journal_parse():
    for obj in JOURNAL[:3]:
        for key in obj:
            print(key)
            print(obj[key])
