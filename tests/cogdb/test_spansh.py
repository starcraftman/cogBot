"""
Tests for cogdb.spansh
"""
import json

import pytest

import cogdb
import cogdb.eddb
import cogdb.spansh
import cog.util
JSON_PATH = cog.util.rel_to_abs('tests', 'cogdb', 'spansh_rana.json')


@pytest.fixture
def f_json():
    with open(JSON_PATH, 'r', encoding='utf-8') as fin:
        yield json.loads(fin.read())[0]


def test_spansh_basic(f_json):
    print(f_json['date'])
    print(sorted(f_json.keys()))
    print(sorted(f_json['factions'][0].keys()))
    __import__('pprint').pprint(f_json['factions'][0])
    print(sorted(f_json['stations'][0].keys()))


def test_spansh_query():
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        faction_map = {x.name: x.id for x in eddb_session.query(cogdb.eddb.Faction)}
        #  system_map = {x.name: x.id for x in eddb_session.query(cogdb.eddb.System)}
        #  station_map = {x.name: x.id for x in eddb_session.query(cogdb.eddb.Station)}
        #  pairings = [
            #  ['factionMap.json', faction_map],
            #  ['systemMap.json', system_map],
            #  ['stationMap.json', station_map]
        #  ]
        #  for fname, themap in pairings:
            #  fpath = cog.util.rel_to_abs('data', fname)
            #  with open(fpath, 'w', encoding='utf-8') as fout:
                #  json.dump(themap, fout, indent=4, sort_keys=True)


def test_verify_factions():
    results = cogdb.spansh.verify_galaxy_ids(cog.util.rel_to_abs('galaxy_stations.json'))
    with open('/tmp/out.txt', 'w', encoding='utf-8') as fout:
        __import__('pprint').pprint(results, fout)
