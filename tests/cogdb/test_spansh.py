"""
Tests for cogdb.spansh
"""
import json
import tempfile

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


def test_parse_date(f_json):
    assert 1680460158.0 == cogdb.spansh.date_to_timestamp(f_json['date'])


def test_parse_station_features(f_json):
    features = ['Dock', 'Market', 'Repair', 'Black Market']
    feature = cogdb.spansh.parse_station_features(features, station_id=20, updated_at=None)
    assert feature.blackmarket
    assert not feature.interstellar_factors


def test_load_system(f_json, eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    result = cogdb.spansh.load_system(data=f_json, mapped=mapped)
    expect = cogdb.eddb.System(id=15976, name='Rana', population=17566027075, needs_permit=None, updated_at=1680460158.0, power_id=9, edsm_id=None, primary_economy_id=1, secondary_economy_id=4, security_id=48, power_state_id=16, controlling_minor_faction_id=67059, x=6.5, y=-21, z=-19.65625)
    assert expect == result


def test_load_factions(f_json, eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    factions, infs = cogdb.spansh.load_factions(data=f_json, mapped=mapped, system_id=15976)
    expect = cogdb.eddb.Faction(id=75397, name='Aegis of Federal Democrats', state_id=None, government_id=96, allegiance_id=3, home_system_id=None, is_player_faction=None, updated_at=None)
    assert expect in factions


def test_load_stations(f_json, eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    results = cogdb.spansh.load_stations(data=f_json, mapped=mapped, system_id=15976)
    expect = cogdb.eddb.Station(id=3706437120, name='J9X-00M', distance_to_star=1892.207358, max_landing_pad_size='L', type_id=24, system_id=15976, controlling_minor_faction_id=77170, updated_at=1659908548.0)
    assert expect in results[0]


def test_load_bodies(f_json, eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    results = cogdb.spansh.load_bodies(data=f_json, mapped=mapped, system_id=15976)
    expect = cogdb.eddb.Station(id=3709136384, name='T9K-T4H', distance_to_star=671.233016, max_landing_pad_size='L', type_id=24, system_id=15976, controlling_minor_faction_id=77170, updated_at=1676028812.0)
    assert expect in results[0]


def test_split_csv_line():
    text = '09117,1231564,"WDS J05353-0522Ja,Jb",605.3125,-439.25,-1092.46875,0,0,176,None,5,None,64,Anarchy,10,None,,,,0,1679067630,1679067630,"PSH 136",,,1,Pristine,367915889796081'
    assert len(cogdb.spansh.split_csv_line(text)) == 28

    text = '17,60,"10 Ursae Majoris",0.03125,34.90625,-39.09375,0,0,176,None,5,None,64,Anarchy,10,None,,,,0,1680292128,1680292128,"10 Ursae Majoris",,,3,Common,2415659059555'
    items = cogdb.spansh.split_csv_line(text)
    assert text.split(',') == items


def test_system_csv_importer():
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        system = next(cogdb.spansh.systems_csv_importer(eddb_session, cogdb.spansh.SYSTEMS_CSV))
        assert system.name == '10 Ursae Majoris'


def test_update_name_map():
    missing_stations = ['station4', 'station6']
    known_stations = {
        'station1': 1,
        'station2': 2,
        'station3': 5,
        'station5': 10,
    }
    with tempfile.NamedTemporaryFile(mode='w') as known_tfile, tempfile.NamedTemporaryFile(mode='w') as new_tfile:
        json.dump(known_stations, known_tfile)
        known_tfile.flush()
        json.dump({}, new_tfile)
        new_tfile.flush()
        cogdb.spansh.update_name_map(
            missing_stations, known_fname=known_tfile.name, new_fname=new_tfile.name
        )

        with open(known_tfile.name, 'r', encoding='utf-8') as fin:
            assert fin.read() == '{"station1": 1, "station2": 2, "station3": 5, "station5": 10, "station4": 3, "station6": 4}'
        with open(new_tfile.name, 'r', encoding='utf-8') as fin:
            assert fin.read() == '{"station4": 3, "station6": 4}'


def test_eddb_maps():
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        result = cogdb.spansh.eddb_maps(eddb_session)
        assert 'stations' in result
        assert 'power' in result


#  def test_spansh_basic(f_json):
    #  print(f_json['date'])
    #  print(sorted(f_json.keys()))
    #  print(sorted(f_json['factions'][0].keys()))
    #  __import__('pprint').pprint(f_json['factions'][0])
    #  print(sorted(f_json['stations'][0].keys()))


#  def test_spansh_query():
    #  with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        #  faction_map = {x.name: x.id for x in eddb_session.query(cogdb.eddb.Faction)}
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


#  def test_verify_factions():
    #  results = cogdb.spansh.verify_galaxy_ids(cog.util.rel_to_abs('galaxy_stations.json'))
    #  with open('/tmp/out.txt', 'w', encoding='utf-8') as fout:
        #  __import__('pprint').pprint(results, fout)
