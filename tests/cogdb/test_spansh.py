"""
Tests for cogdb.spansh
"""
import json
import os
import shutil
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


def test_date_to_timestamp(f_json):
    assert 1680460158.0 == cogdb.spansh.date_to_timestamp(f_json['date'])


def test_parse_station_features(f_json):
    features = ['Dock', 'Market', 'Repair', 'Black Market']
    feature = cogdb.spansh.parse_station_features(features, station_id=20, updated_at=None)
    assert feature['blackmarket']
    assert not feature['interstellar_factors']


def test_load_commodities(f_json, eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    comm = cogdb.spansh.SCommodity(id=128672125, group_id=10, name='Occupied Escape Pod')

    for station in f_json['stations']:
        if station['name'] == 'J9X-00M':
            if 'market' in station and 'commodities' in station['market'] and station['market']['commodities']:
                comms = cogdb.spansh.load_commodities(station=station, mapped=mapped)
                assert comm in comms
                break


def test_load_commodity_pricing(f_json, eddb_session):
    pricing = {
        'buy_price': 615864,
        'commodity_id': 128924331,
        'demand': 0,
        'sell_price': 0,
        'station_id': 67790,
        'supply': 0,
        'updated_at': 1659908548.0
    }

    for station in f_json['stations']:
        if station['name'] == 'J9X-00M':
            if 'market' in station and 'commodities' in station['market'] and station['market']['commodities']:
                pricings = cogdb.spansh.load_commodity_pricing(station=station, station_id=67790)
                found = [x for x in pricings if x['commodity_id'] == 128924331][0]
                assert pricing == found
                break


def test_load_modules(f_json, eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    expect = cogdb.spansh.SModule(id=128777340, group_id=2, ship_id=None, name='Repair Limpet Controller', symbol='Int_DroneControl_Repair_Size5_Class4', mod_class=5, rating='B')
    for station in f_json['stations']:
        if 'outfitting' in station and 'modules' in station['outfitting'] and station['outfitting']['modules']:
            found = cogdb.spansh.load_modules(station=station, mapped=mapped)
            assert expect in found
            return


def test_load_modules_sold(f_json, eddb_session):
    expect = {'module_id': 128049511, 'station_id': 67790, 'updated_at': 1659908548.0}
    for station in f_json['stations']:
        if 'outfitting' in station and 'modules' in station['outfitting'] and station['outfitting']['modules']:
            found = cogdb.spansh.load_modules_sold(station=station, station_id=67790)
            assert expect == found[0]
            return


def test_load_system(f_json, eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    expect = {
        'controlling_minor_faction_id': None,
        'ed_system_id': 83852530386,
        'id': 15976,
        'name': 'Rana',
        'population': 17566027075,
        'power_id': 9,
        'power_state_id': 16,
        'primary_economy_id': 1,
        'secondary_economy_id': 4,
        'security_id': 48,
        'updated_at': 1680460158.0,
        'x': 6.5,
        'y': -21,
        'z': -19.65625
    }
    assert expect == cogdb.spansh.load_system(data=f_json, mapped=mapped)


def test_load_factions(f_json, eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    results = cogdb.spansh.load_factions(data=f_json, mapped=mapped, system_id=15976)
    faction_id = 67059
    expect = {
        'faction': {
            'allegiance': 'Federation',
            'allegiance_id': 3,
            'government': 'Corporate',
            'government_id': 64,
            'id': 67059,
            'name': 'Earth Defense Fleet',
            'state': 'None',
            'updated_at': 1680460158.0
        },
        'influence': {
            'faction_id': 67059,
            'happiness_id': None,
            'influence': 0.46339,
            'is_controlling_faction': True,
            'system_id': 15976,
            'updated_at': 1680460158.0
        },
        'state': {
            'faction_id': 67059,
            'state_id': None,
            'system_id': 15976
        }
    }
    assert expect == results[faction_id]
    assert set(results[faction_id].keys()) == {'faction', 'influence', 'state'}


def test_load_stations(f_json, eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    station_id = 67790
    expect = {
        'controlling_minor_faction_id': 77170,
        'distance_to_star': 1892.207358,
        'id': 67790,
        'max_landing_pad_size': 'L',
        'name': 'J9X-00M',
        'system_id': 15976,
        'type_id': 24,
        'updated_at': 1659908548.0
    }
    results = cogdb.spansh.load_stations(data=f_json, mapped=mapped, system_id=15976)

    assert results[station_id]['station'] == expect
    assert list(sorted(results.keys())) == [11936, 11940, 30145, 49257, 50910, 51460, 55266, 55569, 56837, 67756, 67790, 70338, 71404, 71709, 71876, 72671, 74550, 82881, 88199, 89516, 89759, 98943, 101219, 102555, 103548, 103830, 104507, 105054, 109593, 111433, 492553]
    assert list(sorted(results[station_id].keys())) == ['commodity_pricing', 'economy', 'features', 'modules_sold', 'station']


def test_load_bodies(f_json, eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    results = cogdb.spansh.load_bodies(data=f_json, mapped=mapped, system_id=15976)
    expect = {
        'controlling_minor_faction_id': 77170,
        'distance_to_star': 671.233016,
        'id': 98362,
        'max_landing_pad_size': 'L',
        'name': 'T9K-T4H',
        'system_id': 15976,
        'type_id': 24,
        'updated_at': 1676028812.0
    }
    assert results[98362]['station'] == expect


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


@pytest.mark.skipif(not os.environ.get('ALL_TESTS'), reason='Very slow test.')
def test_generate_name_maps_from_eddb(eddb_session):
    try:
        tdir = tempfile.mkdtemp()
        cogdb.spansh.generate_name_maps_from_eddb(eddb_session, path=tdir)
        with open(os.path.join(tdir, 'factionMap.json'), 'r', encoding='utf-8') as fin:
            cnt = 0
            seen = False
            for line in fin:
                cnt += 1
                if cnt == 5:
                    break

                if "Fearless" in line:
                    seen = True

            assert seen
    finally:
        shutil.rmtree(tdir)


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
            expect = """{
  "station1": 1,
  "station2": 2,
  "station3": 5,
  "station4": 3,
  "station5": 10,
  "station6": 4
}"""
            assert fin.read() == expect
        with open(new_tfile.name, 'r', encoding='utf-8') as fin:
            expect = """{
  "station4": 3,
  "station6": 4
}"""
            assert fin.read() == expect


def test_eddb_maps(eddb_session):
    result = cogdb.spansh.eddb_maps(eddb_session)
    assert 'stations' in result
    assert 'power' in result


@pytest.mark.skipif(not os.environ.get('ALL_TESTS'), reason='Very slow test.')
def test_collect_modules_and_commodity_groups():
    expect = (
        {
            'hardpoint',
            'internal',
            'standard',
            'utility',
        },
        {
            'Chemicals',
            'Consumer Items',
            'Foods',
            'Industrial Materials',
            'Legal Drugs',
            'Machinery',
            'Medicines',
            'Metals',
            'Minerals',
            'Salvage',
            'Slavery',
            'Technology',
            'Textiles',
            'Waste',
            'Weapons'
        }
    )
    assert expect == cogdb.spansh.collect_modules_and_commodity_groups()
