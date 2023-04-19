"""
Tests for cogdb.spansh
"""
import json
import os
import tempfile

import pytest

import cogdb
import cogdb.eddb
import cogdb.spansh
import cog.util
JSON_PATH = cog.util.rel_to_abs('tests', 'cogdb', 'spansh_rana.json')
BODIES_PATH = cog.util.rel_to_abs('tests', 'cogdb', 'spansh_61cygni.json')


@pytest.fixture
def f_json():
    with open(JSON_PATH, 'r', encoding='utf-8') as fin:
        yield json.loads(fin.read())[0]


@pytest.fixture
def f_json_bodies():
    with open(BODIES_PATH, 'r', encoding='utf-8') as fin:
        yield json.loads(fin.read())[0]


def test_date_to_timestamp(f_json):
    assert 1680460158.0 == cogdb.spansh.date_to_timestamp(f_json['date'])


def test_station_key_fleet_carrier():
    info = {
        'name': 'X79-B2S',
        'type': 'Drake-Class Carrier',
    }

    assert cogdb.spansh.station_key(system='Rana', station=info) == info['name']


def test_station_key_starport():
    info = {
        'name': 'Wescott Hub',
        "type": "Orbis Starport",
    }
    assert cogdb.spansh.station_key(system='Rana', station=info) == "Rana_Wescott Hub"

    info = {
        'name': 'Wescott Hub',
    }
    assert cogdb.spansh.station_key(system='Rana', station=info) == "Rana_Wescott Hub"


def test_parse_station_features(f_json):
    features = ['Dock', 'Market', 'Repair', 'Black Market']
    feature = cogdb.spansh.parse_station_features(features, station_id=20, updated_at=None)
    assert feature['blackmarket']
    assert not feature['interstellar_factors']


def test_transform_commodities(f_json, f_spy_ships, eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    expect = cogdb.spansh.SCommodity(id=128924331, group_id=9, name='Alexandrite')

    for station in f_json['stations']:
        comms = cogdb.spansh.transform_commodities(station=station, mapped=mapped)
        assert expect == list(sorted(comms))[0]

        break


def test_transform_commodity_pricing(f_json, f_spy_ships, eddb_session):
    pricing = {
        'buy_price': 615864,
        'commodity_id': 128924331,
        'demand': 0,
        'sell_price': 0,
        'station_id': 67790,
        'supply': 0,
    }

    for station in f_json['stations']:
        if station['name'] == 'J9X-00M':
            if 'market' in station and 'commodities' in station['market'] and station['market']['commodities']:
                pricings = cogdb.spansh.transform_commodity_pricing(station=station, station_id=67790)
                found = [x for x in pricings if x['commodity_id'] == 128924331][0]
                assert pricing == found
                break


def test_transform_modules(f_json, f_spy_ships, eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    expect = cogdb.spansh.SModule(id=128777340, group_id=2, ship_id=None, name='Repair Limpet Controller', symbol='Int_DroneControl_Repair_Size5_Class4', mod_class=5, rating='B')
    for station in f_json['stations']:
        if 'outfitting' in station and 'modules' in station['outfitting'] and station['outfitting']['modules']:
            found = cogdb.spansh.transform_modules(station=station, mapped=mapped)
            assert expect in found
            return


def test_transform_modules_sold(f_json, f_spy_ships, eddb_session):
    expect = {'module_id': 128049511, 'station_id': 67790}
    for station in f_json['stations']:
        if 'outfitting' in station and 'modules' in station['outfitting'] and station['outfitting']['modules']:
            found = cogdb.spansh.transform_modules_sold(station=station, station_id=67790)
            assert expect == found[0]
            return


def test_transform_system(f_json, f_spy_ships, eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    results = cogdb.spansh.transform_system(data=f_json, mapped=mapped)
    expect = {
        'controlling_minor_faction_id': None,
        'ed_system_id': 83852530386,
        'id': results['id'],
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
    assert expect == results


def test_transform_factions(f_json, f_spy_ships, eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    system_id = mapped['systems']['Rana']
    results = cogdb.spansh.transform_factions(data=f_json, mapped=mapped, system_id=system_id)

    faction_id = [x['faction'] for x in results.values() if x['faction']['name'] == 'Earth Defense Fleet'][0]['id']
    expect = {
        'faction': {
            'allegiance_id': 3,
            'government_id': 64,
            'id': faction_id,
            'name': 'Earth Defense Fleet',
            'state_id': 80,
            'updated_at': 1680460158.0,
        },
        'influence': {
            'faction_id': faction_id,
            'happiness_id': None,
            'influence': 0.46339,
            'is_controlling_faction': True,
            'system_id': system_id,
            'updated_at': 1680460158.0,
        },
        'state': {
            'faction_id': faction_id,
            'state_id': 80,
            'system_id': system_id,
            'updated_at': 1680460158.0},
    }

    assert faction_id in results
    assert expect == results[faction_id]
    assert set(results[faction_id].keys()) == {'faction', 'influence', 'state'}


def test_transform_stations(f_json, f_spy_ships, eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    system_id = mapped['systems']['Rana']
    results = cogdb.spansh.transform_stations(data=f_json, mapped=mapped, system_id=system_id, system_name=f_json['name'])
    station_id = [x for x in results if results[x]['station']['name'] == 'Zholobov Gateway'][0]
    expect = {
        'distance_to_star': 1891.965479,
        'id': station_id,
        'max_landing_pad_size': 'L',
        'name': 'Zholobov Gateway',
        'system_id': system_id,
        'type_id': 8,
        'updated_at': 1680456595.0,
    }

    del results[station_id]['station']['controlling_minor_faction_id']
    assert results[station_id]['station'] == expect
    assert len(results.keys()) > 5
    assert list(sorted(results[station_id].keys())) == ['commodity_pricing', 'controlling_factions', 'economy', 'features', 'modules_sold', 'station']


def test_transform_bodies(f_json, f_spy_ships, eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    system_id = mapped['systems']['Rana']
    results = cogdb.spansh.transform_bodies(data=f_json, mapped=mapped, system_id=system_id)
    station_id = list(sorted(results.keys()))[-1]
    station_result = [x['station'] for x in results.values() if x['station']['name'] == 'T9K-T4H'][0]
    expect = {
        'controlling_minor_faction_id': 77170,
        'distance_to_star': 671.233016,
        'id': station_id,
        'max_landing_pad_size': 'L',
        'name': 'T9K-T4H',
        'system_id': system_id,
        'type_id': 24,
        'updated_at': 1676028812.0
    }
    assert station_result == expect


def test_transform_bodies2(f_json_bodies, f_spy_ships, eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    system_id = mapped['systems']['61 Cygni']
    results = cogdb.spansh.transform_bodies(data=f_json_bodies, mapped=mapped, system_id=system_id)
    station_id = list(sorted(results.keys()))[0]
    expect = {
        'controlling_minor_faction_id': 18530,
        'distance_to_star': 24.044962,
        'id': station_id,
        'max_landing_pad_size': 'L',
        'name': 'Broglie Terminal',
        'system_id': system_id,
        'type_id': 7,
        'updated_at': 1681744634.0
    }
    assert expect == results[station_id]['station']



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


def test_eddb_maps(f_spy_ships, eddb_session):
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
