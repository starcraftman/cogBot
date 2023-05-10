"""
Tests for cogdb.spansh
"""
import glob
import io
import json
import os
import pathlib
import shutil
import tempfile

import pytest
import sqlalchemy as sqla

import cogdb
import cogdb.eddb
import cogdb.spansh
from cogdb.spansh import (
    SEP, SCommodityGroup, SCommodity, SCommodityPricing,
    SModuleGroup, SModule, SModuleSold)
import cog.util
JSON_PATH = cog.util.rel_to_abs('tests', 'cogdb', 'spansh_rana.json')
BODIES_PATH = cog.util.rel_to_abs('tests', 'cogdb', 'spansh_61cygni.json')
FAKE_GALAXY = cog.util.rel_to_abs('tests', 'cogdb', 'fake_galaxy.json')
SPANSH_ID = 999999


@pytest.fixture
def f_json():
    with open(JSON_PATH, 'r', encoding='utf-8') as fin:
        yield json.loads(fin.read())[0]


@pytest.fixture
def f_json_bodies():
    with open(BODIES_PATH, 'r', encoding='utf-8') as fin:
        yield json.loads(fin.read())[0]


def test_spansh_commodity_group__repr__():
    found = SCommodityGroup(id=SPANSH_ID, name='TestCommGroup')
    assert "SCommodityGroup(id=999999, name='TestCommGroup')" == repr(found)


def test_spansh_commodity__repr__(eddb_session):
    found = SCommodity(id=SPANSH_ID, group_id=SPANSH_ID, name='TestCommodity')
    assert "SCommodity(id=999999, group_id=999999, name='TestCommodity')" == repr(found)


def test_spansh_module_group__repr__(eddb_session):
    found = SModuleGroup(id=SPANSH_ID, name='TestModGroup')
    assert "SModuleGroup(id=999999, name='TestModGroup')" == repr(found)


def test_spansh_module__repr__(eddb_session):
    found = SModule(id=SPANSH_ID, group_id=SPANSH_ID, name='TestModule')
    assert "SModule(id=999999, group_id=999999, ship_id=None, name='TestModule', symbol=None, mod_class=None, rating=None)" == repr(found)


def test_spansh_module_sold__repr__(eddb_session):
    found = SModuleSold(id=SPANSH_ID, station_id=1, module_id=SPANSH_ID)
    assert "SModuleSold(id=999999, station_id=1, module_id=999999)" == repr(found)


def test_spansh_commodity_pricing__repr__(eddb_session):
    found = SCommodityPricing(id=SPANSH_ID, station_id=1, commodity_id=SPANSH_ID, demand=1000, supply=1000, buy_price=5000, sell_price=2500)
    assert "SCommodityPricing(id=999999, station_id=1, commodity_id=999999, demand=1000, supply=1000, buy_price=5000, sell_price=2500)" == repr(found)


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
    assert cogdb.spansh.station_key(system='Rana', station=info) == f"Rana{SEP}Wescott Hub"

    info = {
        'name': 'Wescott Hub',
    }
    assert cogdb.spansh.station_key(system='Rana', station=info) == f"Rana{SEP}Wescott Hub"


def test_eddb_maps(f_spy_ships, eddb_session):
    result = cogdb.spansh.eddb_maps(eddb_session)
    assert 'stations' in result
    assert 'power' in result


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
    station_id = [key for key, entry in results.items() if entry['station']['name'] == 'Zholobov Gateway'][0]
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
        'controlling_minor_faction_id': mapped['factions']['Elite Rebel Force'],
        'distance_to_star': 24.044962,
        'id': station_id,
        'max_landing_pad_size': 'L',
        'name': 'Broglie Terminal',
        'system_id': system_id,
        'type_id': 7,
        'updated_at': 1681744634.0
    }
    assert expect == results[station_id]['station']


def test_transform_galaxy_json():
    with tempfile.NamedTemporaryFile() as tfile:
        tdir = pathlib.Path(tfile.name).parent
        try:
            shutil.copyfile(FAKE_GALAXY, tfile.name)
            all_stations = cogdb.spansh.transform_galaxy_json(0, 1, tfile.name)
            with open(tdir / 'systems.json.00', 'r', encoding='utf-8') as fin:
                found = eval(fin.read())
                assert found[0]['name'] == '61 Cygni'
            with open(tdir / 'factions.json.00', 'r', encoding='utf-8') as fin:
                found = eval(fin.read())
                assert '61 Cygni Commodities' in [x['name'] for x in found]
            if cogdb.spansh.STATIONS_IN_MEMORY:
                assert 'J0J-N7X' in [x['station']['name'] for x in all_stations]
            else:
                with open(tdir / 'stations.json.00', 'r', encoding='utf-8') as fin:
                    next(fin)
                    found = eval(next(fin))
                    assert 'J0J-N7X' == found['station']['name']
        finally:
            for fname in glob.glob(str(tdir / '*.json.00')):
                os.remove(fname)


def test_update_name_map():
    missing_systems = ['system4', 'system6']
    known_systems = {
        'system1': 1,
        'system2': 2,
        'system3': 5,
        'system5': 10,
    }
    with tempfile.NamedTemporaryFile(mode='w') as known_tfile, tempfile.NamedTemporaryFile(mode='w') as new_tfile:
        json.dump(known_systems, known_tfile)
        known_tfile.flush()
        json.dump({}, new_tfile)
        new_tfile.flush()
        cache = {
            'known': known_systems,
            'new': {},
            'next_id': 11,
        }
        cogdb.spansh.update_name_map(
            missing_systems, name_map=cache
        )

        expect = {
            "system1": 1,
            "system2": 2,
            "system3": 5,
            "system4": 11,
            "system5": 10,
            "system6": 12,
        }
        assert cache['known'] == expect
        expect = {
            "system4": 11,
            "system6": 12,
        }
        assert cache['new'] == expect


def test_collect_unique_names():
    expect = [
        '61 Cygni Commodities',
        '61 Cygni Dragons',
        '61 Cygni Liberty Party',
        '61 Cygni Resistance',
        "Barnard's Star Advanced Corp.",
        'Elite Rebel Force',
        'Kruger 60 Free'
    ], ['61 Cygni'], [
        '61 Cygni||Broglie Terminal',
        '61 Cygni||Weber Hub',
        'H0G-W2Y',
        'H0X-5QF',
        'J0J-N7X',
        'J7G-30L',
        'JZQ-8HZ',
        'K1G-55N',
        'N4G-BSZ',
        'T3Z-06W',
        'V2K-THT',
        'V7Y-L6B',
        'VHB-33N'
    ]
    assert expect == cogdb.spansh.collect_unique_names(FAKE_GALAXY)


def test_determine_missing_keys(eddb_session):
    mapped = cogdb.spansh.eddb_maps(eddb_session)
    missing = cogdb.spansh.determine_missing_keys(['X', 'Y', 'Z'], [], [], mapped=mapped)
    assert ['X', 'Y', 'Z'] == missing[0]


def test_collect_modules_and_commodities():
    expect_mod_groups = [
        'hardpoint',
        'internal',
        'standard',
        'utility',
    ]
    expect_comm_groups = [
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
    ]
    mods, comms, mod_groups, comm_groups = cogdb.spansh.collect_modules_and_commodities(FAKE_GALAXY)
    mod = mods[128049250]
    assert mod['symbol'] == 'SideWinder_Armour_Grade1'
    commodity = comms[128672308]
    assert commodity['symbol'] == 'ThermalCoolingUnits'
    assert expect_comm_groups == comm_groups
    assert expect_mod_groups == mod_groups


def test_generate_module_commodities_caches(eddb_session):
    expect = """{
    "group_id": 3,
    "id": 128049250,
    "mod_class": 1,
    "name": "Lightweight Alloy",
    "rating": "I",
    "ship_id": 31,
    "symbol": "SideWinder_Armour_Grade1"
  },"""
    saved = cogdb.common.PRELOAD_DIR
    tempdir = tempfile.mkdtemp()
    try:
        with tempfile.NamedTemporaryFile() as tfile:
            fname = str(tfile.name)
            shutil.copyfile(FAKE_GALAXY, fname)
            cogdb.spansh.cogdb.common.PRELOAD_DIR = tempdir
            cogdb.spansh.generate_module_commodities_caches(eddb_session, fname)
            result_dir = pathlib.Path(cogdb.common.PRELOAD_DIR)
            assert (result_dir / 'SCommodityGroup.json').exists()
            assert (result_dir / 'SCommodity.json').exists()
            assert (result_dir / 'SModuleGroup.json').exists()
            result = result_dir / "SModule.json"
            with open(result, 'r', encoding='utf-8') as fin:
                assert expect in fin.read()
    finally:
        shutil.rmtree(tempdir)
        cogdb.common.PRELOAD_DIR = saved


def test_merge_factions():
    with tempfile.NamedTemporaryFile(mode='w') as factions,\
         tempfile.NamedTemporaryFile(mode='w') as controllings,\
         tempfile.NamedTemporaryFile(suffix='.json.unique') as outfname:
        factions.write("""[
{'id': 1, 'name': 'Faction1'},
{'id': 2, 'name': 'Faction2'},
]""")
        factions.flush()
        controllings.write("""[
{'id': 1, 'name': 'Faction1'},
{'id': 3, 'name': 'FactionStub'},
]""")
        controllings.flush()
        cogdb.spansh.merge_factions([factions.name], [controllings.name], outfname.name)
        with open(outfname.name, 'r', encoding='utf-8') as fin:
            assert len(eval(fin.read())) == 3
        with open(outfname.name.replace('unique', 'correct'), 'r', encoding='utf-8') as fin:
            assert len(eval(fin.read())) == 1


def test_dump_commodities_and_modules(eddb_session):
    expect_comms = "INSERT INTO `spansh_commodity_pricing` (station_id,commodity_id,demand,supply,buy_price,sell_price) VALUES (None,1000,100,1000,50,25),(None,1001,100,1000,50,25);\n"
    expect_mods = "INSERT INTO `spansh_modules_sold` (station_id,module_id) VALUES (20,1000),(20,1001),(20,1002);\n"

    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8') as comms_out,\
            tempfile.NamedTemporaryFile(mode='w', encoding='utf-8') as mods_out,\
            tempfile.NamedTemporaryFile(mode='w', encoding='utf-8') as tfile:
        comms_out.write(expect_comms)
        comms_out.flush()
        mods_out.write(expect_mods)
        mods_out.flush()

        cogdb.spansh.dump_commodities_modules(comms_out.name, mods_out.name, fname=tfile.name)
        expected_fname = pathlib.Path(tfile.name)
        assert expected_fname.exists()
        with open(expected_fname, 'r', encoding='utf-8') as fin:
            text = fin.read()
            assert expect_comms in text
            assert expect_mods in text


def test_manual_overrides(eddb_session):
    try:
        found = eddb_session.query(cogdb.eddb.Faction).filter(cogdb.eddb.Faction.id == 75878).one()
        found.allegiance_id = 4
        eddb_session.commit()
        eddb_session.close()

        cogdb.spansh.manual_overrides(eddb_session)

        assert eddb_session.query(cogdb.eddb.Faction).filter(cogdb.eddb.Faction.id == 75878).one().allegiance_id == 2
    except sqla.exc.NoResultFound:
        pass


def test_cleanup_scratch_files():
    tdir = tempfile.mkdtemp()
    try:
        for name in ['factions.json.correct', 'systems.json.unique', 'systems.json.00', 'dump.sql']:
            with open(os.path.join(tdir, name), 'w', encoding='utf-8') as fout:
                fout.write('')
        cogdb.spansh.cleanup_scratch_files(tdir)
        assert not glob.glob(os.path.join(tdir, '*'))
    finally:
        shutil.rmtree(tdir)


def test_commswriter_update_comms():
    expect = "INSERT INTO `spansh_commodity_pricing` (station_id,commodity_id,demand,supply,buy_price,sell_price) VALUES (67790,128924331,0,0,615864,0),(67790,128924330,500,0,6000,0);\n"
    comms_out, mods_out = io.StringIO(), io.StringIO()
    writer = cogdb.spansh.CommsModsWriter(comms_out, mods_out, line_limit=100)
    fake_comms = [{
        'buy_price': 615864,
        'commodity_id': 128924331,
        'demand': 0,
        'sell_price': 0,
        'station_id': 67790,
        'supply': 0,
    }, {
        'buy_price': 6000,
        'commodity_id': 128924330,
        'demand': 500,
        'sell_price': 0,
        'station_id': 67790,
        'supply': 0,
    }]
    writer.update_comms(fake_comms)
    writer.finish()
    assert expect == comms_out.getvalue()


def test_commswriter_update_mods():
    expect = "INSERT INTO `spansh_modules_sold` (station_id,module_id) VALUES (67790,128049511),(67790,128049512);\n"
    comms_out, mods_out = io.StringIO(), io.StringIO()
    writer = cogdb.spansh.CommsModsWriter(comms_out, mods_out, line_limit=100)

    fake_mods = [
        {'module_id': 128049511, 'station_id': 67790},
        {'module_id': 128049512, 'station_id': 67790},
    ]
    writer.update_mods(fake_mods)
    writer.finish()
    assert expect == mods_out.getvalue()


def test_commswriter_finish():
    comms_out, mods_out = io.StringIO(), io.StringIO()
    writer = cogdb.spansh.CommsModsWriter(comms_out, mods_out, line_limit=100)
    fake_comms = [{
        'buy_price': 615864,
        'commodity_id': 128924331,
        'demand': 0,
        'sell_price': 0,
        'station_id': 67790,
        'supply': 0,
    }, {
        'buy_price': 6000,
        'commodity_id': 128924330,
        'demand': 500,
        'sell_price': 0,
        'station_id': 67790,
        'supply': 0,
    }]
    writer.update_comms(fake_comms)
    writer.finish()
    assert comms_out.getvalue().endswith(';\n')
