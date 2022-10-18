"""
Tests for cogdb.spy
"""
import json
import os
import pathlib
import pytest
import tempfile

import sqlalchemy as sqla

import cogdb.spy_squirrel as spy
import cogdb.eddb
from cogdb.schema import FortSystem, UMSystem
import tests.conftest

FIXED_TIMESTAMP = 1662390092
INPUT_UPDATE_EDDB_FACTIONS = {
    "Abi": {
        "factions": {
            "Abi Crimson Raiders": 1,
            "Abi Focus": 8.1,
            "Abi Jet Natural Incorporated": 9.3,
            "Abi Progressive Party": 4.5,
            "Knights of Sol Eternal": 18.3,
            "LFT 1723 Energy Commodities": 8.1,
            "LHS 3899 Crimson Bridge Ind": 1,
            "Tibis Alliance": 49.7
        },
        "retrieved": 1663943357.0,
        "updated_at": 1663943357.0
    },
    "Rana": {
        "factions": {
            "Aegis of Federal Democrats": 8.2,
            "Earth Defense Fleet": 47.5,
            "Independent Rana Labour": 12.6,
            "Rana Flag": 6.3,
            "Rana General Co": 15,
            "Rana Regulatory State": 4.5,
            "Rana State Network": 5.8
        },
        "retrieved": 1663943331.0,
        "updated_at": 1663871673.0
    },
    "Sol": {
        "factions": {
            "Aegis Core": 8.9,
            "Federal Congress": 15.9,
            "Mother Gaia": 29,
            "Sol Constitution Party": 12.2,
            "Sol Nationalists": 11.2,
            "Sol Workers' Party": 22.7
        },
        "retrieved": 1663943306.0,
        "updated_at": 1663870671.0
    }
}

# Empty tables before running tests.
spy.empty_tables()


def load_json(fname):
    """Load a json file example for API testing.

    Args:
        fname: The file to load in tests directory.

    Raises:
        FileNotFoundError: The file required is missing.
    """
    path = pathlib.Path(os.path.join(tempfile.gettempdir(), fname))
    if not path.exists():
        tests.conftest.fetch_json_secret(tempfile.gettempdir(), fname.replace('.json', ''))

    with path.open('r', encoding='utf-8') as fin:
        return json.load(fin)


@pytest.fixture()
def base_json():
    yield load_json('base.json')


@pytest.fixture()
def refined_json():
    yield load_json('refined.json')


@pytest.fixture()
def response_json():
    yield load_json('response.json')


@pytest.fixture()
def scrape_json():
    yield load_json('scrape.json')


@pytest.fixture()
def response_news_json(response_json):
    """Returns the news element of refined json, shortcut."""
    yield response_json['123']['news']


@pytest.fixture()
def empty_spy():
    yield
    spy.empty_tables()


@pytest.fixture()
def spy_test_bed(eddb_session):
    objects = [
        spy.SpyVote(
            power_id=1,
            vote=88,
            updated_at=FIXED_TIMESTAMP,
        ),
        spy.SpyVote(
            power_id=11,
            vote=75,
            updated_at=FIXED_TIMESTAMP,
        ),
        spy.SpyPrep(
            id=1,
            ed_system_id=11665533904241,
            power_id=9,
            merits=10000,
            updated_at=FIXED_TIMESTAMP,
        ),
        spy.SpySystem(
            id=1,
            ed_system_id=10477373803,
            power_id=9,
            power_state_id=16,
            income=122,
            upkeep_current=0,
            upkeep_default=21,
            fort=4000,
            fort_trigger=5211,
            um=40000,
            um_trigger=33998,
            updated_at=FIXED_TIMESTAMP,
        ),
        spy.SpySystem(
            id=2,
            ed_system_id=11665533904241,
            power_id=9,
            power_state_id=64,
            income=0,
            upkeep_current=0,
            upkeep_default=0,
            fort_trigger=4872,
            um_trigger=7198,
            updated_at=FIXED_TIMESTAMP,
        ),
        spy.SpyTraffic(
            id=1,
            cnt=1,
            ship_id=1,
            system="Rana",
            updated_at=FIXED_TIMESTAMP,
        ),
        spy.SpyTraffic(
            id=2,
            cnt=5,
            ship_id=2,
            system="Rana",
            updated_at=FIXED_TIMESTAMP,
        ),
        spy.SpyBounty(
            id=1,
            pos=1,
            cmdr_name="Good guy",
            ship_name="Good guy ship",
            last_seen_system="Rana",
            last_seen_station="Wescott Hub",
            bounty=100000,
            is_local=False,
            ship_id=1,
            power_id=9,
            updated_at=FIXED_TIMESTAMP,
        ),
        spy.SpyBounty(
            id=1,
            pos=2,
            cmdr_name="Bad guy",
            ship_name="Bad guy ship",
            last_seen_system="Rana",
            last_seen_station="Ali Hub",
            bounty=10000000,
            is_local=False,
            ship_id=2,
            power_id=1,
            updated_at=FIXED_TIMESTAMP,
        ),
    ]
    eddb_session.add_all([
        spy.SpyShip(id=1, text="Viper Mk. II"),
        spy.SpyShip(id=2, text="Vulture"),
    ])
    eddb_session.commit()
    eddb_session.add_all(objects)
    eddb_session.commit()

    yield objects
    spy.empty_tables()


def test_spy_ship__repr__(spy_test_bed, eddb_session):
    expect = "SpyShip(id=1, text='Viper Mk. II')"
    ship = eddb_session.query(spy.SpyShip).filter(spy.SpyShip.id == 1).one()

    assert expect == repr(ship)


def test_spy_ship__str__(spy_test_bed, eddb_session):
    expect = "Ship: Viper Mk. II"
    ship = eddb_session.query(spy.SpyShip).filter(spy.SpyShip.id == 1).one()

    assert expect == str(ship)


def test_spy_traffic__repr__(spy_test_bed, eddb_session):
    expect = "SpyTraffic(id=1, cnt=1, ship_id=1, updated_at=1662390092)"
    traffic = eddb_session.query(spy.SpyTraffic).filter(spy.SpyTraffic.id == 1).one()

    assert expect == repr(traffic)


def test_spy_traffic__str__(spy_test_bed, eddb_session):
    expect = "Viper Mk. II: 1"
    traffic = eddb_session.query(spy.SpyTraffic).filter(spy.SpyTraffic.id == 1).one()

    assert expect == str(traffic)


def test_spy_bounty__repr__(spy_test_bed, eddb_session):
    expect = "SpyBounty(id=1, pos=1, cmdr_name='Good guy', ship_name='Good guy ship', last_seen_system='Rana', last_seen_station='Wescott Hub', bounty=100000, is_local=False, ship_id=1, updated_at=1662390092)"
    bounty = eddb_session.query(spy.SpyBounty).filter(spy.SpyBounty.id == 1, spy.SpyBounty.pos == 1).one()

    assert expect == repr(bounty)


def test_spy_bounty__str__(spy_test_bed, eddb_session):
    expect = "#1 Good guy in Rana/Wescott Hub (Viper Mk. II) with 100000, updated at 2022-09-05 15:01:32"
    bounty = eddb_session.query(spy.SpyBounty).filter(spy.SpyBounty.id == 1, spy.SpyBounty.pos == 1).one()

    assert expect == str(bounty)


def test_spy_vote__repr__(spy_test_bed):
    expect = 'SpyVote(power_id=1, vote=88, updated_at=1662390092)'
    spyvote = spy_test_bed[0]

    assert expect == repr(spyvote)


def test_spy_vote__str__(spy_test_bed):
    expect = 'Aisling Duval: 88%, updated at 2022-09-05 15:01:32'
    spyvote = spy_test_bed[0]

    assert expect == str(spyvote)


def test_spy_prep__repr__(spy_test_bed):
    expect = 'SpyPrep(id=1, power_id=9, ed_system_id=11665533904241, merits=10000, updated_at=1662390092)'
    spyprep = [x for x in spy_test_bed if isinstance(x, spy.SpyPrep)][0]

    assert expect == repr(spyprep)


def test_spy_prep__str__(spy_test_bed):
    expect = 'Zachary Hudson Allowini: 10000, updated at 2022-09-05 15:01:32'
    spyprep = [x for x in spy_test_bed if isinstance(x, spy.SpyPrep)][0]

    assert expect == str(spyprep)


def test_spy_system_control__repr__(spy_test_bed):
    expect = 'SpySystem(id=1, ed_system_id=10477373803, power_id=9, power_state_id=16, income=122, upkeep_current=0, upkeep_default=21, fort=4000, fort_trigger=5211, um=40000, um_trigger=33998, updated_at=1662390092)'
    spysystem = [x for x in spy_test_bed if isinstance(x, spy.SpySystem) and not x.is_expansion][0]

    assert expect == repr(spysystem)


def test_spy_system_control__str__(spy_test_bed):
    expect = 'Zachary Hudson Sol: 4000/5211 | 40000/33998, updated at 2022-09-05 15:01:32'
    spysystem = [x for x in spy_test_bed if isinstance(x, spy.SpySystem) and not x.is_expansion][0]

    assert expect == str(spysystem)


def test_spy_system_control__str__not_inserted(spy_test_bed):
    expect = '9 2222: 4000/5211 | 40000/33998, updated at 2022-09-05 15:01:32'
    spysystem = spy.SpySystem(
        id=2, ed_system_id=2222, power_id=9, power_state_id=16, income=122, upkeep_current=0,
        upkeep_default=21, fort=4000, fort_trigger=5211, um=40000, um_trigger=33998,
        updated_at=1662390092
    )

    assert expect == str(spysystem)


def test_spy_system_control_is_expansion(spy_test_bed, eddb_session):
    spysystem = [x for x in spy_test_bed if isinstance(x, spy.SpySystem) and not x.is_expansion][0]
    assert not spysystem.is_expansion
    assert eddb_session.query(spy.SpySystem).\
        filter(sqla.not_(spy.SpySystem.is_expansion)).\
        limit(1).\
        one() == spysystem


def test_spy_system_exp__repr__(spy_test_bed):
    expect = 'SpySystem(id=2, ed_system_id=11665533904241, power_id=9, power_state_id=64, income=0, upkeep_current=0, upkeep_default=0, fort=0, fort_trigger=4872, um=0, um_trigger=7198, updated_at=1662390092)'
    spysystem = [x for x in spy_test_bed if isinstance(x, spy.SpySystem) and x.is_expansion][0]

    assert expect == repr(spysystem)


def test_spy_system_exp__str__(spy_test_bed):
    expect = 'Expansion for Zachary Hudson to Allowini: 0/4872 | 0/7198, updated at 2022-09-05 15:01:32'
    spysystem = [x for x in spy_test_bed if isinstance(x, spy.SpySystem) and x.is_expansion][0]

    assert expect == str(spysystem)


def test_spy_system_exp__str__not_inserted(spy_test_bed):
    expect = 'Expansion for 9 to 2222: 4000/5211 | 40000/33998, updated at 2022-09-05 15:01:32'
    spysystem = spy.SpySystem(
        id=2, ed_system_id=2222, power_id=9, power_state_id=64, income=122, upkeep_current=0,
        upkeep_default=21, fort=4000, fort_trigger=5211, um=40000, um_trigger=33998,
        updated_at=1662390092
    )

    assert expect == str(spysystem)


def test_spy_system_exp_is_expansion(spy_test_bed, eddb_session):
    spysystem = [x for x in spy_test_bed if isinstance(x, spy.SpySystem) and x.is_expansion][0]
    assert spysystem.is_expansion
    assert eddb_session.query(spy.SpySystem).\
        filter(spy.SpySystem.is_expansion).\
        limit(1).\
        one() == spysystem


def test_empty_tables(spy_test_bed, eddb_session):
    for table in spy.SPY_TABLES:
        assert eddb_session.query(table).limit(1).all()

    spy.empty_tables()

    for table in spy.SPY_TABLES:
        assert not eddb_session.query(table).limit(1).all()


def test_load_base_json(empty_spy, base_json, eddb_session):
    # Manually insert to test update paths
    eddb_session.add(
        spy.SpySystem(ed_system_id=10477373803, power_id=9, power_state_id=35),
    )
    eddb_session.commit()

    systems = spy.load_base_json(base_json, eddb_session)

    expect_control = spy.SpySystem(
        ed_system_id=10477373803, power_id=9, power_state_id=16,
        income=122, upkeep_current=0, upkeep_default=21, fort_trigger=5211, um_trigger=33998
    )
    expect_taking = spy.SpySystem(
        ed_system_id=11665533904241, power_id=9, power_state_id=64,
        income=0, upkeep_current=0, upkeep_default=0, fort_trigger=4872, um_trigger=7198
    )
    assert expect_control in systems
    assert expect_taking in systems


def test_load_refined_json(empty_spy, base_json, refined_json, eddb_session):
    # Manually insert to test update paths
    eddb_session.add_all([
        spy.SpyVote(power_id=1, vote=10),
        spy.SpyPrep(power_id=1, ed_system_id=79230372211, merits=0),
    ])
    eddb_session.commit()

    db_objects = spy.load_refined_json(refined_json, eddb_session)

    expect_prep = spy.SpyPrep(power_id=9, ed_system_id=2557887812314, merits=14140)
    expect_vote = spy.SpyVote(power_id=11, vote=78)
    assert expect_prep in db_objects
    assert expect_vote in db_objects


# Combined test of base then refined
def test_load_base_and_refined_json(empty_spy, base_json, refined_json, eddb_session):
    spy.load_base_json(base_json, eddb_session)
    db_objects = spy.load_refined_json(refined_json, eddb_session)

    expect_prep = spy.SpyPrep(power_id=9, ed_system_id=2557887812314, merits=14140)
    expect_vote = spy.SpyVote(power_id=11, vote=78)
    expect_expo = spy.SpySystem(power_id=6, ed_system_id=2106438158699, fort=1247, um=53820)
    expect_sys = spy.SpySystem(power_id=11, ed_system_id=22958210698120, fort=464, um=900)
    assert expect_prep in db_objects
    assert expect_vote in db_objects
    assert expect_expo == eddb_session.query(spy.SpySystem).\
        filter(spy.SpySystem.ed_system_id == 2106438158699).\
        one()
    assert expect_sys == eddb_session.query(spy.SpySystem).\
        filter(spy.SpySystem.ed_system_id == 22958210698120).\
        one()


def test_parse_params(response_news_json):
    expect = {
        'factionName': 'Labour of Rhea',
        'list': '$newsfeed_NewsSummaryHeadlines:#Influence=4.8:#Happiness=$Faction_HappinessBand2;;',
        'system': 'Rhea'
    }
    assert expect == spy.parse_params(response_news_json[0]['params'])


def test_parse_response_news_summary(response_news_json):
    expect = {
        'factionName': 'Labour of Rhea',
        'happiness': 2,
        'influence': 4.8,
        'system': 'Rhea'
    }
    assert expect == spy.parse_response_news_summary(response_news_json[0])


def test_parse_response_trade_goods(response_news_json):
    expect = [
        '$DamagedEscapePod_Name;',
        '$BasicMedicines_Name;',
        '$Algae_Name;',
        '$Painite_Name;',
        '$Alexandrite_Name;',
        '$Opal_Name;',
        '$Gold_Name;',
        '$Tritium_Name;',
        '$Thorium_Name;',
        '$Wine_Name;',
        '$MarineSupplies_Name;'
    ]
    assert expect == spy.parse_response_trade_goods(response_news_json[7])


def test_parse_response_bounties_claimed(response_news_json):
    expect = {
        "bountyCount": 439,
        "bountyValue": 75817971,
    }
    assert expect == spy.parse_response_bounties_claimed(response_news_json[8])


def test_parse_response_top5_bounties(response_news_json):
    expect = {
        1: {
            'bountyValue': 22800,
            'commanderId': 5520548,
            'lastLocation': 'Melici - Polansky Landing',
            'name': 'CMDR SunNamida (Krait Mk II "PRIVATE COURIER")'
        },
        2: {
            'bountyValue': 6300,
            'commanderId': 7686998,
            'lastLocation': 'Djiwal - Thompson Dock',
            'name': 'CMDR Bubba Bo Bob (Keelback "KEELBACK")'
        },
        3: {
            'bountyValue': 2600,
            'commanderId': 6750288,
            'lastLocation': 'NLTT 19808',
            'name': 'CMDR MrSkillin (Keelback)'
        },
        4: {
            'bountyValue': 0,
            'commanderId': 0,
            'lastLocation': '',
            'name': ''
        },
        5: {
            'bountyValue': 0,
            'commanderId': 0,
            'lastLocation': '',
            'name': ''
        },
        'type': 'faction'
    }
    assert expect == spy.parse_response_top5_bounties(response_news_json[10])


def test_parse_response_power_update(response_news_json):
    expect = {
        'power': 'Felicia Winters',
        'held_merits': 0,
        'stolen_fort': 0,
    }
    assert expect == spy.parse_response_power_update(response_news_json[12])


def test_parse_response_traffic_totals(response_news_json):
    expect = {
        'by_ship': {
            'adder': 2,
            'anaconda': 35,
            'asp': 18,
            'belugaliner': 4,
            'cobramkiii': 9,
            'cutter': 37,
            'diamondback': 8,
            'diamondbackxl': 14,
            'dolphin': 4,
            'eagle': 1,
            'empire_eagle': 1,
            'federation_corvette': 20,
            'federation_dropship_mkii': 3,
            'ferdelance': 2,
            'hauler': 1,
            'independant_trader': 6,
            'krait_light': 8,
            'krait_mkii': 31,
            'mamba': 3,
            'orca': 4,
            'python': 17,
            'type6': 7,
            'type7': 1,
            'type9': 24,
            'type9_military': 4,
            'typex': 5,
            'typex_2': 2,
            'typex_3': 2,
            'viper': 4,
            'viper_mkiv': 7,
            'vulture': 3
        },
        'total': 287
    }
    assert expect == spy.parse_response_traffic_totals(response_news_json[13])


def test_load_response_json(empty_spy, response_json, eddb_session):
    expect = {
        'bountiesClaimed': {'bountyCount': 439, 'bountyValue': 75817971},
        'bountiesGiven': {'bountyCount': 204, 'bountyValue': 26684911},
        'factions': [
            {'factionName': 'Labour of Rhea', 'happiness': 2, 'influence': 4.8, 'system': 'Rhea'},
            {'factionName': 'Rhea Travel Industry', 'happiness': 2, 'influence': 12.2, 'system': 'Rhea'},
            {'factionName': 'Traditional Rhea Front', 'happiness': 2, 'influence': 8.9, 'system': 'Rhea'},
            {'factionName': 'Rhea Travel Interstellar', 'happiness': 2, 'influence': 12.2, 'system': 'Rhea'},
            {'factionName': 'Rhea Crimson Drug Empire', 'happiness': 2, 'influence': 3.3, 'system': 'Rhea'},
            {'factionName': 'East Galaxy Company', 'happiness': 2, 'influence': 19.0, 'system': 'Rhea'},
            {'factionName': 'Federal Liberal Command', 'happiness': 2, 'influence': 39.5, 'system': 'Rhea'}
        ],
        'power': {'stolen_fort': 0, 'power': 'Felicia Winters', 'held_merits': 0},
        'system': 'Rhea',
        'top5': [
            {
                1: {'bountyValue': 22800, 'commanderId': 5520548, 'lastLocation': 'Melici - Polansky Landing', 'name': 'CMDR SunNamida (Krait Mk II "PRIVATE COURIER")'},
                2: {'bountyValue': 6300, 'commanderId': 7686998, 'lastLocation': 'Djiwal - Thompson Dock', 'name': 'CMDR Bubba Bo Bob (Keelback "KEELBACK")'},
                3: {'bountyValue': 2600, 'commanderId': 6750288, 'lastLocation': 'NLTT 19808', 'name': 'CMDR MrSkillin (Keelback)'},
                4: {'bountyValue': 0, 'commanderId': 0, 'lastLocation': '', 'name': ''},
                5: {'bountyValue': 0, 'commanderId': 0, 'lastLocation': '', 'name': ''},
                'type': 'faction'
            },
            {
                1: {'bountyValue': 859000, 'commanderId': 3378194, 'lastLocation': 'Tjial - Cassidy Landing', 'name': 'CMDR Marduk298 (Federal Corvette "CLOACA MUNCH")'},
                2: {'bountyValue': 765000, 'commanderId': 4868506, 'lastLocation': 'Njuwar', 'name': 'CMDR mÃ¡kos guba (Fer-de-Lance "LSI REGULARITY ALPHA")'},
                3: {'bountyValue': 547000, 'commanderId': 7919910, 'lastLocation': 'Bandjigali', 'name': 'CMDR Kalvunath (Anaconda "BABYLON")'},
                4: {'bountyValue': 0, 'commanderId': 0, 'lastLocation': '', 'name': ''},
                5: {'bountyValue': 0, 'commanderId': 0, 'lastLocation': '', 'name': ''},
                'type': 'super'
            }
        ],
        'trade': [
            '$DamagedEscapePod_Name;',
            '$BasicMedicines_Name;',
            '$Algae_Name;',
            '$Painite_Name;',
            '$Alexandrite_Name;',
            '$Opal_Name;',
            '$Gold_Name;',
            '$Tritium_Name;',
            '$Thorium_Name;',
            '$Wine_Name;',
            '$MarineSupplies_Name;'
        ],
        'traffic': {
            'by_ship': {
                'adder': 2,
                'anaconda': 35,
                'asp': 18,
                'belugaliner': 4,
                'cobramkiii': 9,
                'cutter': 37,
                'diamondback': 8,
                'diamondbackxl': 14,
                'dolphin': 4,
                'eagle': 1,
                'empire_eagle': 1,
                'federation_corvette': 20,
                'federation_dropship_mkii': 3,
                'ferdelance': 2,
                'hauler': 1,
                'independant_trader': 6,
                'krait_light': 8,
                'krait_mkii': 31,
                'mamba': 3,
                'orca': 4,
                'python': 17,
                'type6': 7,
                'type7': 1,
                'type9': 24,
                'type9_military': 4,
                'typex': 5,
                'typex_2': 2,
                'typex_3': 2,
                'viper': 4,
                'viper_mkiv': 7,
                'vulture': 3},
            'total': 287
        }
    }
    assert expect == spy.load_response_json(response_json, eddb_session)


def test_process_scrape_data(empty_spy, scrape_json, eddb_session):
    spy.process_scrape_data(scrape_json)

    eddb_sys = eddb_session.query(cogdb.eddb.System).\
        filter(cogdb.eddb.System.name == 'Aowica').\
        one()
    sys = eddb_session.query(spy.SpySystem).\
        filter(spy.SpySystem.ed_system_id == eddb_sys.ed_system_id).\
        one()
    assert sys.system.name == 'Aowica'
    assert sys.fort == 4464
    assert sys.um_trigger == 11598


def test_compare_sheet_fort_systems_to_spy(empty_spy, db_cleanup, session, eddb_session):
    # Manually setup test case where spy > fort systems
    f_status = 4444
    um_status = 3333
    session.add(FortSystem(name='Sol', sheet_order=1, sheet_col='D', fort_status=0, um_status=0))
    session.commit()
    eddb_session.add(spy.SpySystem(id=1, power_id=9, ed_system_id=10477373803, power_state_id=16, fort=f_status, um=um_status))
    eddb_session.commit()

    result = spy.compare_sheet_fort_systems_to_spy(session, eddb_session)

    expect = [{'fort': 4444, 'sheet_col': 'D', 'sheet_order': 1, 'um': 3333}]
    assert expect == result
    system = session.query(FortSystem).filter(FortSystem.name == "Sol").one()
    assert f_status == system.fort_status
    assert um_status == system.um_status


def test_compare_sheet_um_systems_to_spy(empty_spy, db_cleanup, session, eddb_session):
    # Manually setup test case where spy > fort systems
    us = 4444
    them = 3333
    session.add_all([
        UMSystem(name='Sol', progress_us=0, progress_them=0, sheet_col='D'),
        UMSystem(name='Rana', progress_us=0, progress_them=0, sheet_col='F'),
    ])
    session.commit()
    eddb_session.add_all([
        spy.SpySystem(
            id=1, power_id=9, ed_system_id=10477373803, power_state_id=16,
            fort=them, fort_trigger=3333, um=us, um_trigger=5555
        ),
        spy.SpySystem(
            id=2, power_id=9, ed_system_id=83852530386, power_state_id=16,
            fort=0, fort_trigger=3333, um=0, um_trigger=5555
        ),
    ])
    eddb_session.commit()

    spy.compare_sheet_um_systems_to_spy(session, eddb_session)

    system = session.query(UMSystem).filter(UMSystem.name == "Sol").one()
    assert us == system.progress_us
    assert them / 3333 == system.progress_them
    system = session.query(UMSystem).filter(UMSystem.name == "Rana").one()
    assert 0 == system.progress_us
    assert 0 == system.progress_them


def test_update_eddb_factions(eddb_session):
    infs = []
    for system_name in INPUT_UPDATE_EDDB_FACTIONS.keys():
        infs += eddb_session.query(cogdb.eddb.Influence).\
            join(cogdb.eddb.System).\
            filter(cogdb.eddb.System.name == system_name).\
            all()

    try:
        with cogdb.session_scope(cogdb.EDDBSession) as isolated_session:
            cogdb.spy_squirrel.update_eddb_factions(isolated_session, INPUT_UPDATE_EDDB_FACTIONS)
            abi_tibis = isolated_session.query(cogdb.eddb.Influence).\
                join(cogdb.eddb.System).\
                join(cogdb.eddb.Faction, cogdb.eddb.Influence.faction_id == cogdb.eddb.Faction.id).\
                filter(cogdb.eddb.System.name == 'Abi',
                       cogdb.eddb.Faction.name == 'Tibis Alliance').\
                one()

            assert abi_tibis.influence == 49.7
            assert abi_tibis.updated_at == 1663943357.0
    finally:
        # Ensure EDDB not changed by test on fail
        eddb_session.rollback()
        for inf in infs:
            queried = eddb_session.query(cogdb.eddb.Influence).\
                filter(cogdb.eddb.Influence.id == inf.id).\
                one()
            queried.influence = inf.influence
            queried.updated_at = inf.updated_at
        eddb_session.commit()


def test_preload_spy_tables(empty_spy, eddb_session):
    assert not eddb_session.query(spy.SpyShip).all()
    spy.preload_spy_tables(eddb_session)
    assert eddb_session.query(spy.SpyShip).filter(spy.SpyShip.text == "Vulture").one()
