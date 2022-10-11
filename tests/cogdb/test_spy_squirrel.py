"""
Tests for cogdb.spy
"""
import json
import os
import pathlib
import pytest

import sqlalchemy as sqla

import cog.util
import cogdb.spy_squirrel as spy
import cogdb.eddb
from cogdb.schema import FortSystem, UMSystem

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
    path = pathlib.Path(os.path.join(cog.util.ROOT_DIR, 'tests', 'cogdb', fname))
    if not path.exists():
        raise FileNotFoundError(f"Missing required json file: {str(path)}")

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
    ]
    eddb_session.add_all(objects)
    eddb_session.commit()

    yield objects
    spy.empty_tables()


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


def test_parse_params():
    input = [
        {
            "key": "system",
            "value": "52686561",
            "type": "string"
        },
        {
            "key": "factionName",
            "value": "4C61626F7572206F662052686561",
            "type": "string"
        },
        {
            "key": "list",
            "value": "$newsfeed_NewsSummaryHeadlines:#Influence=4.8:#Happiness=$Faction_HappinessBand2;;",
            "type": "string"
        }
    ]
    expect = {
        'factionName': 'Labour of Rhea',
        'list': '$newsfeed_NewsSummaryHeadlines:#Influence=4.8:#Happiness=$Faction_HappinessBand2;;',
        'system': 'Rhea'
    }

    assert expect == spy.parse_params(input)


def test_parse_response_news_summary():
    input = {
        "type": "NewsSummaryFactionStateTitle",
        "substColon": False,
        "params": [
            {
                "key": "system",
                "value": "52686561",
                "type": "string"
            },
            {
                "key": "factionName",
                "value": "4C61626F7572206F662052686561",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$newsfeed_NewsSummaryHeadlines:#Influence=4.8:#Happiness=$Faction_HappinessBand2;;",
                "type": "string"
            }
        ],
        "date": "7 OCT 3308",
        "sticky": 1
    }
    expect = {
        'factionName': 'Labour of Rhea',
        'happiness': 2,
        'influence': 4.8,
        'system': 'Rhea'
    }
    assert expect == spy.parse_response_news_summary(input)


def test_parse_response_trade_goods():
    input = {
        "date": "7 OCT 3308",
        "type": "stateTradeGoodCommodities",
        "params": [
            {
            "key": "list",
            "value": "2444616D61676564457363617065506F645F4E616D653B",
            "type": "string"
            },
            {
            "key": "list",
            "value": "2442617369634D65646963696E65735F4E616D653B",
            "type": "string"
            },
            {
            "key": "list",
            "value": "24416C6761655F4E616D653B",
            "type": "string"
            },
            {
            "key": "list",
            "value": "245061696E6974655F4E616D653B",
            "type": "string"
            },
            {
            "key": "list",
            "value": "24416C6578616E64726974655F4E616D653B",
            "type": "string"
            },
            {
            "key": "list",
            "value": "244F70616C5F4E616D653B",
            "type": "string"
            },
            {
            "key": "list",
            "value": "24476F6C645F4E616D653B",
            "type": "string"
            },
            {
            "key": "list",
            "value": "245472697469756D5F4E616D653B",
            "type": "string"
            },
            {
            "key": "list",
            "value": "2454686F7269756D5F4E616D653B",
            "type": "string"
            },
            {
            "key": "list",
            "value": "2457696E655F4E616D653B",
            "type": "string"
            },
            {
            "key": "list",
            "value": "244D6172696E65537570706C6965735F4E616D653B",
            "type": "string"
            }
        ]
    }
    expect = {
        'commodities': [
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
    }

    assert expect == spy.parse_response_trade_goods(input)


def test_parse_response_bounties_claimed():
    input = {
        "date": "7 OCT 3308",
        "type": "bountiesClaimed",
        "params": [
            {
                "key": "bountyCount",
                "value": 439,
                "type": "int"
            },
            {
                "key": "bountyValue",
                "value": 75817971,
                "type": "int"
            }
        ]
    }
    expect = {
        "bountyCount": 439,
        "bountyValue": 75817971,
    }
    assert expect == spy.parse_response_bounties_claimed(input)


def test_parse_response_top5_bounties():
    input = {
        "date": "7 OCT 3308",
        "type": "top5Bounties",
        "params": [
            {
                "key": "type",
                "value": "faction",
                "type": "string"
            },
            {
                "key": "commanderId1",
                "value": 5520548,
                "type": "int"
            },
            {
                "key": "name1",
                "value": "434D44522053756E4E616D69646120284B72616974204D6B20494920225052495641544520434F55524945522229",
                "type": "string"
            },
            {
                "key": "lastLocation1",
                "value": "4D656C696369202D20506F6C616E736B79204C616E64696E67",
                "type": "string"
            },
            {
                "key": "bountyValue1",
                "value": 22800,
                "type": "int"
            },
            {
                "key": "commanderId2",
                "value": 7686998,
                "type": "int"
            },
            {
                "key": "name2",
                "value": "434D445220427562626120426F20426F6220284B65656C6261636B20224B45454C4241434B2229",
                "type": "string"
            },
            {
                "key": "lastLocation2",
                "value": "446A6977616C202D2054686F6D70736F6E20446F636B",
                "type": "string"
            },
            {
                "key": "bountyValue2",
                "value": 6300,
                "type": "int"
            },
            {
                "key": "commanderId3",
                "value": 6750288,
                "type": "int"
            },
            {
                "key": "name3",
                "value": "434D4452204D72536B696C6C696E20284B65656C6261636B29",
                "type": "string"
            },
            {
                "key": "lastLocation3",
                "value": "4E4C5454203139383038",
                "type": "string"
            },
            {
                "key": "bountyValue3",
                "value": 2600,
                "type": "int"
            },
            {
                "key": "commanderId4",
                "value": 0,
                "type": "int"
            },
            {
                "key": "name4",
                "value": "",
                "type": "string"
            },
            {
                "key": "lastLocation4",
                "value": "",
                "type": "string"
            },
            {
                "key": "bountyValue4",
                "value": 0,
                "type": "int"
            },
            {
                "key": "commanderId5",
                "value": 0,
                "type": "int"
            },
            {
                "key": "name5",
                "value": "",
                "type": "string"
            },
            {
                "key": "lastLocation5",
                "value": "",
                "type": "string"
            },
            {
                "key": "bountyValue5",
                "value": 0,
                "type": "int"
            }
        ],
        "ugc": True
    }
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

    assert expect == spy.parse_response_top5_bounties(input)


def test_parse_response_traffic_totals():
    input = {
        "date": "7 OCT 3308",
        "type": "trafficTotals",
        "params": [
            {
                "key": "total",
                "value": 287,
                "type": "int"
            },
            {
                "key": "list",
                "value": "$ASP_NAME; - 18",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$ADDER_NAME; - 2",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$CUTTER_NAME; - 37",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$FEDERATION_CORVETTE_NAME; - 20",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$DIAMONDBACKXL_NAME; - 14",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$ANACONDA_NAME; - 35",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$TYPEX_NAME; - 5",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$DIAMONDBACK_NAME; - 8",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$DOLPHIN_NAME; - 4",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$ORCA_NAME; - 4",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$VIPER_MKIV_NAME; - 7",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$KRAIT_LIGHT_NAME; - 8",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$VULTURE_NAME; - 3",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$KRAIT_MKII_NAME; - 31",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$TYPE6_NAME; - 7",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$COBRAMKIII_NAME; - 9",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$TYPE9_NAME; - 24",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$INDEPENDANT_TRADER_NAME; - 6",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$PYTHON_NAME; - 17",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$FERDELANCE_NAME; - 2",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$BELUGALINER_NAME; - 4",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$EAGLE_NAME; - 1",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$HAULER_NAME; - 1",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$TYPEX_2_NAME; - 2",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$VIPER_NAME; - 4",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$TYPE9_MILITARY_NAME; - 4",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$FEDERATION_DROPSHIP_MKII_NAME; - 3",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$TYPEX_3_NAME; - 2",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$MAMBA_NAME; - 3",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$TYPE7_NAME; - 1",
                "type": "string"
            },
            {
                "key": "list",
                "value": "$EMPIRE_EAGLE_NAME; - 1",
                "type": "string"
            }
        ]
    }
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
    assert expect == spy.parse_response_traffic_totals(input)


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
        'power': {'fort': 0, 'power': 'Felicia Winters', 'um': 0},
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
                'type': 'super'}
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
