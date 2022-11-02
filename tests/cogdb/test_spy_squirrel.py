"""
Tests for cogdb.spy
"""
import os
import pathlib

import pytest
import sqlalchemy as sqla

import cogdb.spy_squirrel as spy
from cogdb.schema import FortSystem, UMSystem
from cogdb.spy_squirrel import load_json_secret


FIXED_TIMESTAMP = 1662390092

# Empty tables before running tests.
spy.empty_tables()


@pytest.fixture()
def base_json():
    yield load_json_secret('base.json')


@pytest.fixture()
def refined_json():
    yield load_json_secret('refined.json')


@pytest.fixture()
def response_json():
    yield load_json_secret('response.json')


@pytest.fixture()
def scrape_json():
    yield load_json_secret('scrape.json')


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
            power_id=9,
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
            system='Rana',
            cmdr_name="Good guy",
            ship_name="Good guy ship",
            last_seen_system="Rana",
            last_seen_station="Wescott Hub",
            bounty=100000,
            category=1,
            ship_id=1,
            power_id=9,
            updated_at=FIXED_TIMESTAMP,
        ),
        spy.SpyBounty(
            id=2,
            pos=2,
            system='Rana',
            cmdr_name="Bad guy",
            ship_name="Bad guy ship",
            last_seen_system="Rana",
            last_seen_station="Ali Hub",
            bounty=10000000,
            category=1,
            ship_id=2,
            power_id=1,
            updated_at=FIXED_TIMESTAMP,
        ),
    ]
    eddb_session.add_all([
        spy.SpyShip(id=1, text="Viper Mk. II", traffic_text='viper_mkii'),
        spy.SpyShip(id=2, text="Vulture", traffic_text='vulture'),
    ])
    eddb_session.commit()
    eddb_session.add_all(objects)
    eddb_session.commit()

    yield objects
    spy.empty_tables()


def test_spy_ship__repr__(spy_test_bed, eddb_session):
    expect = "SpyShip(id=1, text='Viper Mk. II', traffic_text='viper_mkii')"
    ship = eddb_session.query(spy.SpyShip).filter(spy.SpyShip.id == 1).one()

    assert expect == repr(ship)


def test_spy_ship__str__(spy_test_bed, eddb_session):
    expect = "Ship: Viper Mk. II"
    ship = eddb_session.query(spy.SpyShip).filter(spy.SpyShip.id == 1).one()

    assert expect == str(ship)


def test_spy_traffic__repr__(spy_test_bed, eddb_session):
    expect = "SpyTraffic(id=1, system='Rana', ship_id=1, cnt=1, updated_at=1662390092)"
    traffic = eddb_session.query(spy.SpyTraffic).filter(spy.SpyTraffic.id == 1).one()

    assert expect == repr(traffic)


def test_spy_traffic__str__(spy_test_bed, eddb_session):
    expect = "Rana Viper Mk. II: 1"
    traffic = eddb_session.query(spy.SpyTraffic).filter(spy.SpyTraffic.id == 1).one()

    assert expect == str(traffic)


def test_spy_bounty__repr__(spy_test_bed, eddb_session):
    expect = "SpyBounty(id=1, category=1, system='Rana', pos=1, cmdr_name='Good guy', ship_name='Good guy ship', last_seen_system='Rana', last_seen_station='Wescott Hub', bounty=100000, ship_id=1, updated_at=1662390092)"
    bounty = eddb_session.query(spy.SpyBounty).filter(spy.SpyBounty.id == 1, spy.SpyBounty.pos == 1).one()

    assert expect == repr(bounty)


def test_spy_bounty__str__(spy_test_bed, eddb_session):
    expect = """#1 Good guy last seen in Rana/Wescott Hub (Viper Mk. II)
Has 100,000 in bounty, updated at 2022-09-05 15:01:32"""
    bounty = eddb_session.query(spy.SpyBounty).filter(spy.SpyBounty.id == 1, spy.SpyBounty.pos == 1).one()

    assert expect == str(bounty)


def test_spy_bounty_from_bounty_post_valid(spy_test_bed, eddb_session):
    post = {
        'commanderId': 3378194,
        'lastLocation': 'Tjial - Cassidy Landing',
        'name': 'CMDR Marduk298 (Federal Corvette "CLOACA MUNCH")',
        'pos': 1,
        'value': 859000,
        'category': 'power',
        'system': 'Rana',
        'updated_at': FIXED_TIMESTAMP,
    }

    expect = "SpyBounty(id=None, category=2, system='Rana', pos=1, cmdr_name='Marduk298', ship_name='CLOACA MUNCH', last_seen_system='Tjial', last_seen_station='Cassidy Landing', bounty=859000, ship_id=None, updated_at=1662390092)"
    bounty = spy.SpyBounty.from_bounty_post(post, power_id=11)
    assert expect == repr(bounty)


def test_spy_bounty_from_bounty_post_empty(spy_test_bed, eddb_session):
    post = {
        'commanderId': 0,
        'lastLocation': '',
        'name': '',
        'pos': 4,
        'value': 0,
        'category': 'power',
        'system': 'Rana',
        'updated_at': FIXED_TIMESTAMP,
    }
    expect = "SpyBounty(id=None, category=2, system='Rana', pos=4, cmdr_name='', ship_name='', last_seen_system='', last_seen_station='', bounty=0, ship_id=None, updated_at=1662390092)"
    bounty = spy.SpyBounty.from_bounty_post(post, power_id=11)
    assert expect == repr(bounty)


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


def test_fetch_json_secret():
    fpath = pathlib.Path(os.path.join('/tmp', 'base.json'))
    try:
        os.remove(fpath)
    except OSError:
        pass

    assert not fpath.exists()
    spy.fetch_json_secret('/tmp', 'base')
    assert fpath.exists()


def test_fetch_load_secret():
    base_json = spy.load_json_secret('base.json')

    assert base_json


def test_load_base_json(empty_spy, base_json, eddb_session):
    # Manually insert to test update paths
    eddb_session.add(
        spy.SpySystem(ed_system_id=10477373803, power_id=9, power_state_id=35),
    )
    eddb_session.commit()

    spy.load_base_json(base_json)

    expect_control = spy.SpySystem(
        ed_system_id=10477373803, power_id=9, power_state_id=16,
        income=122, upkeep_current=0, upkeep_default=21, fort_trigger=5211, um_trigger=33998
    )
    expect_taking = spy.SpySystem(
        ed_system_id=11665533904241, power_id=9, power_state_id=64,
        income=0, upkeep_current=0, upkeep_default=0, fort_trigger=4872, um_trigger=7198
    )

    systems = eddb_session.query(spy.SpySystem).\
        filter(spy.SpySystem.ed_system_id.in_([11665533904241, 10477373803])).\
        all()
    assert expect_control in systems
    assert expect_taking in systems


def test_load_refined_json(empty_spy, base_json, refined_json, eddb_session):
    # Manually insert to test update paths
    eddb_session.add_all([
        spy.SpyVote(power_id=1, vote=10),
        spy.SpyPrep(power_id=1, ed_system_id=79230372211, merits=0),
    ])
    eddb_session.commit()

    spy.load_refined_json(refined_json)

    expect_prep = spy.SpyPrep(power_id=9, ed_system_id=2557887812314, merits=14140)
    expect_vote = spy.SpyVote(power_id=11, vote=78)

    assert expect_prep == eddb_session.query(spy.SpyPrep).\
        filter(spy.SpyPrep.ed_system_id == 2557887812314).\
        one()
    assert expect_vote == eddb_session.query(spy.SpyVote).\
        filter(spy.SpyVote.power_id == 11).\
        one()


# Combined test of base then refined
def test_load_base_and_refined_json(empty_spy, base_json, refined_json, eddb_session):
    spy.load_base_json(base_json)
    spy.load_refined_json(refined_json)

    expect_prep = spy.SpyPrep(power_id=9, ed_system_id=2557887812314, merits=14140)
    expect_vote = spy.SpyVote(power_id=11, vote=78)
    expect_expo = spy.SpySystem(power_id=6, ed_system_id=2106438158699, fort=1247, um=53820)
    expect_sys = spy.SpySystem(power_id=11, ed_system_id=22958210698120, fort=464, um=900)

    assert expect_prep == eddb_session.query(spy.SpyPrep).\
        filter(spy.SpyPrep.ed_system_id == 2557887812314).\
        one()
    assert expect_vote == eddb_session.query(spy.SpyVote).\
        filter(spy.SpyVote.power_id == 11).\
        one()
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
        'name': 'Labour of Rhea',
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
            'pos': 1,
            'value': 22800,
            'commanderId': 5520548,
            'lastLocation': 'Melici - Polansky Landing',
            'name': 'CMDR SunNamida (Krait Mk II "PRIVATE COURIER")',
            'category': 'power',
        },
        2: {
            'pos': 2,
            'value': 6300,
            'commanderId': 7686998,
            'lastLocation': 'Djiwal - Thompson Dock',
            'name': 'CMDR Bubba Bo Bob (Keelback "KEELBACK")',
            'category': 'power',
        },
        3: {
            'pos': 3,
            'value': 2600,
            'commanderId': 6750288,
            'lastLocation': 'NLTT 19808',
            'name': 'CMDR MrSkillin (Keelback)',
            'category': 'power',
        },
        4: {
            'pos': 4,
            'value': 0,
            'commanderId': 0,
            'lastLocation': '',
            'name': '',
            'category': 'power',
        },
        5: {
            'pos': 5,
            'value': 0,
            'commanderId': 0,
            'lastLocation': '',
            'name': '',
            'category': 'power',
        },
    }
    assert expect == spy.parse_response_top5_bounties(response_news_json[10])


def test_parse_response_power_update(response_news_json):
    expect = {
        'power': 'Felicia Winters',
        'held_merits': 0,
        'stolen_forts': 0,
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
    spy.preload_spy_tables(eddb_session)
    spy.load_response_json(response_json)

    assert eddb_session.query(spy.SpyBounty).all()
    assert eddb_session.query(spy.SpyTraffic).all()


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


def test_get_spy_systems_for_galpow(empty_spy, db_cleanup, spy_test_bed, eddb_session):
    controls, preps, expansions, vote = spy.get_spy_systems_for_galpow(eddb_session, 9)

    assert controls
    assert preps
    assert expansions
    assert vote


def test_preload_spy_tables(empty_spy, eddb_session):
    assert not eddb_session.query(spy.SpyShip).all()
    spy.preload_spy_tables(eddb_session)
    assert eddb_session.query(spy.SpyShip).filter(spy.SpyShip.text == "Vulture").one()


def test_get_vote_of_power(empty_spy, eddb_session, spy_test_bed):
    assert 75 == spy.get_vote_of_power(eddb_session)
    assert 0 == spy.get_vote_of_power(eddb_session, power='winters')
