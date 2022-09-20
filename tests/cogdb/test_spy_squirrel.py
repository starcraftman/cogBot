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


def test_base_loads(empty_spy, base_json, eddb_session):
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


def test_refined_loads(empty_spy, base_json, refined_json, eddb_session):
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
def test_base_and_refined_loads(empty_spy, base_json, refined_json, eddb_session):
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
        UMSystem(name='Sol', progress_us=0, progress_them=0),
        UMSystem(name='Rana', progress_us=0, progress_them=0),
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
