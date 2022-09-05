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
    path = pathlib.Path(os.path.join(cog.util.ROOT_DIR, 'tests', fname))
    if not path.exists():
        raise FileNotFoundError(f"Missing required json file: {str(path)}")

    with path.open('r', encoding='utf-8') as fin:
        return json.load(fin)


@pytest.fixture()
def base_json():
    return load_json('base.json')


@pytest.fixture()
def refined_json():
    return load_json('refined.json')


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
    expect_control = spy.SpySystem(
        ed_system_id=10477373803, power_id=9, power_state_id=16,
        income=122, upkeep_current=0, upkeep_default=21, fort_trigger=5211, um_trigger=33998
    )
    expect_taking = spy.SpySystem(
        ed_system_id=11665533904241, power_id=9, power_state_id=64,
        income=0, upkeep_current=0, upkeep_default=0, fort_trigger=4872, um_trigger=7198
    )
    systems = spy.load_base_json(base_json, eddb_session)

    assert expect_control in systems
    assert expect_taking in systems


def test_refined_loads(empty_spy, base_json, refined_json, eddb_session):
    expect_prep = spy.SpyPrep(power_id=9, ed_system_id=2557887812314, merits=14140)
    expect_vote = spy.SpyVote(power_id=11, vote=78)
    expect_expo = spy.SpySystem(power_id=6, ed_system_id=2106438158699, fort=1247, um=53820)
    expect_sys = spy.SpySystem(power_id=11, ed_system_id=22958210698120, fort=464, um=900)

    spy.load_base_json(base_json, eddb_session)
    db_objects = spy.load_refined_json(refined_json, eddb_session)

    assert expect_prep in db_objects
    assert expect_vote in db_objects
    assert expect_expo == eddb_session.query(spy.SpySystem).\
        filter(spy.SpySystem.ed_system_id == 2106438158699).\
        one()
    assert expect_sys == eddb_session.query(spy.SpySystem).\
        filter(spy.SpySystem.ed_system_id == 22958210698120).\
        one()
