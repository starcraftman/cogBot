"""
Tests for cogdb.spy
"""
import json
import os
import pathlib
import pytest

import cog.util
import cogdb.spy_squirrel as spy


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


def test_base_loads(base_json):
    expect_control = spy.SpySystem(
        system_id='10477373803', power_id=9, power_state_id=16, is_expansion=False,
        income=122, upkeep_current=0, upkeep_default=21, fort_trigger=5211, um_trigger=33998
    )
    expect_taking = spy.SpySystem(
        system_id='11665533904241', power_id=9, power_state_id=64, is_expansion=True,
        income=0, upkeep_current=0, upkeep_default=0, fort_trigger=4872, um_trigger=7198
    )
    systems = spy.load_base_json(base_json)

    assert expect_control in systems
    assert expect_taking in systems


def test_refined_loads(refined_json):
    expect_prep = spy.SpyPrep(power_id=9, system_id=2557887812314, merits=14140)
    expect_vote = spy.SpyVote(power_id=11, vote=78)
    expect_expo = spy.SpySystem(power_id=6, system_id=2106438158699, fort=1247, um=53820, is_expansion=True)
    expect_sys = spy.SpySystem(power_id=11, system_id=22958210698120, fort=464, um=900, is_expansion=False)


    preps, votes, systems = spy.load_refined_json(refined_json)

    assert expect_prep in preps
    assert expect_vote in votes
    assert expect_expo in systems
    assert expect_sys in systems
