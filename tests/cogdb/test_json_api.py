"""
Tests for cogdb.json_api
"""
import json
import os
import pathlib
import pytest

import cog.util
import cogdb.json_api


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
    expect_control = {
        'id': '10477373803',
        'income': 122,
        'state': 'control',
        'tAgainst': 33998,
        'tFor': 5211,
        'upkeep': 0,
        'upkeep_default': 21
    }
    expect_taking = {
        'id': '11665533904241',
        'income': 0,
        'state': 'takingControl',
        'tAgainst': 7198,
        'tFor': 4872,
        'upkeep': 0,
        'upkeep_default': 0
    }
    powers = cogdb.json_api.load_base_json(base_json)

    assert expect_control in powers['Zachary Hudson']
    assert expect_taking in powers['Zachary Hudson']


def test_refined_loads(refined_json):
    expect = {'id': 2557887812314, 'total': 14140}

    preps = cogdb.json_api.load_refined_json(refined_json)

    assert expect in preps['Zachary Hudson']
