"""
Test util the grab all module.
"""
from __future__ import absolute_import, print_function
import os

import mock
import pytest

import cog.util


def test_modformatter_record():
    record = mock.Mock()
    record.__dict__['pathname'] = cog.util.rel_to_abs('cog', 'util.py')
    with pytest.raises(TypeError):
        cog.util.ModFormatter().format(record)
    assert record.__dict__['relmod'] == 'cog/util'


def test_dict_to_columns():
    data = {
        'first': [1, 2, 3],
        'more': [100],
        'second': [10, 30, 50],
        'three': [22, 19, 26, 23],
    }
    expect = [
        ['first (3)', 'more (1)', 'second (3)', 'three (4)'],
        [1, 100, 10, 22],
        [2, '', 30, 19],
        [3, '', 50, 26],
        ['', '', '', 23]
    ]
    assert cog.util.dict_to_columns(data) == expect


def test_get_config():
    assert cog.util.get_config('paths', 'log_conf') == 'data/log.yml'


def test_rel_to_abs():
    expect = os.path.join(cog.util.ROOT_DIR, 'data', 'log.yml')
    assert cog.util.rel_to_abs('data', 'log.yml') == expect


def test_substr_ind():
    assert cog.util.substr_ind('ale', 'alex') == [0, 3]
    assert cog.util.substr_ind('ALEX', 'Alexander') == [0, 4]
    assert cog.util.substr_ind('nde', 'Alexander') == [5, 8]

    assert not cog.util.substr_ind('ALe', 'Alexander', ignore_case=False)
    assert not cog.util.substr_ind('not', 'alex')
    assert not cog.util.substr_ind('longneedle', 'alex')

    assert cog.util.substr_ind('16 cyg', '16 c y  gni') == [0, 9]


def test_substr_match():
    assert cog.util.substr_match('ale', 'alex')
    assert cog.util.substr_match('ALEX', 'Alexander')
    assert cog.util.substr_match('nde', 'Alexander')

    assert not cog.util.substr_match('ALe', 'Alexander', ignore_case=False)
    assert not cog.util.substr_match('not', 'alex')
    assert not cog.util.substr_match('longneedle', 'alex')

    assert cog.util.substr_ind('16 cyg', '16 c y  gni') == [0, 9]


@pytest.mark.asyncio
async def test_get_coords():
    expect = [
        {
            'name': 'Adeo',
            'coords': {'x': -81.625, 'y': 31.40625, 'z': 27.1875},
        },
        {
            'name': 'Frey',
            'coords': {'x': -74.625, 'y': -21.625, 'z': 76.40625},
        },
        {
            'name': 'Sol',
            'coords': {'x': 0, 'y': 0, 'z': 0},
        },
    ]

    names = [sys['name'] for sys in expect]
    assert await cog.util.get_coords(names) == expect


@pytest.mark.asyncio
async def test_get_coords_fail():
    expect = [
        {
            'name': 'Adddddeo',
            'coords': {'x': -81.625, 'y': 31.40625, 'z': 27.1875},
        },
        {
            'name': 'Fre',
            'coords': {'x': -74.625, 'y': -21.625, 'z': 76.40625},
        },
        {
            'name': 'Solllll',
            'coords': {'x': 0, 'y': 0, 'z': 0},
        },
    ]

    names = [sys['name'] for sys in expect]
    assert await cog.util.get_coords(names) == []


@pytest.mark.asyncio
async def test_get_coords_partial_fail():
    expect = [
        {
            'name': 'Frey',
            'coords': {'x': -74.625, 'y': -21.625, 'z': 76.40625},
        },
    ]

    assert await cog.util.get_coords(['Adddddeo', 'Frey', 'Solllll']) == expect


def test_compute_dists():
    start = {'name': 'Sol', 'coords': {'x': 0, 'y': 0, 'z': 0}}
    expect = [
        {
            'name': 'Adeo',
            'coords': {'x': -81.625, 'y': 31.40625, 'z': 27.1875},
            'dist': 91.58686215998722
        },
        {
            'name': 'Frey',
            'coords': {'x': -74.625, 'y': -21.625, 'z': 76.40625},
            'dist': 108.96993295887862
        }
    ]

    import copy
    systems = copy.deepcopy(expect)
    del systems[0]['dist']
    del systems[1]['dist']
    cog.util.compute_dists(start, systems)

    assert systems == expect
