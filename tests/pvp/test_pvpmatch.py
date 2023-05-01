"""
Test pvp.match
"""
import pytest

import pvp.match


@pytest.fixture
def f_pvpmatch():
    match = pvp.match.Match(5, limit=10)
    match.add_players(['cmdr1', 'cmdr2', 'cmdr3', 'cmdr4', 'cmdr5'])

    yield match


def test_match__repr__(f_pvpmatch):
    print(repr(f_pvpmatch))


def test_match_add_players(f_pvpmatch):
    f_pvpmatch.remove_players(['cmdr1', 'cmdr4', 'cmdr5'])
    assert 'cmdr2' in f_pvpmatch.players
    assert 'cmdr1' not in f_pvpmatch.players
    assert len(f_pvpmatch.players) == 2


def test_match_remove_players(f_pvpmatch):
    f_pvpmatch.remove_players(['cmdr1', 'cmdr4', 'cmdr5'])
    assert 'cmdr2' in f_pvpmatch.players
    assert 'cmdr1' not in f_pvpmatch.players
    assert len(f_pvpmatch.players) == 2


def test_match_roll_teams(f_pvpmatch):
    assert not f_pvpmatch.teams
    print(f_pvpmatch.roll_teams())
    assert 'cmdr1' in f_pvpmatch.teams[1] or 'cmdr1' in f_pvpmatch.teams[2]


def test_match_embed_dict(f_pvpmatch):
    expect = {
        'author': {'icon_url': None, 'name': 'PvP Match'},
        'color': 906286,
        'fields': [
            {'inline': True, 'name': 'Team 1', 'value': 'cmdr1\ncmdr3\ncmdr4'},
            {'inline': True, 'name': 'Team 2', 'value': 'cmdr2\ncmdr5'},
        ],
        'provider': {'name': 'N/A'},
        'title': 'PVP Match: 5/10'
    }
    f_pvpmatch.teams = {
        1: ['cmdr1', 'cmdr3', 'cmdr4'],
        2: ['cmdr2', 'cmdr5'],
    }
    assert expect == f_pvpmatch.embed_dict()
