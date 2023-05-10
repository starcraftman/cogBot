"""
Test pvp.match
"""
import pytest

import pvp.match


@pytest.fixture
def f_pvpmatch():
    match = pvp.match.Match(mat_id=1, channel_id=5, limit=10)
    match.add_players(['cmdr1', 'cmdr2', 'cmdr3', 'cmdr4', 'cmdr5'])

    yield match


def test_match__repr__(f_pvpmatch):
    expect = "Match(id=1, channel_id=5, limit=10, players=['cmdr1', 'cmdr2', 'cmdr3', 'cmdr4', 'cmdr5'], teams={})"
    assert repr(f_pvpmatch) == expect


def test_match_num_players(f_pvpmatch):
    assert f_pvpmatch.num_players == 5


def test_match_add_players(f_pvpmatch):
    f_pvpmatch.remove_players(['cmdr1', 'cmdr4', 'cmdr5'])
    assert 'cmdr2' in f_pvpmatch.players
    assert 'cmdr1' not in f_pvpmatch.players
    assert len(f_pvpmatch.players) == 2


def test_match_balance_teams_odd(f_pvpmatch):
    f_pvpmatch.teams.update({
        1: ['cmdr1', 'cmdr3'],
        2: ['cmdr2', 'cmdr4', 'cmdr5'],
    })
    f_pvpmatch.balance_teams('cmdr7')
    assert f_pvpmatch.teams[1] == ['cmdr1', 'cmdr3', 'cmdr7']


def test_match_balance_teams_even(f_pvpmatch):
    f_pvpmatch.players = [f'cmdr{num}' for num in range(0, 7)]
    f_pvpmatch.teams.update({
        1: ['cmdr1', 'cmdr3', 'cmdr6'],
        2: ['cmdr2', 'cmdr4', 'cmdr5'],
    })
    f_pvpmatch.balance_teams('cmdr7')
    assert 'cmdr7' in f_pvpmatch.teams[1] or 'cmdr7' in f_pvpmatch.teams[2]


def test_match_sort_players(f_pvpmatch):
    f_pvpmatch.players = ['cmdr5', 'CMDR1', 'CmDr3']
    f_pvpmatch.sort_players()
    assert f_pvpmatch.players == ['CMDR1', 'CmDr3', 'cmdr5']


def test_match_remove_players(f_pvpmatch):
    f_pvpmatch.remove_players(['cmdr1', 'cmdr4', 'cmdr5'])
    assert 'cmdr2' in f_pvpmatch.players
    assert 'cmdr1' not in f_pvpmatch.players
    assert len(f_pvpmatch.players) == 2


def test_match_roll_teams(f_pvpmatch):
    assert not f_pvpmatch.teams
    f_pvpmatch.roll_teams()
    assert 'cmdr1' in f_pvpmatch.teams[1] or 'cmdr1' in f_pvpmatch.teams[2]


def test_match_embed_dict(f_pvpmatch):
    expect = {
        'author': {'icon_url': None, 'name': 'PvP Match'},
        'color': 906286,
        'fields': [
            {'inline': True, 'name': 'Registered', 'value': 'cmdr1\ncmdr2\ncmdr3\ncmdr4\ncmdr5'},
            {'inline': True, 'name': 'Team 1', 'value': 'cmdr1\ncmdr3\ncmdr4'},
            {'inline': True, 'name': 'Team 2', 'value': 'cmdr2\ncmdr5'},
        ],
        'provider': {'name': 'N/A'},
        'title': 'PVP Match ID: 1, 5/10'
    }
    f_pvpmatch.teams = {
        1: ['cmdr1', 'cmdr3', 'cmdr4'],
        2: ['cmdr2', 'cmdr5'],
    }
    assert expect == f_pvpmatch.embed_dict()


def test_match_create_match():
    try:
        mat = pvp.match.create_match(channel_id=5, limit=10)
        assert mat.channel_id == 5
        assert pvp.match.MATCH_NUM in pvp.match.MATCHES
    finally:
        pvp.match.MATCHES.clear()


def test_match_get_match_exists():
    try:
        pvp.match.create_match(channel_id=7, limit=10)
        mat = pvp.match.get_match(pvp.match.MATCH_NUM)
        assert mat.channel_id == 7
    finally:
        pvp.match.MATCHES.clear()


def test_match_get_match_none():
    assert not pvp.match.get_match(0)
