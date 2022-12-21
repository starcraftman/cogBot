"""
Tests for pvp.journal_parser
"""
import os
import json

import pytest

import cog.util
import pvp.schema
import pvp.journal_parser

JOURNAL_PATH = os.path.join(cog.util.ROOT_DIR, 'tests', 'pvp', 'player_journal.jsonl')


@pytest.fixture
def f_pvpclean():
    yield
    pvp.schema.empty_tables()



def test_datetime_to_tstamp():
    assert 1465583523 == pvp.journal_parser.datetime_to_tstamp("2016-06-10T14:32:03Z")


def test_parse_died_simple(eddb_session, f_pvpclean):
    data = json.loads('{ "timestamp":"2016-06-10T14:32:03Z", "event":"Died", "KillerName":"$ShipName_Police_Independent;", "KillerShip":"viper", "KillerRank":"Deadly" }')
    data['cmdr_id'] = 1
    data['event_at'] = pvp.journal_parser.datetime_to_tstamp(data['timestamp'])
    jsons = pvp.journal_parser.parse_died(eddb_session, data)
    print(jsons)


def test_parse_pvpkill(eddb_session, f_pvpclean):
    data = json.loads('{ "timestamp":"2016-06-10T14:32:03Z", "event":"PVPKill", "Victim":"BadGuy", "CombatRank":8 }')
    data['cmdr_id'] = 1
    data['event_at'] = pvp.journal_parser.datetime_to_tstamp(data['timestamp'])
    jsons = pvp.journal_parser.parse_pvpkill(eddb_session, data)
    print(jsons)


def test_load_journal_possible(f_pvpclean):
    jsons = pvp.journal_parser.load_journal_possible(JOURNAL_PATH)
    assert jsons[-1]['event'] == 'PVPKill'
