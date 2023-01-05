"""
Tests for pvp.journal
"""
import os
import json

import pytest

import cog.util
import pvp.schema
import pvp.journal
from pvp.schema import (
    PVPInterdictedKill, PVPInterdictedDeath, PVPInterdictionKill, PVPInterdictionDeath,
    PVPInterdicted, PVPInterdiction, PVPDeathKiller, PVPDeath, PVPKill, PVPLocation, PVPCmdr
)

JOURNAL_PATH = os.path.join(cog.util.ROOT_DIR, 'tests', 'pvp', 'player_journal.jsonl')


@pytest.fixture
def f_pvpcmdrs(eddb_session):
    eddb_session.add_all([
        PVPCmdr(id=1, name='coolGuy'),
        PVPCmdr(id=2, name='shyGuy'),
        PVPCmdr(id=3, name='shootsALot'),
    ])
    eddb_session.commit()


@pytest.fixture
def f_pvpclean():
    yield
    pvp.schema.empty_tables()



def test_datetime_to_tstamp():
    assert 1465583523 == pvp.journal.datetime_to_tstamp("2016-06-10T14:32:03Z")


def test_parse_died_simple(f_spy_ships, f_pvpclean, f_pvpcmdrs, eddb_session):
    data = json.loads('{ "timestamp":"2016-06-10T14:32:03Z", "event":"Died", "KillerName":"$ShipName_Police_Independent;", "KillerShip":"viper", "KillerRank":"Deadly" }')
    data['cmdr_id'] = 1
    data['event_at'] = pvp.journal.datetime_to_tstamp(data['timestamp'])
    pvp.journal.parse_died(eddb_session, data)
    eddb_session.commit()

    death = eddb_session.query(PVPDeath).filter(PVPDeath.cmdr_id == 1).one()
    assert 1 == death.cmdr_id
    assert len(death.killers) == 1
    assert death.killers[0].name.startswith("$ShipName_Police")


def test_parse_died_many(f_spy_ships, f_pvpclean, f_pvpcmdrs, eddb_session):
    data = json.loads('{ "timestamp":"2016-06-10T14:32:03Z", "event":"Died", "Killers":[ { "Name":"Cmdr HRC1", "Ship":"Vulture", "Rank":"Competent" }, { "Name":"Cmdr HRC2", "Ship":"Python", "Rank":"Master" } ] }')
    data['cmdr_id'] = 1
    data['event_at'] = pvp.journal.datetime_to_tstamp(data['timestamp'])
    pvp.journal.parse_died(eddb_session, data)
    eddb_session.commit()

    death = eddb_session.query(PVPDeath).filter(PVPDeath.cmdr_id == 1).one()
    assert 1 == death.cmdr_id
    assert len(death.killers) == 2
    assert death.killers[0].name == "HRC1"


def test_parse_pvpkill(f_pvpclean, f_pvpcmdrs, eddb_session):
    data = json.loads('{ "timestamp":"2016-06-10T14:32:03Z", "event":"PVPKill", "Victim":"BadGuy", "CombatRank":8 }')
    data['cmdr_id'] = 1
    data['event_at'] = pvp.journal.datetime_to_tstamp(data['timestamp'])
    pvp.journal.parse_pvpkill(eddb_session, data)
    eddb_session.commit()

    kill = eddb_session.query(PVPKill).filter(PVPKill.cmdr_id == 1).one()
    assert 1 == kill.cmdr_id
    assert kill.victim_name == "BadGuy"


def test_parse_interdiction(f_pvpclean, f_pvpcmdrs, eddb_session):
    data = json.loads('{ "timestamp":"2016-06-10T14:32:03Z", "event":"interdiction", "Success":true, "Interdicted":"Fred Flintstone", "IsPlayer":true, "CombatRank":5 }')
    data['cmdr_id'] = 1
    data['event_at'] = pvp.journal.datetime_to_tstamp(data['timestamp'])
    pvp.journal.parse_pvpinterdiction(eddb_session, data)
    eddb_session.commit()

    interdiction = eddb_session.query(PVPInterdiction).filter(PVPInterdiction.cmdr_id == 1).one()
    assert 1 == interdiction.cmdr_id
    assert "Fred Flintstone" == interdiction.victim_name
    assert 5 == interdiction.victim_rank
    assert interdiction.is_player


def test_parse_interdicted(f_pvpclean, f_pvpcmdrs, eddb_session):
    data = json.loads('{ "timestamp":"2016-06-10T14:32:03Z", "event":"interdicted", "Submitted":false, "Interdictor":"Dread Pirate Roberts", "IsPlayer":false, "Faction": "Timocani Purple Posse" }')
    data['cmdr_id'] = 1
    data['event_at'] = pvp.journal.datetime_to_tstamp(data['timestamp'])
    pvp.journal.parse_pvpinterdicted(eddb_session, data)
    eddb_session.commit()

    interdicted = eddb_session.query(PVPInterdicted).filter(PVPInterdicted.cmdr_id == 1).one()
    assert 1 == interdicted.cmdr_id
    assert "Dread Pirate Roberts" == interdicted.interdictor_name
    assert 0 == interdicted.interdictor_rank
    assert not interdicted.is_player


def test_parse_location(f_pvpclean, f_pvpcmdrs, eddb_session):
    data = json.loads('{ "timestamp":"2016-07-21T13:14:25Z", "event":"Location", "Docked":true, "StationName":"Azeban City", "StationType":"Coriolis", "StarSystem":"Eranin", "StarPos":[-22.844,36.531,-1.188], "Allegiance":"Alliance", "Economy":"$economy_Agri;", "Government":"$government_Communism;", "Security":"$SYSTEM_SECURITY_medium;", "Faction":"Eranin Peoples Party" }')
    data['cmdr_id'] = 1
    data['event_at'] = pvp.journal.datetime_to_tstamp(data['timestamp'])
    pvp.journal.parse_location(eddb_session, data)
    eddb_session.commit()

    location = eddb_session.query(PVPLocation).filter(PVPLocation.cmdr_id == 1).one()
    assert location.cmdr.name == "coolGuy"
    assert location.system.name == "Eranin"


def test_parse_fsdjump(f_pvpclean, f_pvpcmdrs, eddb_session):
    data = json.loads('{ "timestamp":"2016-07-21T13:16:49Z", "event":"FSDJump", "StarSystem":"LP 98-132", "StarPos":[-26.781,37.031,-4.594], "Economy":"$economy_Extraction;", "Allegiance":"Federation", "Government":"$government_Anarchy;", "Security":"$SYSTEM_SECURITY_high_anarchy;", "JumpDist":5.230, "FuelUsed":0.355614, "FuelLevel":12.079949, "Faction":"Brotherhood of LP 98-132", "FactionState":"Outbreak" }')
    data['cmdr_id'] = 1
    data['event_at'] = pvp.journal.datetime_to_tstamp(data['timestamp'])
    pvp.journal.parse_location(eddb_session, data)
    eddb_session.commit()

    location = eddb_session.query(PVPLocation).filter(PVPLocation.cmdr_id == 1).one()
    assert location.cmdr.name == "coolGuy"
    assert location.system.name == "LP 98-132"


def test_journal_parser_load(f_spy_ships, f_pvpclean, f_pvpcmdrs, eddb_session):
    parser = pvp.journal.Parser(fname=JOURNAL_PATH, cmdr_id=1, eddb_session=eddb_session)
    parser.load()
    assert 'FileHeader' in parser.lines[0]


def test_journal_parser_parse(f_spy_ships, f_pvpclean, f_pvpcmdrs, eddb_session):
    parser = pvp.journal.Parser(fname=JOURNAL_PATH, cmdr_id=1, eddb_session=eddb_session)
    parser.load()
    results = parser.parse()
    eddb_session.commit()

    assert isinstance(results[-1], PVPDeath)
    input()


def test_get_parser():
    event, parser = pvp.journal.get_parser({'event': 'Died'})
    assert event == 'Died'
    assert parser == pvp.journal.parse_died


def test_journal_rank_maps():
    assert pvp.journal.COMBAT_RANK_TO_VALUE['Elite'] == 8
    assert pvp.journal.VALUE_TO_COMBAT_RANK[8] == 'Elite'


def test_ship_name_map(f_spy_ships):
    ship_map = pvp.journal.ship_name_map()
    assert 'ferdelance' in ship_map
    assert 'fer-de-lance' in ship_map
    assert 'Vulture' in ship_map


def test_clean_cmdr_name():
    assert "NotReallyCool" == pvp.journal.clean_cmdr_name("cMdR   NotReallyCool ")
    assert "thisisareallylongnamethatshouldbecutshortforthedat" == pvp.journal.clean_cmdr_name("cmdr thisisareallylongnamethatshouldbecutshortforthedatabase")
