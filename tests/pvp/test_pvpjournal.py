# pylint: disable=redefined-outer-name,missing-function-docstring,unused-argument
"""
Tests for pvp.journal
"""
import concurrent.futures as cfut
import os
import json
import pathlib
import shutil
import tempfile

import pytest

import cog.util
import pvp.schema
import pvp.journal
from pvp.schema import (
    PVPInterdicted, PVPInterdiction, PVPDeath, PVPKill, PVPLocation,
)
from tests.conftest import PVP_TIMESTAMP

JOURNAL_PATH = os.path.join(cog.util.ROOT_DIR, 'tests', 'pvp', 'player_journal.jsonl')


def test_datetime_to_tstamp():
    assert 1465569123.0 == pvp.journal.datetime_to_tstamp("2016-06-10T14:32:03Z")


def test_parse_died_simple_npc(f_spy_ships, f_pvp_testbed, eddb_session):
    data = json.loads('{ "timestamp":"2016-06-10T14:32:03Z", "event":"Died", "KillerName":"$ShipName_Police_Independent;", "KillerShip":"viper", "KillerRank":"Deadly" }')
    data['cmdr_id'] = 1
    data['system_id'] = 1005
    data['event_at'] = pvp.journal.datetime_to_tstamp(data['timestamp'])
    assert not pvp.journal.parse_died(eddb_session, data)


def test_parse_died_simple(f_spy_ships, f_pvp_testbed, eddb_session):
    data = json.loads('{ "timestamp":"2016-06-10T14:32:03Z", "event":"Died", "KillerName":"CMDR Ruin", "KillerShip":"viper", "KillerRank":"Deadly" }')
    data['cmdr_id'] = 1
    data['system_id'] = 1005
    data['event_at'] = pvp.journal.datetime_to_tstamp(data['timestamp'])
    pvp.journal.parse_died(eddb_session, data)
    eddb_session.commit()

    death = eddb_session.query(PVPDeath).filter(PVPDeath.system_id == 1005).one()
    assert 1 == death.cmdr_id
    assert len(death.killers) == 1
    assert death.killers[0].name.startswith("Ruin")


def test_parse_died_many(f_spy_ships, f_pvp_testbed, eddb_session):
    data = json.loads('{ "timestamp":"2016-06-10T14:32:03Z", "event":"Died", "Killers":[ { "Name":"Cmdr HRC1", "Ship":"Vulture", "Rank":"Competent" }, { "Name":"Cmdr HRC2", "Ship":"Python", "Rank":"Master" } ] }')
    data['cmdr_id'] = 1
    data['system_id'] = 1005
    data['event_at'] = pvp.journal.datetime_to_tstamp(data['timestamp'])
    pvp.journal.parse_died(eddb_session, data)
    eddb_session.commit()

    death = eddb_session.query(PVPDeath).filter(PVPDeath.system_id == 1005).one()
    assert 1 == death.cmdr_id
    assert len(death.killers) == 2
    assert death.killers[0].name == "HRC1"


def test_parse_pvpkill(f_pvp_testbed, eddb_session):
    data = json.loads('{ "timestamp":"2016-06-10T14:32:03Z", "event":"PVPKill", "Victim":"BadGuy", "CombatRank":8 }')
    data['cmdr_id'] = 1
    data['system_id'] = 1005
    data['event_at'] = pvp.journal.datetime_to_tstamp(data['timestamp'])
    pvp.journal.parse_kill(eddb_session, data)
    eddb_session.commit()

    kill = eddb_session.query(PVPKill).filter(PVPKill.system_id == 1005).one()
    assert 1 == kill.cmdr_id
    assert kill.victim_name == "BadGuy"


def test_parse_interdiction(f_pvp_testbed, eddb_session):
    data = json.loads('{ "timestamp":"2016-06-10T14:32:03Z", "event":"interdiction", "Success":true, "Interdicted":"Fred Flintstone", "IsPlayer":true, "CombatRank":5 }')
    data['cmdr_id'] = 1
    data['system_id'] = 1005
    data['event_at'] = pvp.journal.datetime_to_tstamp(data['timestamp'])
    pvp.journal.parse_interdiction(eddb_session, data)
    eddb_session.commit()

    interdiction = eddb_session.query(PVPInterdiction).filter(PVPInterdiction.system_id == 1005).one()
    assert 1 == interdiction.cmdr_id
    assert "Fred Flintstone" == interdiction.victim_name
    assert 5 == interdiction.victim_rank
    assert interdiction.is_player


def test_parse_interdicted(f_pvp_testbed, eddb_session):
    data = json.loads('{ "timestamp":"2016-06-10T14:32:03Z", "event":"interdicted", "Submitted":false, "Interdictor":"Dread Pirate Roberts", "IsPlayer":true, "CombatRank":7, "Faction": "Timocani Purple Posse" }')
    data['cmdr_id'] = 1
    data['system_id'] = 1005
    data['event_at'] = pvp.journal.datetime_to_tstamp(data['timestamp'])
    pvp.journal.parse_interdicted(eddb_session, data)
    eddb_session.commit()

    interdicted = eddb_session.query(PVPInterdicted).filter(PVPInterdicted.system_id == 1005).one()
    assert 1 == interdicted.cmdr_id
    assert "Dread Pirate Roberts" == interdicted.interdictor_name
    assert 7 == interdicted.interdictor_rank
    assert interdicted.is_player


def test_parse_location(f_pvp_testbed, eddb_session):
    data = json.loads('{ "timestamp":"2016-07-21T13:14:25Z", "event":"Location", "Docked":true, "StationName":"Azeban City", "StationType":"Coriolis", "StarSystem":"Eranin", "StarPos":[-22.844,36.531,-1.188], "Allegiance":"Alliance", "Economy":"$economy_Agri;", "Government":"$government_Communism;", "Security":"$SYSTEM_SECURITY_medium;", "Faction":"Eranin Peoples Party" }')
    data['cmdr_id'] = 1
    data['system_id'] = 1005
    data['event_at'] = pvp.journal.datetime_to_tstamp(data['timestamp'])
    pvp.journal.parse_location(eddb_session, data)
    eddb_session.commit()

    location = eddb_session.query(PVPLocation).filter(PVPLocation.system_id == 4611).one()
    assert location.cmdr.name == "coolGuy"
    assert location.system.name == "Eranin"


def test_parse_fsdjump(f_pvp_testbed, eddb_session):
    data = json.loads('{ "timestamp":"2016-07-21T13:16:49Z", "event":"FSDJump", "StarSystem":"LP 98-132", "StarPos":[-26.781,37.031,-4.594], "Economy":"$economy_Extraction;", "Allegiance":"Federation", "Government":"$government_Anarchy;", "Security":"$SYSTEM_SECURITY_high_anarchy;", "JumpDist":5.230, "FuelUsed":0.355614, "FuelLevel":12.079949, "Faction":"Brotherhood of LP 98-132", "FactionState":"Outbreak" }')
    data['cmdr_id'] = 1
    data['system_id'] = 1005
    data['event_at'] = pvp.journal.datetime_to_tstamp(data['timestamp'])
    pvp.journal.parse_location(eddb_session, data)
    eddb_session.commit()

    location = eddb_session.query(PVPLocation).filter(PVPLocation.system_id == 12533).one()
    assert location.cmdr.name == "coolGuy"
    assert location.system.name == "LP 98-132"


def test_link_interdiction_to_kill(f_pvp_testbed, eddb_session):
    kill = PVPKill(id=10, cmdr_id=1, system_id=1000, victim_name='LeSuck', victim_rank=3, event_at=PVP_TIMESTAMP + 10)
    interdiction = PVPInterdiction(id=10, cmdr_id=1, system_id=1000, is_player=True, is_success=True, survived=False,
                                   victim_name="LeSuck", victim_rank=3, event_at=PVP_TIMESTAMP + 10)
    eddb_session.add_all([kill, interdiction])
    eddb_session.flush()
    linked = pvp.journal.link_interdiction_to_kill(eddb_session, interdiction, kill)
    linked = pvp.journal.link_interdiction_to_kill(eddb_session, interdiction, kill)  # Will return found event
    assert linked.pvp_kill_id == kill.id
    assert linked.pvp_interdiction_id == interdiction.id


def test_link_interdiction_to_death(f_pvp_testbed, eddb_session):
    death = PVPDeath(id=10, cmdr_id=1, system_id=1000, is_wing_kill=True, event_at=PVP_TIMESTAMP + 10)
    interdiction = PVPInterdiction(id=10, cmdr_id=1, system_id=1000, is_player=True, is_success=True, survived=False,
                                   victim_name="LeSuck", victim_rank=3, event_at=PVP_TIMESTAMP + 10)
    eddb_session.add_all([death, interdiction])
    eddb_session.flush()
    linked = pvp.journal.link_interdiction_to_death(eddb_session, interdiction, death)
    linked = pvp.journal.link_interdiction_to_death(eddb_session, interdiction, death)  # Will return found event
    assert linked.pvp_death_id == death.id
    assert linked.pvp_interdiction_id == interdiction.id


def test_link_interdicted_to_kill(f_pvp_testbed, eddb_session):
    kill = PVPKill(id=10, cmdr_id=1, system_id=1000, victim_name='LeSuck', victim_rank=3, event_at=PVP_TIMESTAMP + 10)
    interdicted = PVPInterdicted(id=10, cmdr_id=1, system_id=1000, is_player=True, did_submit=False, survived=False,
                                 interdictor_name="BadGuyWon", interdictor_rank=7, event_at=PVP_TIMESTAMP + 10)
    eddb_session.add_all([kill, interdicted])
    eddb_session.flush()
    linked = pvp.journal.link_interdicted_to_kill(eddb_session, interdicted, kill)
    linked = pvp.journal.link_interdicted_to_kill(eddb_session, interdicted, kill)  # Will return found event
    assert linked.pvp_kill_id == kill.id
    assert linked.pvp_interdicted_id == interdicted.id


def test_link_interdicted_to_death(f_pvp_testbed, eddb_session):
    death = PVPDeath(id=10, cmdr_id=1, system_id=1000, is_wing_kill=True, event_at=PVP_TIMESTAMP + 10)
    interdicted = PVPInterdicted(id=10, cmdr_id=1, system_id=1000, is_player=True, did_submit=False, survived=False,
                                 interdictor_name="BadGuyWon", interdictor_rank=7, event_at=PVP_TIMESTAMP + 10)
    eddb_session.add_all([death, interdicted])
    eddb_session.flush()
    linked = pvp.journal.link_interdicted_to_death(eddb_session, interdicted, death)
    linked = pvp.journal.link_interdicted_to_death(eddb_session, interdicted, death)  # Will return found event
    assert linked.pvp_death_id == death.id
    assert linked.pvp_interdicted_id == interdicted.id


def test_parse_cmdr_name():
    data = json.loads('{ "timestamp":"2023-01-01T12:43:20Z", "event":"LoadGame", "FID":"F9999999", "Commander":"coolGuy", "Horizons":true, "Odyssey":false, "Ship":"Federation_Corvette", "Ship_Localised":"Federal Corvette", "ShipID":33, "ShipName":"ANS RELIANT", "ShipIdent":"FRC-03", "FuelLevel":32.000000, "FuelCapacity":32.000000, "GameMode":"Solo", "Credits":2969730326, "Loan":0, "language":"English/UK", "gameversion":"4.0.0.1476", "build":"r289925/r0 " }')
    assert 'coolGuy' == pvp.journal.parse_cmdr_name(data)

    data = json.loads('{ "timestamp":"2023-01-01T12:43:19Z", "event":"Commander", "FID":"F9999999", "Name":"coolGuy" }')
    assert 'coolGuy' == pvp.journal.parse_cmdr_name(data)

    data = json.loads('{ "timestamp":"2016-07-21T13:16:49Z", "event":"FSDJump", "StarSystem":"LP 98-132", "StarPos":[-26.781,37.031,-4.594], "Economy":"$economy_Extraction;", "Allegiance":"Federation", "Government":"$government_Anarchy;", "Security":"$SYSTEM_SECURITY_high_anarchy;", "JumpDist":5.230, "FuelUsed":0.355614, "FuelLevel":12.079949, "Faction":"Brotherhood of LP 98-132", "FactionState":"Outbreak" }')
    assert not pvp.journal.parse_cmdr_name(data)


@pytest.mark.asyncio
async def test_find_cmdr_name():
    cmdr_name = await pvp.journal.find_cmdr_name(JOURNAL_PATH)
    assert "HRC1" == cmdr_name


def test_journal_parser_load(f_spy_ships, f_pvp_testbed, eddb_session):
    parser = pvp.journal.Parser(fname=JOURNAL_PATH, cmdr_id=1, eddb_session=eddb_session)
    parser.load()
    assert 'FileHeader' in parser.lines[0]


def test_journal_parser_parse(f_spy_ships, f_pvp_testbed, eddb_session):
    parser = pvp.journal.Parser(fname=JOURNAL_PATH, cmdr_id=1, eddb_session=eddb_session)
    parser.load()
    results = parser.parse()
    eddb_session.commit()

    assert isinstance(results[-1], PVPDeath)


def test_get_event_parser():
    event, parser = pvp.journal.get_event_parser({'event': 'Died'})
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


def test_clean_died_killers():
    data = {
        'Killers': [
            {'Name': '$ShipName_Police_Independent'},
            {'Name': '$ShipName_Police_Independent'},
            {'Name': 'CMDR Ruin'},
        ]
    }
    data = pvp.journal.clean_died_killers(data)
    assert [x['Name'] for x in data['Killers']] == ['CMDR Ruin']

    data = {
        'KillerName': '$ShipName_Police_Indepenent', 'KillerShip': 'Python',

    }
    data = pvp.journal.clean_died_killers(data)
    assert not [x['Name'] for x in data['Killers']]

    data = {
        'KillerName': 'CMDR Ruin', 'KillerShip': 'Python',
    }
    data = pvp.journal.clean_died_killers(data)
    assert [x['Name'] for x in data['Killers']] == ['CMDR Ruin']


def test_filter_log():
    with tempfile.NamedTemporaryFile() as tfile:
        pvp.journal.filter_log(JOURNAL_PATH, tfile.name)
        with open(tfile.name, 'r', encoding='utf-8') as fin:
            for line in fin:
                assert '"event":"Scan"' not in line


def test_filter_archive(f_test_zip):
    tempd = pathlib.Path('/tmp/tmpfilter')
    expect = tempd / f_test_zip.name.replace('.zip', '.filter.zip')

    try:
        try:
            shutil.rmtree(tempd)
        except FileNotFoundError:
            pass
        tempd.mkdir()
        assert str(expect) == pvp.journal.filter_archive(f_test_zip, output_d=tempd, orig_fname=f_test_zip.name)
        assert expect.exists()
    finally:
        shutil.rmtree(tempd)


@pytest.mark.asyncio
async def test_filter_tempfile_zip(f_test_zip):
    tempd = pathlib.Path('/tmp/tmpfilter')
    expect = f_test_zip.parent / f_test_zip.name.replace('.zip', '.filter.zip')
    print(expect)

    with cfut.ProcessPoolExecutor(1) as pool:
        try:
            try:
                shutil.rmtree(tempd)
            except FileNotFoundError:
                pass
            tempd.mkdir()
            fut = await pvp.journal.filter_tempfile(pool=pool, dest_dir=tempd, tfile=f_test_zip, attach_fname='original.zip')
            await fut
            input()

            assert expect.exists()
        finally:
            shutil.rmtree(tempd)
