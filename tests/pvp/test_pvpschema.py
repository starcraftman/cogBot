# pylint: disable=redefined-outer-name,missing-function-docstring,unused-argument
"""
Tests for pvp.schema
"""
import tempfile
import datetime
import pytest

import pvp.schema
from pvp.schema import (
    PVPStat, PVPInterdictedKill, PVPInterdictedDeath, PVPInterdictionKill, PVPInterdictionDeath,
    PVPInterdicted, PVPInterdiction, PVPDeathKiller, PVPDeath, PVPKill, PVPCmdr
)


def test_pvpcmdr__str__(f_pvp_testbed, eddb_session):
    cmdr = eddb_session.query(PVPCmdr).filter(PVPCmdr.id == 1).one()
    assert "CMDR coolGuy (1)" == str(cmdr)


def test_pvpkill__str__(f_pvp_testbed, eddb_session):
    kill = eddb_session.query(PVPKill).filter(PVPKill.id == 1).one()
    assert "CMDR coolGuy killed CMDR LeSuck at 2022-12-21 20:42:57" == str(kill)


def test_pvpdeath__str__(f_pvp_testbed, eddb_session):
    death = eddb_session.query(PVPDeath).filter(PVPDeath.id == 1).one()
    assert "CMDR coolGuy was killed by: [CMDR BadGuyHelper (Vulture), CMDR BadGuyWon (Python)] at 2022-12-21 20:42:57" == str(death)


def test_pvpdeathkiller__str__(f_pvp_testbed, eddb_session):
    killer = eddb_session.query(PVPDeathKiller).filter(PVPDeathKiller.pvp_death_id == 1, PVPDeathKiller.name == 'BadGuyWon').one()
    assert "CMDR BadGuyWon (Python)" == str(killer)


def test_pvpinterdiction__str__(f_pvp_testbed, eddb_session):
    interdiction = eddb_session.query(PVPInterdiction).filter(PVPInterdiction.id == 1).one()
    assert "CMDR coolGuy interdicted CMDR LeSuck at 2022-12-21 20:42:57. Pulled from SC: True Escaped: False" == str(interdiction)


def test_pvpinterdicted__str__(f_pvp_testbed, eddb_session):
    interdicted = eddb_session.query(PVPInterdicted).filter(PVPInterdicted.id == 1).one()
    assert "CMDR coolGuy was interdicted by CMDR BadGuyWon at 2022-12-21 20:42:57. Submitted: False. Escaped: False" == str(interdicted)


def test_pvp_get_pvp_cmdr(f_pvp_testbed, eddb_session):
    assert pvp.schema.get_pvp_cmdr(eddb_session, cmdr_id=1)
    assert not pvp.schema.get_pvp_cmdr(eddb_session, cmdr_id=1000)
    assert pvp.schema.get_pvp_cmdr(eddb_session, cmdr_name='coolGuy')


def test_pvp_add_pvp_cmdr(f_pvp_testbed, eddb_session):
    assert pvp.schema.add_pvp_cmdr(eddb_session, 10, 'NewGuy', '666666')
    assert pvp.schema.get_pvp_cmdr(eddb_session, cmdr_id=10)


def test_pvp_get_pvp_stats(f_pvp_testbed, eddb_session):
    stats = pvp.schema.get_pvp_stats(eddb_session, 1)
    assert 3 == stats.kills
    assert not pvp.schema.get_pvp_stats(eddb_session, 10)


def test_pvp_update_pvp_stats(f_pvp_testbed, eddb_session):
    eddb_session.query(PVPStat).delete()
    eddb_session.commit()
    stats = pvp.schema.update_pvp_stats(eddb_session, 1)
    assert 3 == stats.kills


def test_pvp_get_pvp_events(f_pvp_testbed, eddb_session):
    events = pvp.schema.get_pvp_events(eddb_session, 1)
    assert events[-1].victim_name == 'LeSuck'


@pytest.mark.asyncio
async def test_pvp_add_pvp_log(f_pvp_testbed, eddb_session):
    with tempfile.NamedTemporaryFile(suffix='.log') as tfile:
        tfile.write(b"This is a sample log file.")
        tfile.flush()
        pvp_log = await pvp.schema.add_pvp_log(eddb_session, fname=tfile.name, cmdr_id=1)
        assert 1 == pvp_log.cmdr_id
        expect_hash = 'efacef55cc78da2ce5cac8f50104e28d616c3bde9c27b1cdfb4dd8aa'\
                     '6e5d6a46e4b6873b06c88b7b4c031400459a75366207dcb98e29623a170997da5aedb539'
        assert expect_hash == pvp_log.file_hash


def test_pvp_is_safe_to_drop():
    assert pvp.schema.is_safe_to_drop('pvp_cmdrs')
    assert not pvp.schema.is_safe_to_drop('spy_ships')
    assert not pvp.schema.is_safe_to_drop('stations')


class EventAtObj(pvp.schema.EventTimeMixin):
    """ Dummy object for mixin test. """
    def __init__(self):
        self.event_at = datetime.datetime(2021, 10, 21, 7, 0, tzinfo=datetime.timezone.utc).timestamp()


def test_event_at_notz():
    actual = EventAtObj()
    assert actual.event_date.tzname() is None
    assert "2021-10-21 07:00:00" == str(actual.event_date)


def test_event_at_tz():
    actual = EventAtObj()
    assert "UTC" == actual.event_date_tz.tzname()
    assert "2021-10-21 07:00:00+00:00" == str(actual.event_date_tz)
