"""
Tests for pvp.schema
"""
import datetime
import pytest

import pvp.schema
from pvp.schema import (
    PVPInterdictedKill, PVPInterdictedDeath, PVPInterdictionKill, PVPInterdictionDeath,
    PVPInterdicted, PVPInterdiction, PVPDeathKiller, PVPDeath, PVPKill, PVPCmdr
)

TIMESTAMP = 1671655377


@pytest.fixture
def f_pvp_testbed(f_spy_ships, eddb_session):
    """
    Massive fixture intializes an entire dummy testbed of pvp objects.
    """
    eddb_session.add_all([
        PVPCmdr(id=1, name='coolGuy', updated_at=TIMESTAMP),
        PVPCmdr(id=2, name='shyGuy', updated_at=TIMESTAMP),
        PVPCmdr(id=3, name='shootsALot', updated_at=TIMESTAMP),
    ])
    eddb_session.flush()
    eddb_session.add_all([
        PVPKill(id=1, cmdr_id=1, victim_name='LeSuck', victim_rank=3, event_at=TIMESTAMP),
        PVPKill(id=2, cmdr_id=1, victim_name='BadGuy', victim_rank=7, event_at=TIMESTAMP),
        PVPKill(id=3, cmdr_id=1, victim_name='LeSuck', victim_rank=3, event_at=TIMESTAMP),
        PVPKill(id=4, cmdr_id=2, victim_name='CanNotShoot', victim_rank=8, event_at=TIMESTAMP),

        PVPDeath(id=1, cmdr_id=1, is_wing_kill=True, event_at=TIMESTAMP),
        PVPDeath(id=2, cmdr_id=1, is_wing_kill=False, event_at=TIMESTAMP),
        PVPDeath(id=3, cmdr_id=3, is_wing_kill=False, event_at=TIMESTAMP),
        PVPDeathKiller(cmdr_id=1, pvp_death_id=1, name='BadGuyWon', rank=7, ship_id=30, event_at=TIMESTAMP),
        PVPDeathKiller(cmdr_id=1, pvp_death_id=1, name='BadGuyHelper', rank=5, ship_id=38, event_at=TIMESTAMP),
        PVPDeathKiller(cmdr_id=2, pvp_death_id=2, name='BadGuyWon', rank=7, ship_id=30, event_at=TIMESTAMP),
        PVPDeathKiller(cmdr_id=3, pvp_death_id=3, name='BadGuyWon', rank=7, ship_id=30, event_at=TIMESTAMP),

        PVPInterdiction(id=1, cmdr_id=1, is_player=True, is_success=True, did_escape=False,
                        victim_name="LeSuck", victim_rank=3, event_at=TIMESTAMP),
        PVPInterdiction(id=2, cmdr_id=1, is_player=True, is_success=True, did_escape=True,
                        victim_name="LeSuck", victim_rank=3, event_at=TIMESTAMP),

        PVPInterdicted(id=1, cmdr_id=1, is_player=True, did_submit=False, did_escape=False,
                       interdictor_name="BadGuyWon", interdictor_rank=7, event_at=TIMESTAMP),
        PVPInterdicted(id=2, cmdr_id=2, is_player=True, did_submit=True, did_escape=True,
                       interdictor_name="BadGuyWon", interdictor_rank=7, event_at=TIMESTAMP),

    ])
    eddb_session.flush()
    eddb_session.add_all([
        PVPInterdictionKill(cmdr_id=1, pvp_interdiction_id=1, pvp_kill_id=1),
        PVPInterdictionDeath(cmdr_id=2, pvp_interdiction_id=2, pvp_death_id=2),
        PVPInterdictedKill(cmdr_id=3, pvp_interdicted_id=2, pvp_kill_id=3),
        PVPInterdictedDeath(cmdr_id=1, pvp_interdicted_id=1, pvp_death_id=1),
    ])
    eddb_session.commit()

    yield
    pvp.schema.empty_tables()


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
    assert "CMDR coolGuy interdicted CMDR LeSuck. Pulled from SC: True Escaped: False" == str(interdiction)


def test_pvpinterdicted__str__(f_pvp_testbed, eddb_session):
    interdicted = eddb_session.query(PVPInterdicted).filter(PVPInterdicted.id == 1).one()
    assert "CMDR coolGuy was interdicted by CMDR BadGuyWon. Submitted: False. Escaped: False" == str(interdicted)


def test_pvp_is_safe_to_drop():
    assert pvp.schema.is_safe_to_drop('pvp_cmdrs')
    assert not pvp.schema.is_safe_to_drop('spy_ships')
    assert not pvp.schema.is_safe_to_drop('stations')


class EventAtObj(pvp.schema.EventTimeMixin):
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
