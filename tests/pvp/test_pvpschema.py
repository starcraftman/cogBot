# pylint: disable=redefined-outer-name,missing-function-docstring,unused-argument
"""
Tests for pvp.schema
"""
import tempfile
import datetime
import pytest

import cog.exc
import pvp.schema
from pvp.schema import (
    PVPMatchState, PVPMatch, PVPMatchPlayer, PVPLog, PVPStat,
    PVPInterdicted, PVPInterdiction, PVPDeathKiller, PVPDeath, PVPKill, PVPLocation, PVPCmdr
)


def test_pvpcmdr__str__(f_pvp_testbed, eddb_session):
    cmdr = eddb_session.query(PVPCmdr).filter(PVPCmdr.id == 1).one()
    assert "CMDR coolGuy (1)" == str(cmdr)


def test_pvpcmdr__repr__(f_pvp_testbed, eddb_session):
    cmdr = eddb_session.query(PVPCmdr).filter(PVPCmdr.id == 1).one()
    assert "PVPCmdr(id=1, name='coolGuy', updated_at=1671655377)" == repr(cmdr)


def test_pvplocation__str__(f_pvp_testbed, eddb_session):
    location = eddb_session.query(PVPLocation).filter(PVPLocation.id == 1).one()
    assert 'CMDR coolGuy located in Anja at 2022-12-21 20:42:57.' == str(location)


def test_pvplocation__repr__(f_pvp_testbed, eddb_session):
    location = eddb_session.query(PVPLocation).filter(PVPLocation.id == 1).one()
    assert 'PVPLocation(id=1, cmdr_id=1, system_id=1000, created_at=1671655377, event_at=1671655377)' == repr(location)


def test_pvplocation_embed(f_pvp_testbed, eddb_session):
    location = eddb_session.query(PVPLocation).filter(PVPLocation.id == 1).one()
    assert 'Anja (2022-12-21 20:42:57)' == location.embed()


def test_pvpkill__str__(f_pvp_testbed, eddb_session):
    kill = eddb_session.query(PVPKill).filter(PVPKill.id == 1).one()
    assert "CMDR coolGuy killed CMDR LeSuck at 2022-12-21 20:42:57" == str(kill)


def test_pvpkill__repr__(f_pvp_testbed, eddb_session):
    kill = eddb_session.query(PVPKill).filter(PVPKill.id == 1).one()
    assert "PVPKill(id=1, cmdr_id=1, system_id=1000, victim_name='LeSuck', victim_rank=3, created_at=1671655377, event_at=1671655377)" == repr(kill)


def test_pvpkill_embed(f_pvp_testbed, eddb_session):
    kill = eddb_session.query(PVPKill).filter(PVPKill.id == 1).one()
    assert "CMDR LeSuck (2022-12-21 20:42:57)" == kill.embed()


def test_pvpdeath__str__(f_pvp_testbed, eddb_session):
    death = eddb_session.query(PVPDeath).filter(PVPDeath.id == 1).one()
    assert "CMDR coolGuy was killed by: [CMDR BadGuyHelper (Vulture), CMDR BadGuyWon (Python)] at 2022-12-21 20:42:57" == str(death)


def test_pvpdeath__repr__(f_pvp_testbed, eddb_session):
    death = eddb_session.query(PVPDeath).filter(PVPDeath.id == 1).one()
    assert "PVPDeath(id=1, cmdr_id=1, system_id=1000, is_wing_kill=True, created_at=1671655377, event_at=1671655377)" == repr(death)


def test_pvpdeath_embed(f_pvp_testbed, eddb_session):
    death = eddb_session.query(PVPDeath).filter(PVPDeath.id == 1).one()
    assert "[CMDR BadGuyHelper (Vulture), CMDR BadGuyWon (Python)] (2022-12-21 20:42:57)" == death.embed()


def test_pvpdeathkiller__str__(f_pvp_testbed, eddb_session):
    killer = eddb_session.query(PVPDeathKiller).filter(PVPDeathKiller.pvp_death_id == 1, PVPDeathKiller.name == 'BadGuyWon').one()
    assert "CMDR BadGuyWon (Python)" == str(killer)


def test_pvpdeathkiller__repr__(f_pvp_testbed, eddb_session):
    killer = eddb_session.query(PVPDeathKiller).filter(PVPDeathKiller.pvp_death_id == 1, PVPDeathKiller.name == 'BadGuyWon').one()
    assert "PVPDeathKiller(pvp_death_id=1, name='BadGuyWon', rank=7, ship_id=30, created_at=1671655377, event_at=1671655377)" == repr(killer)


def test_pvpinterdiction__str__(f_pvp_testbed, eddb_session):
    interdiction = eddb_session.query(PVPInterdiction).filter(PVPInterdiction.id == 1).one()
    assert "CMDR coolGuy interdicted CMDR LeSuck at 2022-12-21 20:42:57. Pulled from SC: True Escaped: False" == str(interdiction)


def test_pvpinterdiction__repr__(f_pvp_testbed, eddb_session):
    interdiction = eddb_session.query(PVPInterdiction).filter(PVPInterdiction.id == 1).one()
    assert "PVPInterdiction(id=1, cmdr_id=1, system_id=1000, is_player=True, is_success=True, survived=False, victim_name='LeSuck', victim_rank=3, created_at=1671655377, event_at=1671655377)" == repr(interdiction)


def test_pvpinterdiction_embed(f_pvp_testbed, eddb_session):
    interdiction = eddb_session.query(PVPInterdiction).filter(PVPInterdiction.id == 1).one()
    assert "CMDR LeSuck (2022-12-21 20:42:57)" == interdiction.embed()


def test_pvpinterdicted__str__(f_pvp_testbed, eddb_session):
    interdicted = eddb_session.query(PVPInterdicted).filter(PVPInterdicted.id == 1).one()
    assert "CMDR coolGuy was interdicted by CMDR BadGuyWon at 2022-12-21 20:42:57. Submitted: False. Escaped: False" == str(interdicted)


def test_pvpinterdicted__repr__(f_pvp_testbed, eddb_session):
    interdicted = eddb_session.query(PVPInterdicted).filter(PVPInterdicted.id == 1).one()
    assert "PVPInterdicted(id=1, cmdr_id=1, system_id=1000, is_player=True, did_submit=False, survived=False, interdictor_name='BadGuyWon', interdictor_rank=7, created_at=1671655377, event_at=1671655377)" == repr(interdicted)


def test_pvpinterdicted_embed(f_pvp_testbed, eddb_session):
    interdicted = eddb_session.query(PVPInterdicted).filter(PVPInterdicted.id == 1).one()
    assert "CMDR BadGuyWon (2022-12-21 20:42:57)" == interdicted.embed()


def test_pvpstat__str__(f_pvp_testbed, eddb_session):
    expect = """      Statistic       |                     Value
--------------------- | ---------------------------------------------
Kills                 | 3
Deaths                | 2
K/D                   | 1.5
Interdictions         | 2
Interdiction -> Kill  | 1
Interdiction -> Death | 0
Interdicteds          | 1
Interdicted -> Kill   | 0
Interdicted -> Death  | 1
Most Kills            | CMDR LeSuck
Most Deaths By        | CMDR BadGuyWon
Most Interdictions    | CMDR LeSuck
Most Interdicted By   | CMDR BadGuyWon
Most Kills In         | Anja
Most Deaths In        | Anjana
Last Location         | Anja (2022-12-21 20:42:57)
Last Kill             | CMDR LeSuck (2022-12-21 20:43:01)
Last Death By         | CMDR BadGuyWon (Python) (2022-12-21 20:42:59)
Last Interdiction     | CMDR LeSuck (2022-12-21 20:42:59)
Last Interdicted By   | CMDR BadGuyWon (2022-12-21 20:42:57)"""
    stat = eddb_session.query(PVPStat).filter(PVPStat.cmdr_id == 1).one()
    assert expect == str(stat)


def test_pvpstat_kill_ratio(f_pvp_testbed, eddb_session):
    stat = eddb_session.query(PVPStat).filter(PVPStat.cmdr_id == 1).one()
    assert 1.5 == stat.kill_ratio

    stat.deaths = 0
    assert 0.0 == stat.kill_ratio


def test_pvpstat_embed_values(f_pvp_testbed, eddb_session):
    stat = eddb_session.query(PVPStat).filter(PVPStat.cmdr_id == 1).one()
    expect = [
        {'inline': True, 'name': 'Kills', 'value': '3'},
        {'inline': True, 'name': 'Deaths', 'value': '2'},
        {'inline': True, 'name': 'K/D', 'value': '1.5'},
        {'inline': True, 'name': 'Interdictions', 'value': '2'},
        {'inline': True, 'name': 'Interdiction -> Kill', 'value': '1'},
        {'inline': True, 'name': 'Interdiction -> Death', 'value': '0'},
        {'inline': True, 'name': 'Interdicteds', 'value': '1'},
        {'inline': True, 'name': 'Interdicted -> Kill', 'value': '0'},
        {'inline': True, 'name': 'Interdicted -> Death', 'value': '1'},
        {'inline': True, 'name': 'Most Kills', 'value': 'CMDR LeSuck'},
        {'inline': True, 'name': 'Most Deaths By', 'value': 'CMDR BadGuyWon'},
        {'inline': True, 'name': 'Most Interdictions', 'value': 'CMDR LeSuck'},
        {'inline': True, 'name': 'Most Interdicted By', 'value': 'CMDR BadGuyWon'},
        {'inline': True, 'name': 'Most Kills In', 'value': 'Anja'},
        {'inline': True, 'name': 'Most Deaths In', 'value': 'Anjana'},
        {'inline': True, 'name': 'Last Location', 'value': 'Anja (2022-12-21 20:42:57)'},
        {'inline': True, 'name': 'Last Kill', 'value': 'CMDR LeSuck (2022-12-21 20:43:01)'},
        {'inline': True, 'name': 'Last Death By', 'value': 'CMDR BadGuyWon (Python) (2022-12-21 20:42:59)'},
        {'inline': True, 'name': 'Last Interdiction', 'value': 'CMDR LeSuck (2022-12-21 20:42:59)'},
        {'inline': True, 'name': 'Last Interdicted By', 'value': 'CMDR BadGuyWon (2022-12-21 20:42:57)'}
    ]

    assert expect == stat.embed_values


def test_pvplog__repr__(f_pvp_testbed, eddb_session):
    log = eddb_session.query(PVPLog).filter(PVPLog.id == 1).one()
    assert "PVPLog(id=1, cmdr_id=1, func_used=0, file_hash='hash', filename='first.log', msg_id=1, filtered_msg_id=10, updated_at=1671655377)" == repr(log)


def test_pvpmatch__repr__(f_pvp_testbed, eddb_session):
    match = eddb_session.query(PVPMatch).filter(PVPMatch.id == 1).one()
    assert "PVPMatch(id=1, limit=10, state=0, created_at=1671655377, updated_at=1671655377)" == repr(match)


def test_pvpmatch_validate_state(f_pvp_testbed, eddb_session):
    match = eddb_session.query(PVPMatch).filter(PVPMatch.id == 1).one()
    match.state = 1
    assert 1 == match.state
    match.state = PVPMatchState.FINISHED
    assert 2 == match.state

    with pytest.raises(cog.exc.ValidationFail):
        match.state = 'aaa'


def test_pvpmatch_players(f_pvp_testbed, eddb_session):
    match = eddb_session.query(PVPMatch).filter(PVPMatch.id == 1).one()
    assert match.players
    assert len(match.players) == 3


def test_pvpmatch_clone(f_pvp_testbed, eddb_session):
    match = eddb_session.query(PVPMatch).filter(PVPMatch.id == 1).one()
    match.roll_teams()
    new_match = match.clone(eddb_session)

    assert new_match.id
    assert match.id != new_match.id
    assert match.state == new_match.state
    for player, new_player in zip(match.players, new_match.players):
        assert new_player.id
        assert player.id != new_player.id
        assert player.team == new_player.team


def test_pvpmatch_roll_teams(f_pvp_testbed, eddb_session):
    match = eddb_session.query(PVPMatch).filter(PVPMatch.id == 1).one()
    assert {0} == {x.team for x in match.players}
    match.roll_teams()
    assert {1, 2} == {x.team for x in match.players}


def test_pvpmatch_finish(f_pvp_testbed, eddb_session):
    match = eddb_session.query(PVPMatch).filter(PVPMatch.id == 1).one()
    for player in match.players:
        assert not player.won
        assert player.team == 0

    match.roll_teams()
    match.finish(winning_team=1)
    teams = match.teams_dict()

    for player in teams[1]:
        assert player.won
    for player in teams[2]:
        assert not player.won


def test_pvpmatch_winners(f_pvp_testbed, eddb_session):
    match = eddb_session.query(PVPMatch).filter(PVPMatch.id == 1).one()
    for player in match.players[1:]:
        player.won = True

    assert match.players[1:] == match.winners


def test_pvpmatch_get_player(f_pvp_testbed, eddb_session):
    match = eddb_session.query(PVPMatch).filter(PVPMatch.id == 1).one()

    assert match.players[0] == match.get_player(cmdr_id=1)


def test_pvpmatch_add_player(f_pvp_testbed, eddb_session):
    match = eddb_session.query(PVPMatch).filter(PVPMatch.id == 1).one()
    assert not match.add_player(eddb_session, cmdr_id=1)
    assert match.add_player(eddb_session, cmdr_id=4)
    assert 4 == len(match.players)


def test_pvpmatch_teams_dict(f_pvp_testbed, eddb_session):
    match = eddb_session.query(PVPMatch).filter(PVPMatch.id == 1).one()
    teams = match.teams_dict()
    assert list(teams.keys()) == [0]
    expect = list(sorted(match.players))
    assert expect == list(sorted(teams[0]))


def test_pvpmatch_embed_dict(f_pvp_testbed, eddb_session):
    match = eddb_session.query(PVPMatch).filter(PVPMatch.id == 1).one()
    expect = {
        'author': {'icon_url': None, 'name': 'PvP Match'},
        'color': 906286,
        'fields': [
            {'inline': True, 'name': 'State', 'value': 'Setup'},
            {'inline': True, 'name': 'Team 0', 'value': 'coolGuy\nshootsALot\nshyGuy'}
        ],
        'provider': {'name': 'N/A'},
        'title': 'PVP Match: 3/10'
    }
    assert expect == match.embed_dict()


def test_pvpmatch_cascade_delete(f_pvp_testbed, eddb_session):
    match = eddb_session.query(PVPMatch).filter(PVPMatch.id == 1).one()
    eddb_session.delete(match)
    eddb_session.commit()

    assert len(eddb_session.query(PVPMatch).all()) == 2
    assert len(eddb_session.query(PVPMatchPlayer).all()) == 1


def test_pvpmatchplayer__repr__(f_pvp_testbed, eddb_session):
    player = eddb_session.query(PVPMatchPlayer).filter(PVPMatchPlayer.id == 1).one()
    assert "PVPMatchPlayer(id=1, cmdr_id=1, match_id=1, team=0, updated_at=1671655377)" == repr(player)


def test_pvpmatchplayer__lt__(f_pvp_testbed, eddb_session):
    player = eddb_session.query(PVPMatchPlayer).filter(PVPMatchPlayer.id == 1).one()
    player2 = eddb_session.query(PVPMatchPlayer).filter(PVPMatchPlayer.id == 2).one()
    assert player < player2
    assert not player2 < player


def test_pvp_get_pvp_cmdr(f_pvp_testbed, eddb_session):
    assert pvp.schema.get_pvp_cmdr(eddb_session, cmdr_id=1)
    assert not pvp.schema.get_pvp_cmdr(eddb_session, cmdr_id=1000)
    assert pvp.schema.get_pvp_cmdr(eddb_session, cmdr_name='coolGuy')


def test_pvp_add_pvp_cmdr(f_pvp_testbed, eddb_session):
    assert pvp.schema.add_pvp_cmdr(eddb_session, 10, 'NewGuy', '666666')
    assert pvp.schema.get_pvp_cmdr(eddb_session, cmdr_id=10)


def test_pvp_get_pvp_stats(f_pvp_testbed, eddb_session):
    stats = pvp.schema.get_pvp_stats(eddb_session, cmdr_id=1)
    assert 3 == stats.kills
    assert not pvp.schema.get_pvp_stats(eddb_session, 10)


def test_pvp_get_event_cmdrs(f_pvp_testbed, eddb_session):
    found = pvp.schema.get_pvp_event_cmdrs(eddb_session, cmdr_id=1)
    expect = {
        'killed_most': 'LeSuck',
        'most_deaths_by': 'BadGuyWon',
        'most_interdicted_by': 'BadGuyWon',
        'most_interdictions': 'LeSuck'
    }
    assert expect == found


def test_pvp_get_last_events(f_pvp_testbed, eddb_session):
    found = pvp.schema.get_pvp_last_events(eddb_session, cmdr_id=1)
    expect = {
        'last_death_id': 2,
        'last_interdicted_id': 1,
        'last_interdiction_id': 2,
        'last_kill_id': 3,
        'last_location_id': 1,
    }
    assert expect == found


def test_pvp_update_pvp_stats(f_pvp_testbed, eddb_session):
    eddb_session.query(PVPStat).delete()
    eddb_session.commit()
    stats = pvp.schema.update_pvp_stats(eddb_session, cmdr_id=1)
    assert 3 == stats.kills


def test_pvp_get_pvp_events(f_pvp_testbed, eddb_session):
    events = pvp.schema.get_pvp_events(eddb_session, cmdr_id=1)
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


def test_pvp_add_pvp_match(f_pvp_testbed, eddb_session):
    old_match = eddb_session.query(PVPMatch).order_by(PVPMatch.id.desc()).limit(1).one()
    assert 10 != old_match.limit

    pvp.schema.add_pvp_match(eddb_session, discord_channel_id=200, limit=10)

    match = eddb_session.query(PVPMatch).order_by(PVPMatch.id.desc()).limit(1).one()
    assert 10 == match.limit
    assert match.id != old_match.id
    assert match.discord_channel_id == 200


def test_pvp_get_pvp_match(f_pvp_testbed, eddb_session):
    match = pvp.schema.get_pvp_match(eddb_session, discord_channel_id=100)
    assert match.id == 2

    match = pvp.schema.get_pvp_match(eddb_session, discord_channel_id=99, state=PVPMatchState.SETUP)
    assert match.id == 1

    new_match = pvp.schema.add_pvp_match(eddb_session, discord_channel_id=99, limit=4)
    match = pvp.schema.get_pvp_match(eddb_session, discord_channel_id=99)
    assert match.id == new_match.id


def test_pvp_remove_players_from_match(f_pvp_testbed, eddb_session):
    match = eddb_session.query(PVPMatch).filter(PVPMatch.id == 1).one()
    assert len(match.players) == 3

    pvp.schema.remove_players_from_match(eddb_session, match_id=match.id, cmdr_ids=[1, 2])

    assert len(match.players) == 1


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
