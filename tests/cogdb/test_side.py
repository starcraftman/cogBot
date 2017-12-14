"""
Test remote queries to sidewinder's db.
"""
from __future__ import absolute_import, print_function
import datetime
import pytest

from sqlalchemy.sql import text as sql_text

import cog.exc
import cog.util
import cogdb.side
from cogdb.side import BGSTick, SystemAge, System


def test_bgstick__repr__(side_session):
    tick = BGSTick(day=datetime.date(2017, 5, 1), tick=datetime.datetime(2017, 5, 1, 16, 0), unix_from=1493654400, unix_to=1493740800)
    assert repr(tick) == "BGSTick(day=datetime.date(2017, 5, 1), tick=datetime.datetime(2017, 5, 1, 16, 0), unix_from=1493654400, unix_to=1493740800)"


def test_systemage__repr__(side_session):
    system = SystemAge(control='16 Cygni', system='16 Cygni', age=1)
    assert repr(system) == "SystemAge(control='16 Cygni', system='16 Cygni', age=1)"


def test_next_bgs_tick(side_session, f_bot):
    cog.util.BOT = f_bot
    query = sql_text("SELECT tick FROM bgs_tick ORDER BY tick desc LIMIT 1")
    last_tick = side_session.execute(query).fetchone()[0]

    before_last = last_tick - datetime.timedelta(hours=4)
    msg = cogdb.side.next_bgs_tick(side_session, before_last)
    assert "BGS Tick in **4:00:00**" in msg

    after_last = last_tick + datetime.timedelta(hours=4)
    with pytest.raises(cog.exc.NoMoreTargets):
        msg = cogdb.side.next_bgs_tick(side_session, after_last)


def test_exploited_systems_by_age(side_session):
    query = sql_text("SELECT control, system FROM v_age ORDER BY system asc LIMIT 1")
    control, system = side_session.execute(query).fetchone()

    result = cogdb.side.exploited_systems_by_age(side_session, control)
    for row in result:
        assert row.control == control
    assert (result[0].control, result[0].system) == (control, system)


def test_influence_in_system(side_session):
    assert "Mother Gaia" in [ent[0] for ent in cogdb.side.influence_in_system(side_session, 'Sol')]


def test_station_suffix():
    assert cogdb.side.station_suffix('default not found') == ' (No Dock)'
    assert cogdb.side.station_suffix('Planetary Outpost') == ' (P)'
    assert cogdb.side.station_suffix('Orbis Starport') == ' (L)'
    assert cogdb.side.station_suffix('Asteroid Base') == ' (AB)'
    assert cogdb.side.station_suffix('Military Outpost') == ' (M)'


def test_stations_in_system(side_session):
    station_dict = cogdb.side.stations_in_system(side_session, 15222)
    stations = []
    for station in station_dict.values():
        stations.extend(station)

    assert set(['Virtanen Gateway (L)', 'Grover Point (P)']) == set(stations)


def test_influence_history_in_system(side_session):
    fact_ids = [75621, 38140, 38134, 33854, 8190, 38139]
    inf_history = cogdb.side.influence_history_in_system(side_session, 15222, fact_ids)

    for key in inf_history:
        assert key in fact_ids
        for inf in inf_history[key]:
            assert inf.faction_id == key
            assert isinstance(inf, cogdb.side.InfluenceHistory)


def test_system_overview(side_session):
    system, factions = cogdb.side.system_overview(side_session, 'Palaemon')

    assert system.name == 'Palaemon'
    frg = factions[0]
    assert frg['name'] == '-> Conf | Federal Republican Guard'
    assert frg['player'] == 1


def test_count_factions_in_system(side_session):
    sys_id = side_session.query(System.id).filter(System.name == "Sol").one()
    assert cogdb.side.count_factions_in_systems(side_session, sys_id) == {"Sol": 6}


def test_inf_history_for_pairs(side_session):
    # Pairs in Sol, essentially static, last is engineer ignore
    pairs = [(17072, 588), (17072, 589), (17072, 591), (17072, 592), (17072, 593)]
    result = cogdb.side.inf_history_for_pairs(side_session, pairs)

    for sys_id, fact_id in pairs:
        assert "{}_{}".format(sys_id, fact_id) in result


def test_dash_overview(side_session):
    control, factions, net_change, fact_count = cogdb.side.dash_overview(side_session, 'Sol')

    assert control.name == "Sol"
    assert fact_count["Sol"] == 6
    assert "Sol" in net_change
    sol_control = [x[1] for x in factions if x[0].name == "Sol"][0]
    assert sol_control.id in [588, 589, 591, 592, 593]
