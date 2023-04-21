# pylint: disable=redefined-outer-name,missing-function-docstring,unused-argument
"""
Test remote queries to sidewinder's db.
"""
import datetime
import pytest

from sqlalchemy.sql import text as sql_text

import cog.exc
import cog.util
import cogdb.side
from cogdb.side import BGSTick, SystemAge, System, Faction, FactionState, Government, Influence


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
    query = sql_text("SELECT control, system FROM v_age LIMIT 1")
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
    assert cogdb.side.station_suffix('Fleet Carrier') == ' (C)'


def test_stations_in_system(side_session):
    station_dict = cogdb.side.stations_in_system(side_session, 15222)
    stations = []
    for station in station_dict.values():
        stations.extend(station)

    assert set(['Virtanen Gateway (L)', 'Grover Point (P)']) == set(stations)


def test_influence_history_in_system(side_session):
    fact_ids = [75621, 38140, 38134, 33854, 8190, 38139]
    inf_history = cogdb.side.influence_history_in_system(side_session, 15222, fact_ids)

    for key, infs in inf_history.items():
        assert key in fact_ids
        for inf in infs:
            assert inf.faction_id == key
            assert isinstance(inf, cogdb.side.InfluenceHistory)


def test_system_overview(side_session):
    system, factions = cogdb.side.system_overview(side_session, 'Palaemon')

    assert system.name == 'Palaemon'
    frg = factions[0]
    assert frg['name'] == '-> Conf | Federal Republican Guard'
    assert frg['player'] == 1


def test_count_factions_in_system(side_session):
    sys_ids = side_session.query(System.id).filter(System.name.in_(["Sol", "Rana", "Othime"])).all()
    sys_ids = [x[0] for x in sys_ids]

    result = cogdb.side.count_factions_in_systems(side_session, sys_ids)
    assert isinstance(result, type({}))
    for key in ("Sol", "Othime", "Rana"):
        assert key in result
        assert result[key] > 1 and result[key] < 9


def test_inf_history_for_pairs(side_session):
    # Pairs in Sol, essentially static
    pairs = [(17072, 588), (17072, 589), (17072, 591), (17072, 592)]
    result = cogdb.side.inf_history_for_pairs(side_session, pairs)

    assert isinstance(result, type({}))
    for sys_id, fact_id in pairs:
        assert f"{sys_id}_{fact_id}" in result


def test_dash_overview(side_session):
    control, factions, net_change, fact_count = cogdb.side.dash_overview(side_session, 'Abi')

    assert control.name == "Abi"
    assert fact_count["Abi"] >= 6
    assert "Abi" in net_change
    abi_control = [x[1] for x in factions if x[0].name == "Abi"][0]

    assert abi_control.id in [2473, 2474, 2475, 2476, 23290, 24164, 49170, 76877]


def test_find_favorable(side_session):
    matches = cogdb.side.find_favorable(side_session, 'Nurundere')
    assert matches[1][-1] == "Monarchy of Orisala"

    matches = cogdb.side.find_favorable(side_session, 'Rana', 50)
    assert len(matches) > 50


def test_expansion_candidates(side_session):
    system = side_session.query(System).filter(System.name == 'Arnemil').one()
    faction = side_session.query(Faction).filter(Faction.name == 'Arnemil Monarchy').one()
    matches = cogdb.side.expansion_candidates(side_session, system, faction)
    assert len(matches) != 1


def test_get_factions_in_system(side_session):
    factions = cogdb.side.get_factions_in_system(side_session, 'Rana')
    assert "Rana State Network" in [fact.name for fact in factions]
    for fact in factions:
        assert isinstance(fact, cogdb.side.Faction)

    factions = cogdb.side.get_factions_in_system(side_session, 'Nurunddddddd')
    assert not factions


def test_expand_to_candidates(side_session):
    matches = cogdb.side.expand_to_candidates(side_session, 'Rana')
    assert len(matches) > 1


def test_system_calc_upkeep(side_session):
    pow_hq, target = side_session.query(System).\
        filter(System.name.in_(["Nanomam", "Rana"])).\
        order_by(System.name).\
        all()
    assert target.calc_upkeep(pow_hq) == 22.1


def test_system_calc_fort_trigger(side_session):
    pow_hq, target = side_session.query(System).\
        filter(System.name.in_(["Nanomam", "Rana"])).\
        order_by(System.name).\
        all()
    assert target.calc_fort_trigger(pow_hq) == 5620


def test_system_calc_um_trigger(side_session):
    pow_hq, target = side_session.query(System).\
        filter(System.name.in_(["Nanomam", "Rana"])).\
        order_by(System.name).\
        all()
    assert target.calc_um_trigger(pow_hq) == 13786


# TODO: Think a better test, other data optional
def test_get_system_ages(side_session):
    sys_ages = cogdb.side.get_system_ages(side_session, ['Rana', 'Sol'])

    assert isinstance(sys_ages, type({}))
    assert isinstance(sys_ages['Rana'], type([]))
    for age in sys_ages['Rana']:
        assert age.control == 'Rana'
    assert isinstance(sys_ages['Sol'], type([]))
    for age in sys_ages['Sol']:
        assert age.control == 'Sol'


def test_get_monitor_systems(side_session):
    systems = ['Rana', 'Sol', 'Rhea', 'Adeo']
    results = cogdb.side.get_monitor_systems(side_session, systems)

    assert len(results) > 70


def test_control_dictators(side_session):
    gov_dic = side_session.query(Government.id).\
        filter(Government.text.in_(["Anarchy", "Dictatorship"])).\
        scalar_subquery()
    system_ids = side_session.query(Influence.system_id).\
        join(System, Influence.system_id == System.id).\
        join(Faction, Influence.faction_id == Faction.id).\
        join(Government, Faction.government_id == Government.id).\
        filter(Government.id.in_(gov_dic),
               System.dist_to_nanomam < 90).\
        group_by(System.id).\
        limit(50).\
        all()
    system_ids = [x[0] for x in system_ids]

    results = cogdb.side.control_dictators(side_session, system_ids)
    assert "| Dictatorship |" in str(results)


def test_moving_dictators(side_session):
    gov_dic = side_session.query(Government.id).\
        filter(Government.text.in_(["Anarchy", "Dictatorship"])).\
        scalar_subquery()
    system_ids = side_session.query(Influence.system_id).\
        join(System, Influence.system_id == System.id).\
        join(Faction, Influence.faction_id == Faction.id).\
        join(Government, Faction.government_id == Government.id).\
        filter(Government.id.in_(gov_dic),
               System.dist_to_nanomam < 90).\
        group_by(System.id).\
        limit(20).\
        all()
    system_ids = [x[0] for x in system_ids]

    results = cogdb.side.moving_dictators(side_session, system_ids)
    assert "| Dic |" in str(results)


# Changing states relative data  makes meaningful test difficult
def test_monitor_events(side_session):
    monitor_states = side_session.query(FactionState.id).\
        filter(FactionState.text.in_(["Election", "War", "Civil War", "Expansion", "Retreat"])).\
        scalar_subquery()
    system_ids = side_session.query(System.id).\
        join(Faction, System.controlling_faction_id == Faction.id).\
        join(FactionState, Faction.state_id == FactionState.id).\
        filter(FactionState.id.in_(monitor_states),
               System.dist_to_nanomam < 90).\
        limit(25).\
        all()
    system_ids = [x[0] for x in system_ids]

    results = cogdb.side.monitor_events(side_session, system_ids)
    assert results


def test_monitor_factions(side_session):
    faction_names = ["Sol Workers' Party", "Sol Nationalists"]
    results = cogdb.side.monitor_factions(side_session, faction_names)

    assert "Sol         | Sol            | Sol Workers' Par | Dem |" in '\n'.join(results)


def test_service_status(side_session):
    cells = cogdb.side.service_status(side_session)
    assert 'Sidewinder DB' in cells[0][0]
    assert cells[0][1] in ['Up', 'Down']
    assert 'Last estimated tick' in cells[1][0]
