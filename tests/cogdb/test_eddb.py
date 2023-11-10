# pylint: disable=redefined-outer-name,missing-function-docstring,unused-argument
"""
Tests for local eddb copy
"""
import tempfile
import pytest

import cog.exc
import cogdb.eddb
from cogdb.eddb import (
    Station, System, Faction, Influence, HistoryInfluence, HistoryTrack, TraderType, Ship
)

FAKE_ID1 = 942834121
FAKE_ID2 = FAKE_ID1 + 1
FAKE_ID3 = FAKE_ID1 + 2
HISTORY_TIMESTAMP = 1664897410


def test_ship__repr__(eddb_session):
    expect = "Ship(id=1, text='Adder', traffic_text='adder')"
    ship = eddb_session.query(Ship).filter(Ship.id == 1).one()

    assert expect == repr(ship)


def test_ship__str__(eddb_session):
    expect = "Ship: Adder"
    ship = eddb_session.query(Ship).filter(Ship.id == 1).one()

    assert expect == str(ship)


def test_station_carrier_factory(eddb_session):
    carrier = Station.carrier(
        name="TestCarrier",
        station_id=1,
        system_id=1,
        distance_to_star=1000,
    )
    assert carrier.type_id == 24
    assert carrier.name == "TestCarrier"


def test_system_controls(eddb_session):
    system = eddb_session.query(System).\
        filter(System.name == 'Togher').\
        one()

    assert len(system.controls) > 1


def test_system_exploiteds(eddb_session):
    system = eddb_session.query(System).\
        filter(System.name == 'Wat Yu').\
        one()

    assert len(system.exploiteds) > 10


def test_system_contesteds(eddb_session):
    system = eddb_session.query(System).\
        filter(System.name == 'Wat Yu').\
        one()

    assert system.contesteds[0].name == "Togher"


def test_base_get_stations(eddb_session):
    result = cogdb.eddb.base_get_stations(eddb_session, centre_name='Sol')

    system_names = {x[0] for x in result}
    assert "Lacaille 9352" in system_names


def test_get_nearest_stations_with_features(eddb_session):
    result = cogdb.eddb.get_nearest_stations_with_features(
        eddb_session, centre_name='Sol',
        features=['apexinterstellar', 'materialtrader', 'shipyard']
    )

    system_names = {x[0] for x in result}
    assert "GD 1192" in system_names


def test_get_nearest_traders_brokers_guardian(eddb_session):
    result = cogdb.eddb.get_nearest_traders(eddb_session, centre_name='Sol', trader_type=TraderType.BROKERS_GUARDIAN)

    station_names = {x[2] for x in result}
    assert "[L] Magnus Gateway" in station_names


def test_get_nearest_traders_brokers_human(eddb_session):
    result = cogdb.eddb.get_nearest_traders(eddb_session, centre_name='Sol', trader_type=TraderType.BROKERS_HUMAN)

    station_names = {x[2] for x in result}
    assert "[L] McKay Ring" in station_names


def test_get_nearest_traders_mats_data(eddb_session):
    result = cogdb.eddb.get_nearest_traders(eddb_session, centre_name='Sol', trader_type=TraderType.MATS_DATA)

    station_names = {x[2] for x in result}
    assert "[L] Magnus Gateway" in station_names


def test_get_nearest_traders_mats_raw(eddb_session):
    result = cogdb.eddb.get_nearest_traders(eddb_session, centre_name='Sol', trader_type=TraderType.MATS_RAW)

    station_names = {x[2] for x in result}
    assert "[L] Broglie Terminal" in station_names


def test_get_nearest_traders_mats_manufactured(eddb_session):
    result = cogdb.eddb.get_nearest_traders(eddb_session, centre_name='Sol', trader_type=TraderType.MATS_MANUFACTURED)

    station_names = {x[2] for x in result}
    assert "[L] Patterson Enter." in station_names


def test_get_shipyard_stations(eddb_session):
    actual = cogdb.eddb.get_shipyard_stations(eddb_session, "Rana")
    assert actual[0][:3] == ['Rana', 0.0, '[L] Ali Hub']
    assert len(actual) >= 10


def test_get_shipyard_stations_dist(eddb_session):
    actual = cogdb.eddb.get_shipyard_stations(eddb_session, "Rana", sys_dist=30)
    wolf_124 = [x for x in actual if x[0] == 'Wolf 124'][0]
    assert wolf_124[:3] == ['Wolf 124', 15.08, '[L] Willis Port']
    assert len(actual) >= 10


def test_get_shipyard_stations_dist_arrival(eddb_session):
    actual = cogdb.eddb.get_shipyard_stations(eddb_session, "Rana", sys_dist=15, arrival=50000)
    found = False
    for row in actual:
        if row[0] == "LTT 2151":
            found = True

    assert found
    assert len(actual) >= 10


def test_get_shipyard_stations_include_medium(eddb_session):
    actual = cogdb.eddb.get_shipyard_stations(eddb_session, "Rana", include_medium=True)
    found = False
    for row in actual:
        if row[2] == "[M] Virchow Orbital":
            found = True

    assert found
    assert len(actual) >= 10


def test_get_systems(eddb_session):
    system_names = ["Arnemil", "Rana", "Sol", "Frey", "Nanomam"]
    for system in cogdb.eddb.get_systems(eddb_session, system_names):
        assert isinstance(system, cogdb.eddb.System)
        assert system.name in system_names
        system_names.remove(system.name)

    assert not system_names


def test_get_systems_around(eddb_session):
    expected = [
        '44 chi Draconis',
        'Acihaut',
        'Bodedi',
        'DX 799',
        'G 239-25',
        'Lalande 18115',
        'LFT 880',
        'LHS 1885',
        'LHS 215',
        'LHS 221',
        'LHS 2211',
        'LHS 2459',
        'LHS 246',
        'LHS 262',
        'LHS 283',
        'LHS 316',
        'LHS 6128',
        'LP 5-88',
        'LP 64-194',
        'Nang Ta-khian',
        'Nanomam',
        'SZ Ursae Majoris',
        'Tollan'
    ]
    results = [x.name for x in cogdb.eddb.get_systems_around(eddb_session, "Nanomam")]
    assert results == expected


def test_get_influences_by_id(eddb_session):
    assert len(cogdb.eddb.get_influences_by_id(eddb_session, [1, 2, 3])) == 3


def test_nearest_system(eddb_session):
    system_names = ["Arnemil", "Rana", "Sol", "Frey", "Nanomam"]
    systems = eddb_session.query(cogdb.eddb.System).\
        filter(cogdb.eddb.System.name.in_(system_names)).\
        all()

    result = cogdb.eddb.nearest_system(systems[0], systems[1:])
    assert int(result[0]) == 61
    assert result[1].name == "Nanomam"


def test_find_route(eddb_session):
    system_names = ["Arnemil", "Rana", "Sol", "Frey", "Nanomam"]
    systems = eddb_session.query(cogdb.eddb.System).\
        filter(cogdb.eddb.System.name.in_(system_names)).\
        all()

    result = cogdb.eddb.find_route(eddb_session, systems[0], systems[1:])
    assert int(result[0]) == 246
    assert [x.name for x in result[1]] == ['Arnemil', 'Nanomam', 'Sol', 'Rana', 'Frey']


def test_find_best_route(eddb_session):
    system_names = ["Arnemil", "Rana", "Sol", "Frey", "Nanomam"]
    result = cogdb.eddb.find_best_route(eddb_session, system_names)
    assert int(result[0]) == 246
    assert [x.name for x in result[1]] == ['Arnemil', 'Nanomam', 'Sol', 'Rana', 'Frey']


def test_get_nearest_controls(eddb_session):
    result = [x.name for x in cogdb.eddb.get_nearest_controls(eddb_session, power='%hudson', limit=3)]

    assert result == ['Sol', 'Lung', 'Groombridge 1618']

    result = [x.name for x in cogdb.eddb.get_nearest_controls(eddb_session, centre_name='cubeo', power='%hudson', limit=3)]

    assert result == ['Ptah', 'LHS 1197', 'Alpha Fornacis']


def test_compute_dists(eddb_session):
    expect = [
        ('Othime', 83.67581252406517),
        ('Rana', 46.100296145334035),
        ('Sol', 28.938141191600405),
    ]
    actual = cogdb.eddb.compute_dists(eddb_session, ['Nanomam', 'Sol', 'Rana', 'Othime'])
    assert actual == expect


def test_compute_dists_incomplete(eddb_session):
    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.eddb.compute_dists(eddb_session, ['Nanomam', 'Sol', 'Rana', 'Othimezzz'])


def test_bgs_funcs_hudson():
    strong, weak = cogdb.eddb.bgs_funcs('Nanomam')

    assert strong("Feudal")
    assert strong("Patronage")
    assert weak("Dictatorship")


def test_bgs_funcs_winters():
    strong, weak = cogdb.eddb.bgs_funcs('Rhea')

    assert strong("Corporate")
    assert weak("Communism")
    assert weak("Cooperative")
    assert weak("Feudal")
    assert weak("Patronage")


def test_get_power_hq():
    assert cogdb.eddb.get_power_hq('hudson') == ['Zachary Hudson', 'Nanomam']


def test_get_power_hq_too_many():
    with pytest.raises(cog.exc.InvalidCommandArgs):
        assert cogdb.eddb.get_power_hq('duval')


def test_get_power_hq_no_match():
    with pytest.raises(cog.exc.InvalidCommandArgs):
        assert cogdb.eddb.get_power_hq('zzzzzzz')


def test_um_trigger(eddb_session):
    system_names = ["Arnemil", "Nanomam"]
    systems = eddb_session.query(cogdb.eddb.System). \
        filter(cogdb.eddb.System.name.in_(system_names)). \
        all()
    assert cogdb.eddb.System.calc_um_trigger(systems[0], systems[1], 25) == 13306


def test_eddb_dump_db(eddb_session):
    with tempfile.NamedTemporaryFile() as temp:
        cogdb.eddb.dump_db(eddb_session, [cogdb.eddb.Allegiance], temp.name)
        with open(temp.name, encoding='utf-8') as fin:
            assert "Allegiance(" in fin.read()


def test_get_controls_of_power(eddb_session):
    systems = cogdb.eddb.get_controls_of_power(eddb_session, power='%hudson')
    assert "Nanomam" in systems

    systems = cogdb.eddb.get_controls_of_power(eddb_session, power='%winters')
    assert "Rhea" in systems


def test_get_systems_of_power(eddb_session):
    systems = cogdb.eddb.get_systems_of_power(eddb_session, power='%hudson')
    assert "Nanomam" in systems
    assert "Yen Ti" in systems

    systems = cogdb.eddb.get_systems_of_power(eddb_session, power='%winters')
    assert "Rhea" in systems
    assert "Shalit" in systems


def test_is_system_of_power(eddb_session):
    assert cogdb.eddb.is_system_of_power(eddb_session, "Nanomam", power='%hudson')
    assert cogdb.eddb.is_system_of_power(eddb_session, "Yen Ti", power='%hudson')
    assert not cogdb.eddb.is_system_of_power(eddb_session, "Rhea", power='%hudson')

    assert cogdb.eddb.is_system_of_power(eddb_session, "Rhea", power='%winters')
    assert cogdb.eddb.is_system_of_power(eddb_session, "Shalit", power='%winters')
    assert not cogdb.eddb.is_system_of_power(eddb_session, "Nanomam", power='%winters')


def test_get_system_closest_to_HQ(eddb_session):
    systems = ['Rana', 'Adeo', 'Cubeo', 'Sol', 'Rhea']
    result = cogdb.eddb.get_system_closest_to_HQ(eddb_session, systems)
    assert result.name == "Sol"


def test_find_route_from_hq(eddb_session):
    systems = ['Rana', 'Adeo', 'Cubeo', 'Sol', 'Rhea']
    expected = ['Sol', 'Rana', 'Rhea', 'Adeo', 'Cubeo']

    _, sorted_systems = cogdb.eddb.find_route_closest_hq(eddb_session, systems)
    assert [x.name for x in sorted_systems] == expected


def test_get_closest_station_by_government(eddb_session):
    results = cogdb.eddb.get_closest_station_by_government(eddb_session, 'Rana', 'Prison')
    expect = 'The Pillar of Fortitude'
    assert results[0][0].name == expect


def test_get_closest_station_by_government_bad_system(eddb_session):
    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.eddb.get_closest_station_by_government(eddb_session, 'zxzxzx', 'Prison')


def test_get_all_systems_named_exploiteds(eddb_session):
    found, not_found = cogdb.eddb.get_all_systems_named(eddb_session, ['Abi', 'Nanomam'], include_exploiteds=True)

    found = {x.name for x in found}
    assert 'LHS 215' in found
    assert 'Abi' in found
    assert not not_found


def test_get_all_systems_named_missing(eddb_session):
    found, not_found = cogdb.eddb.get_all_systems_named(eddb_session, ['ZZZZ', 'Nanomam'], include_exploiteds=True)

    found = {x.name for x in found}
    assert 'LHS 215' in found
    assert 'Abi' not in found
    assert ['ZZZZ'] == not_found


def test_add_history_track_empty(eddb_session):
    try:
        assert not eddb_session.query(HistoryTrack).all()
        cogdb.eddb.add_history_track(eddb_session, ['Rana', 'LHS 215'])
        assert len(eddb_session.query(HistoryTrack).all()) == 2
    finally:
        eddb_session.rollback()
        eddb_session.query(HistoryTrack).delete()


def test_add_history_track_exists(eddb_session):
    try:
        rana = eddb_session.query(cogdb.eddb.System).\
            filter(cogdb.eddb.System.name == 'Rana').\
            one()
        eddb_session.add(HistoryTrack(system_id=rana.id))
        eddb_session.commit()
        cogdb.eddb.add_history_track(eddb_session, ['Rana', 'LHS 215'])
        assert len(eddb_session.query(HistoryTrack).all()) == 2
    finally:
        eddb_session.rollback()
        eddb_session.query(HistoryTrack).delete()


def test_remove_history_track_exists(eddb_session):
    try:
        rana = eddb_session.query(cogdb.eddb.System).\
            filter(cogdb.eddb.System.name == 'Rana').\
            one()
        eddb_session.add(HistoryTrack(system_id=rana.id))
        eddb_session.commit()
        assert eddb_session.query(HistoryTrack).all()
        cogdb.eddb.remove_history_track(eddb_session, ['Rana', 'LHS 215'])
        assert not eddb_session.query(HistoryTrack).all()
    finally:
        eddb_session.rollback()
        eddb_session.query(HistoryTrack).delete()


def test_add_history_influence_not_Tracked(eddb_session):
    try:
        assert not eddb_session.query(HistoryInfluence).all()
        system = eddb_session.query(System).first()
        faction = eddb_session.query(Faction).first()
        cogdb.eddb.add_history_influence(eddb_session, Influence(
            system_id=system.id,
            faction_id=faction.id,
            happiness_id=1,
            influence=20,
            is_controlling_faction=False,
            updated_at=HISTORY_TIMESTAMP))
        assert not eddb_session.query(HistoryInfluence).all()
    finally:
        eddb_session.rollback()
        eddb_session.query(HistoryInfluence).delete()
        eddb_session.query(HistoryTrack).delete()


def test_add_history_influence_empty(eddb_session):
    try:
        system = eddb_session.query(System).first()
        faction = eddb_session.query(Faction).first()
        eddb_session.add(HistoryTrack(system_id=system.id))
        eddb_session.commit()
        assert not eddb_session.query(HistoryInfluence).all()
        cogdb.eddb.add_history_influence(eddb_session, Influence(
            system_id=system.id,
            faction_id=faction.id,
            happiness_id=1,
            influence=20,
            is_controlling_faction=False,
            updated_at=HISTORY_TIMESTAMP))
        assert eddb_session.query(HistoryInfluence).all()
    finally:
        eddb_session.rollback()
        eddb_session.query(HistoryInfluence).delete()
        eddb_session.query(HistoryTrack).delete()


def test_add_history_influence_limit_passed(eddb_session):
    system = eddb_session.query(System).first()
    faction = eddb_session.query(Faction).first()
    eddb_session.add(HistoryTrack(system_id=system.id))
    eddb_session.commit()
    influence = Influence(
        system_id=system.id,
        faction_id=faction.id,
        happiness_id=1,
        influence=20,
        is_controlling_faction=False,
        updated_at=HISTORY_TIMESTAMP
    )
    try:
        cnt = cogdb.eddb.HISTORY_INF_LIMIT + 10
        while cnt:
            cogdb.eddb.add_history_influence(eddb_session, influence)
            cnt -= 1
            influence.influence += 1
            influence.updated_at += 60 * 60 * 4
        assert len(eddb_session.query(HistoryInfluence).all()) == 41

    finally:
        eddb_session.rollback()
        eddb_session.query(HistoryInfluence).delete()
        eddb_session.query(HistoryTrack).delete()


def test_add_history_influence_inf_diff(eddb_session):
    system = eddb_session.query(System).first()
    faction = eddb_session.query(Faction).first()
    eddb_session.add(HistoryTrack(system_id=system.id))
    eddb_session.commit()
    influence = Influence(
        system_id=system.id,
        faction_id=faction.id,
        happiness_id=1,
        influence=20,
        is_controlling_faction=False,
        updated_at=HISTORY_TIMESTAMP
    )
    try:
        cogdb.eddb.add_history_influence(eddb_session, influence)
        cogdb.eddb.add_history_influence(eddb_session, influence)
        cogdb.eddb.add_history_influence(eddb_session, influence)
        influence.updated_at += 60 * 60 * 2
        influence.influence += 1.0
        cogdb.eddb.add_history_influence(eddb_session, influence)
        assert len(eddb_session.query(HistoryInfluence).all()) == 2

    finally:
        eddb_session.rollback()
        eddb_session.query(HistoryInfluence).delete()
        eddb_session.query(HistoryTrack).delete()


def test_get_power_by_name(eddb_session):
    assert "Felicia Winters" == cogdb.eddb.get_power_by_name(eddb_session, 'FW').text
    assert "Zachary Hudson" == cogdb.eddb.get_power_by_name(eddb_session, 'huds').text


def test_get_power_by_name_fails(eddb_session):
    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.eddb.get_power_by_name(eddb_session, 'ADD')
    try:
        cogdb.eddb.get_power_by_name(eddb_session, 'ADD')
    except cog.exc.InvalidCommandArgs as exc:
        print(str(exc))


def test_service_status(eddb_session):
    cells = cogdb.eddb.service_status(eddb_session)
    assert 'Latest EDDB' in cells[0][0]
    assert 'ago)' in cells[0][1]
