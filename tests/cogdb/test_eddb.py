"""
Tests for local eddb copy
"""
import tempfile
import pytest

import cog.exc
import cogdb.eddb
from cogdb.eddb import (Commodity, CommodityCat, Module, ModuleGroup,
                        System, Faction, Allegiance, Government,
                        Station, StationFeatures, StationEconomy,
                        Influence, HistoryInfluence, HistoryTrack)

FAKE_ID1 = 942834121
FAKE_ID2 = FAKE_ID1 + 1
FAKE_ID3 = FAKE_ID1 + 2
HISTORY_TIMESTAMP = 1664897410


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
        'LHS 2459',
        'LHS 246',
        'LHS 262',
        'LHS 283',
        'LHS 6128',
        'LP 5-88',
        'LP 64-194',
        'Nang Ta-khian',
        'Nanomam',
        'Tollan'
    ]
    results = [x.name for x in cogdb.eddb.get_systems_around(eddb_session, "Nanomam", 15)]
    assert results == expected


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

    assert result == ['Ptah', 'Mombaluma', 'LHS 1197']


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


def test_get_nearest_ifactors(eddb_session):
    result = cogdb.eddb.get_nearest_ifactors(eddb_session, centre_name='Sol')

    system_names = {x[0] for x in result}
    assert "Lacaille 9352" in system_names


def test_um_trigger(eddb_session):
    system_names = ["Arnemil", "Nanomam"]
    systems = eddb_session.query(cogdb.eddb.System). \
        filter(cogdb.eddb.System.name.in_(system_names)). \
        all()
    assert cogdb.eddb.System.calc_um_trigger(systems[0], systems[1], 25) == 13306


def test_eddb_dump_db(eddb_session):
    with tempfile.NamedTemporaryFile() as temp:
        cogdb.eddb.dump_db(eddb_session, [cogdb.eddb.Allegiance], temp.name)
        with open(temp.name) as fin:
            assert "Allegiance(" in fin.read()


def test_check_eddb_base_subclass():
    assert cogdb.eddb.check_eddb_base_subclass(cogdb.eddb.System)
    assert not cogdb.eddb.check_eddb_base_subclass(cogdb.eddb.Base)


def test_eddb_make_parser():
    args = cogdb.eddb.make_parser().parse_args(["--preload", "--yes"])
    assert args.yes
    assert args.preload


def test_load_commodities():
    fname = cog.util.rel_to_abs("tests", "eddb_fake", "commodities.jsonl")
    objs = cogdb.eddb.load_commodities(fname)
    expected = (
        {CommodityCat(id=10000, name='FakeCategory')},
        [Commodity(id=20000, category_id=10000, name='FakeProdut', average_price=108, is_rare=0)]
    )
    assert objs == expected


def test_load_modules():
    fname = cog.util.rel_to_abs("tests", "eddb_fake", "modules.jsonl")
    objs = cogdb.eddb.load_modules(fname)
    expected = (
        {ModuleGroup(id=1000, name='Lightweight Alloy', category='Bulkhead', category_id=40)},
        [Module(id=10000, name=None, group_id=1000, size=1, rating='I', mass=None, price=None, ship='Sidewinder Mk. I', weapon_mode=None)]
    )
    assert objs == expected


def test_load_systems(eddb_session):
    power_ids = {x.text: x.id for x in eddb_session.query(cogdb.eddb.Power).all()}
    power_ids[None] = power_ids["None"]

    fname = cog.util.rel_to_abs("tests", "eddb_fake", "systems_populated.jsonl")
    objs = cogdb.eddb.load_systems(fname, power_ids)
    assert len(objs) == 3
    assert objs[0][0].name == "FakeSystem"


def test_load_factions():
    fname = cog.util.rel_to_abs("tests", "eddb_fake", "factions.jsonl")

    objs = cogdb.eddb.load_factions(fname, preload=False)
    expected = (
        {Allegiance(id=1000, text='FakeAllegiance', eddn=None)},
        {Government(id=1000, text='FakeGovernment', eddn=None)},
        [Faction(id=942834121, name='FakeFactionName', state_id=None, government_id=1000, allegiance_id=1000, home_system_id=None, is_player_faction=False, updated_at=1546622499)]
    )
    assert objs == expected


def test_load_stations(eddb_session):
    economy_ids = {x.text: x.id for x in eddb_session.query(cogdb.eddb.Economy).all()}
    economy_ids[None] = economy_ids['None']

    fname = cog.util.rel_to_abs("tests", "eddb_fake", "stations.jsonl")
    objs = cogdb.eddb.load_stations(fname, economy_ids)
    expected = (
        [],
        [Station(id=942834121, name='Porta', distance_to_star=995, max_landing_pad_size='L', type_id=8, system_id=189, controlling_minor_faction_id=13968, updated_at=1621238405)],
        [StationEconomy(id=942834121, economy_id=7, primary=True, proportion=None)],
        [StationFeatures(id=942834121, blackmarket=False, market=True, refuel=True, repair=True, rearm=True, outfitting=True, shipyard=True, dock=True, commodities=True)]
    )
    assert objs == expected


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

    dist, sorted_systems = cogdb.eddb.find_route_closest_hq(eddb_session, systems)
    assert [x.name for x in sorted_systems] == expected


def test_get_closest_station_by_government(eddb_session):
    results = cogdb.eddb.get_closest_station_by_government(eddb_session, 'Rana', 'Prison')
    expect = 'The Pillar of Fortitude'
    assert results[0][0].name == expect


def test_get_closest_station_by_government_bad_system(eddb_session):
    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.eddb.get_closest_station_by_government(eddb_session, 'zxzxzx', 'Prison')


def test_compute_all_exploits_from_control(eddb_session):
    found, not_found = cogdb.eddb.compute_all_exploits_from_controls(eddb_session, ['Abi', 'Nanomam'])

    assert 'LHS 215' in found
    assert 'Abi' in found
    assert not not_found


def test_compute_all_exploits_from_control_missing(eddb_session):
    found, not_found = cogdb.eddb.compute_all_exploits_from_controls(eddb_session, ['ZZZZ', 'Nanomam'])

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
        eddb_session.add(HistoryTrack(system_id=11905))
        eddb_session.commit()
        cogdb.eddb.add_history_track(eddb_session, ['Rana', 'LHS 215'])
        assert len(eddb_session.query(HistoryTrack).all()) == 2
    finally:
        eddb_session.rollback()
        eddb_session.query(HistoryTrack).delete()


def test_remove_history_track_exists(eddb_session):
    try:
        eddb_session.add(HistoryTrack(system_id=11905))
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
        cogdb.eddb.add_history_influence(eddb_session, Influence(
            system_id=1,
            faction_id=1,
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
        eddb_session.add(HistoryTrack(system_id=1))
        eddb_session.commit()
        assert not eddb_session.query(HistoryInfluence).all()
        cogdb.eddb.add_history_influence(eddb_session, Influence(
            system_id=1,
            faction_id=1,
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
    eddb_session.add(HistoryTrack(system_id=1))
    eddb_session.commit()
    influence = Influence(
        system_id=1,
        faction_id=1,
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
    eddb_session.add(HistoryTrack(system_id=1))
    eddb_session.commit()
    influence = Influence(
        system_id=1,
        faction_id=1,
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
