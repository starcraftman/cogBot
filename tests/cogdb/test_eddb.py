"""
Tests for local eddb copy
"""
import pytest

import cog.exc
import cogdb.eddb


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
    result = [x.name for x in cogdb.eddb.get_nearest_controls(eddb_session, power='Hudson')[0:3]]

    assert result == ['Sol', 'Lung', 'Groombridge 1618']

    result = [x.name for x in cogdb.eddb.get_nearest_controls(eddb_session, centre_name='cubeo', power='Hudson')[0:3]]

    assert result == ['Caspatsuria', 'Clayahu', 'Epsilon Pavonis']


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
    strong, weak = cogdb.eddb.bgs_funcs('Rana')

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

    system_names = list(set([x[0] for x in result]))
    assert "Stopover" in system_names
    assert "LHS 449" in system_names
