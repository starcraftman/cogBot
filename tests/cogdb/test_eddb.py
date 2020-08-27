"""
Tests for local eddb copy
"""
import cogdb.eddb


def test_get_shipyard_stations(eddb_session):
    actual = cogdb.eddb.get_shipyard_stations(eddb_session, "Rana")
    assert actual[0][:3] == ['Rana', 0.0, 'Ali Hub']
    assert len(actual) > 10


def test_get_shipyard_stations_dist(eddb_session):
    actual = cogdb.eddb.get_shipyard_stations(eddb_session, "Rana", 30)
    wolf_124 = [x for x in actual if x[0] == 'Wolf 124'][0]
    assert wolf_124[:3] == ['Wolf 124', 15.08, 'Willis Port']
    assert len(actual) > 100


def test_get_shipyard_stations_dist_arrival(eddb_session):
    actual = cogdb.eddb.get_shipyard_stations(eddb_session, "Rana", 15, 50000)
    found = False
    for row in actual:
        if row[0] == "LTT 2151":
            found = True

    assert found
    assert len(actual) > 10


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
