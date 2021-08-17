"""
Tests for local eddb copy
"""
import tempfile
import pytest
from sqlalchemy.orm.exc import NoResultFound
import sqlalchemy.orm as sqla_orm

import cog.exc
import cogdb.eddb
from cogdb.eddb import (Commodity, CommodityCat, Module, ModuleGroup,
                        System, Influence, FactionActiveState, Faction,
                        Allegiance, Government, Station, StationFeatures, StationEconomy,
                        SystemControl)

FAKE_ID1 = 942834121
FAKE_ID2 = FAKE_ID1 + 1
FAKE_ID3 = FAKE_ID1 + 2


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
    result = [x.name for x in cogdb.eddb.get_nearest_controls(eddb_session, power='Hudson')[0:3]]

    assert result == ['Sol', 'Lung', 'Groombridge 1618']

    result = [x.name for x in cogdb.eddb.get_nearest_controls(eddb_session, centre_name='cubeo', power='Hudson')[0:3]]

    assert result == ['Caspatsuria', 'LTT 9472', 'Clayahu']


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

    system_names = {x[0] for x in result}
    assert "Barnard's Star" in system_names


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


def test_load_commodities(eddb_session):
    fname = cog.util.rel_to_abs("tests", "eddb_fake", "commodities.jsonl")
    try:
        cogdb.eddb.load_commodities(eddb_session, fname)
        assert eddb_session.query(Commodity).filter(Commodity.id == 20000).one()
        assert eddb_session.query(CommodityCat).filter(CommodityCat.id == 10000).one()
    finally:
        eddb_session.rollback()
        try:
            eddb_session.query(Commodity).filter(Commodity.id == 20000).delete()
        except NoResultFound:
            pass
        try:
            eddb_session.query(CommodityCat).filter(CommodityCat.id == 10000).delete()
        except NoResultFound:
            pass


def test_load_modules(eddb_session):
    fname = cog.util.rel_to_abs("tests", "eddb_fake", "modules.jsonl")
    try:
        cogdb.eddb.load_modules(eddb_session, fname)
        assert eddb_session.query(Module).filter(Module.id == 10000).one()
        assert eddb_session.query(ModuleGroup).filter(ModuleGroup.id == 1000).one()
    finally:
        eddb_session.rollback()
        try:
            eddb_session.query(Module).filter(Module.id == 10000).delete()
        except NoResultFound:
            pass
        try:
            eddb_session.query(ModuleGroup).filter(ModuleGroup.id == 1000).delete()
        except NoResultFound:
            pass


def test_load_systems(eddb_session):
    fname = cog.util.rel_to_abs("tests", "eddb_fake", "systems_populated.jsonl")
    try:
        cogdb.eddb.load_systems(eddb_session, fname)
        assert eddb_session.query(Influence).filter(Influence.system_id == FAKE_ID1).all()
        assert eddb_session.query(FactionActiveState).filter(FactionActiveState.system_id == FAKE_ID1).all()
        assert eddb_session.query(System).filter(System.id == FAKE_ID1).one()
    finally:
        eddb_session.rollback()
        try:
            eddb_session.query(FactionActiveState).filter(FactionActiveState.system_id == FAKE_ID1).delete()
        except NoResultFound:
            pass
        try:
            eddb_session.query(Influence).filter(Influence.system_id == FAKE_ID1).delete()
        except NoResultFound:
            pass
        try:
            eddb_session.query(System).filter(System.id == FAKE_ID1).delete()
        except NoResultFound:
            pass


def test_load_factions(eddb_session):
    fname = cog.util.rel_to_abs("tests", "eddb_fake", "factions.jsonl")
    try:
        cogdb.eddb.load_factions(eddb_session, fname, preload=False)
        assert eddb_session.query(Faction).filter(Faction.id == FAKE_ID1).one()
        assert eddb_session.query(Allegiance).filter(Allegiance.id == 1000).one()
        assert eddb_session.query(Government).filter(Government.id == 1000).one()
    finally:
        eddb_session.rollback()
        try:
            eddb_session.query(Faction).filter(Faction.id == FAKE_ID1).delete()
        except NoResultFound:
            pass
        try:
            eddb_session.query(Allegiance).filter(Allegiance.id == 1000).delete()
        except NoResultFound:
            pass
        try:
            eddb_session.query(Government).filter(Government.id == 1000).delete()
        except NoResultFound:
            pass


def test_load_stations(eddb_session):
    fname = cog.util.rel_to_abs("tests", "eddb_fake", "stations.jsonl")
    try:
        cogdb.eddb.load_stations(eddb_session, fname)
        assert eddb_session.query(StationEconomy).filter(StationEconomy.id == FAKE_ID1).one()
        assert eddb_session.query(StationFeatures).filter(StationFeatures.id == FAKE_ID1).one()
        assert eddb_session.query(Station).filter(Station.id == FAKE_ID1).one()
    finally:
        eddb_session.rollback()
        try:
            eddb_session.query(StationEconomy).filter(StationEconomy.id == FAKE_ID1).delete()
        except NoResultFound:
            pass
        try:
            eddb_session.query(StationFeatures).filter(StationFeatures.id == FAKE_ID1).delete()
        except NoResultFound:
            pass
        try:
            eddb_session.query(Station).filter(Station.id == FAKE_ID1).delete()
        except NoResultFound:
            pass
