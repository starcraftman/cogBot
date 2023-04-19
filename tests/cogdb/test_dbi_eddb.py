"""
Tests for cogdb.dbi_eddb
"""
import tempfile

import pytest

import cogdb.dbi_eddb
from cogdb.eddb import (
    Allegiance, Government,
    Commodity, CommodityCat, Module, ModuleGroup, Influence,
    Faction, FactionActiveState, FactionPendingState, FactionRecoveringState,
    System, Station, StationEconomy, StationFeatures,
)
import cog.util


@pytest.fixture
def fixed_systems_json(eddb_session):
    faction_id = eddb_session.query(Faction.id).filter(Faction.name == "1 G. Caeli Empire League").scalar()
    factions = eddb_session.query(Faction).filter(Faction.name != "1 G. Caeli Empire League").limit(7).all()

    with tempfile.NamedTemporaryFile() as tfile:
        fname = cog.util.rel_to_abs("tests", "eddb_fake", "systems_populated.jsonl")
        with open(fname, 'r', encoding='utf-8') as fin, open(tfile.name, 'w', encoding='utf-8') as fout:
            text = fin.read().replace('"FACTIONID"', str(faction_id))
            for ind, faction in enumerate(factions, start=1):
                text = text.replace(f"INFID{ind}", str(faction.id))
            fout.write(text)

        yield tfile.name


@pytest.fixture
def fixed_station_json(eddb_session):
    faction_id = eddb_session.query(Faction.id).filter(Faction.name == "1 G. Caeli Empire League").scalar()

    with tempfile.NamedTemporaryFile() as tfile:
        fname = cog.util.rel_to_abs("tests", "eddb_fake", "stations.jsonl")
        with open(fname, 'r', encoding='utf-8') as fin, open(tfile.name, 'w', encoding='utf-8') as fout:
            text = fin.read().replace('"FACTIONID"', str(faction_id))
            fout.write(text)

        yield tfile.name


@pytest.fixture
def fixed_factions_json(eddb_session):
    yield cog.util.rel_to_abs("tests", "eddb_fake", "factions.jsonl")


def test_check_eddb_base_subclass():
    assert cogdb.dbi_eddb.check_eddb_base_subclass(cogdb.eddb.System)
    assert not cogdb.dbi_eddb.check_eddb_base_subclass(cogdb.eddb.Base)


def test_eddb_make_parser():
    args = cogdb.dbi_eddb.make_parser().parse_args(["--no-fetch", "--recreate", "--yes"])
    assert args.yes
    assert not args.fetch
    assert args.recreate
    assert not args.empty


def test_load_commodities(eddb_session):
    try:
        fname = cog.util.rel_to_abs("tests", "eddb_fake", "commodities.jsonl")
        cogdb.dbi_eddb.load_commodities(fname, preload=False)

        assert eddb_session.query(Commodity).filter(Commodity.id == 20000).one()
        assert eddb_session.query(CommodityCat).filter(CommodityCat.id == 10000).one()
    finally:
        eddb_session.query(Commodity).filter(Commodity.id == 20000).delete()
        eddb_session.query(CommodityCat).filter(CommodityCat.id == 10000).delete()


def test_load_modules(eddb_session):
    try:
        fname = cog.util.rel_to_abs("tests", "eddb_fake", "modules.jsonl")
        cogdb.dbi_eddb.load_modules(fname, preload=False)

        assert eddb_session.query(Module).filter(Module.id == 10000).one()
        assert eddb_session.query(ModuleGroup).filter(ModuleGroup.id == 1000).one()
    finally:
        eddb_session.query(Module).filter(Module.id == 10000).delete()
        eddb_session.query(ModuleGroup).filter(ModuleGroup.id == 1000).delete()


def test_load_factions(eddb_session, fixed_factions_json):
    try:
        cogdb.dbi_eddb.load_factions(fixed_factions_json, preload=False)

        assert eddb_session.query(Allegiance).filter(Allegiance.id == 1000).one
        assert eddb_session.query(Government).filter(Government.id == 1000).one()
        assert eddb_session.query(Faction).filter(Faction.id == 942834121).one()
    finally:
        eddb_session.query(Faction).filter(Faction.id == 942834121).delete()
        eddb_session.query(Allegiance).filter(Allegiance.id == 1000).delete()
        eddb_session.query(Government).filter(Government.id == 1000).delete()


def test_load_systems(eddb_session, fixed_systems_json):
    try:
        power_ids = {x.text: x.id for x in eddb_session.query(cogdb.dbi_eddb.Power).all()}
        power_ids[None] = power_ids["None"]
        eddb_session.close()

        cogdb.dbi_eddb.load_systems(fixed_systems_json, power_ids)
        assert eddb_session.query(System).filter(System.id == 942834121).one()
    finally:
        eddb_session.query(System).filter(System.id == 942834121).delete()


def test_load_influences(eddb_session, fixed_systems_json, fixed_factions_json):
    try:
        power_ids = {x.text: x.id for x in eddb_session.query(cogdb.dbi_eddb.Power).all()}
        power_ids[None] = power_ids["None"]
        eddb_session.close()

        cogdb.dbi_eddb.load_factions(fixed_factions_json, preload=False)
        cogdb.dbi_eddb.load_systems(fixed_systems_json, power_ids)
        cogdb.dbi_eddb.load_influences(fixed_systems_json, power_ids)

        assert 7 == eddb_session.query(Influence).filter(Influence.system_id == 942834121).count()
        assert 7 == eddb_session.query(FactionActiveState).filter(FactionActiveState.system_id == 942834121).count()
    finally:
        for cls in [Influence, FactionActiveState, FactionPendingState, FactionRecoveringState]:
            eddb_session.query(cls).filter(cls.system_id == 942834121).delete()
        eddb_session.query(System).filter(System.id == 942834121).delete()
        eddb_session.query(Faction).filter(Faction.id == 942834121).delete()
        eddb_session.query(Allegiance).filter(Allegiance.id == 1000).delete()
        eddb_session.query(Government).filter(Government.id == 1000).delete()


def test_load_stations(eddb_session, fixed_systems_json, fixed_factions_json, fixed_station_json):
    try:
        power_ids = {x.text: x.id for x in eddb_session.query(cogdb.dbi_eddb.Power).all()}
        power_ids[None] = power_ids["None"]
        economy_ids = {x.text: x.id for x in eddb_session.query(cogdb.dbi_eddb.Economy).all()}
        economy_ids[None] = economy_ids['None']
        eddb_session.close()

        cogdb.dbi_eddb.load_factions(fixed_factions_json, preload=False)
        cogdb.dbi_eddb.load_systems(fixed_systems_json, power_ids)
        cogdb.dbi_eddb.load_influences(fixed_systems_json, power_ids)
        cogdb.dbi_eddb.load_stations(fixed_station_json, economy_ids, preload=False, refresh_all=True)

        assert eddb_session.query(Station).filter(Station.id == 942834121).one()
        assert eddb_session.query(StationFeatures).filter(StationFeatures.id == 942834121).one()
        assert eddb_session.query(StationEconomy).filter(StationEconomy.id == 942834121).all()
    finally:
        for cls in [StationFeatures, StationEconomy, Station]:
            eddb_session.query(cls).filter(cls.id == 942834121).delete()
        for cls in [Influence, FactionActiveState, FactionPendingState, FactionRecoveringState]:
            eddb_session.query(cls).filter(cls.system_id == 942834121).delete()
        for cls in [System, Faction]:
            eddb_session.query(cls).filter(cls.id == 942834121).delete()
        eddb_session.query(Allegiance).filter(Allegiance.id == 1000).delete()
        eddb_session.query(Government).filter(Government.id == 1000).delete()
