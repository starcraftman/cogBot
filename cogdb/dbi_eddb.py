"""
Dbi command that will import eddb.io
Extracted CLI interface from cogdb.eddb
Mainly archived in case need arises again.
"""
import asyncio
import argparse
import concurrent.futures as cfut
import copy
import datetime
import glob
import inspect
import os
import sys

import aiofiles
import aiohttp
import ijson
import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm

import cogdb.eddb
from cogdb.eddb import (
    Allegiance, Government, Power, PowerState, Economy,
    Commodity, CommodityCat, Module, ModuleGroup, Influence,
    Faction, FactionActiveState, FactionPendingState, FactionRecoveringState,
    System, Station, StationType, StationEconomy, StationFeatures,
    FACTION_STATE_PAIRS, SystemContestedV, SystemControlV
)
import cog.util


EDDB_D = cog.util.rel_to_abs('data', 'eddb')
CHUNK_LIMIT = 10000
EDDB_URLS = [
    #  "https://eddb.io/archive/v6/attractions.json",  # Beacons, abandoned bases
    "https://eddb.io/archive/v6/commodities.json",
    "https://eddb.io/archive/v6/factions.json",
    # "https://eddb.io/archive/v6/listings.csv",  # Commodity pricing
    "https://eddb.io/archive/v6/modules.json",
    "https://eddb.io/archive/v6/stations.json",
    #  "https://eddb.io/archive/v6/systems.csv",  # All systems
    "https://eddb.io/archive/v6/systems_populated.json",
]


async def a_jq_post_process(fname):
    """
    Use jq command line to reprocess fname (a json) into ...
        - A pretty printed jsonl file for easy reading.
        - A ONE object per line file for parallel processing.
    """
    async with aiofiles.open(fname + 'l', "w") as fout_l:
        async with aiofiles.open(fname + '_per_line', "w") as fout_line:
            await asyncio.gather(
                asyncio.create_subprocess_shell(f'jq . -S {fname}', stdout=fout_l),
                asyncio.create_subprocess_shell(f'jq -c .[] {fname}', stdout=fout_line),
            )
    print("Created PRETTY file", fname + 'l')
    print("Created object PER LINE file", fname + '_per_line')


# Directed Header: Accept-Encoding: gzip, deflate, sdch
async def fetch(url, fname, sort=True):
    """
    Fetch a file and write it out in chunks to a file named.
    """
    print("Download started", url)

    headers = {"Accept-Encoding": "gzip, deflate, sdch"}
    async with aiofiles.open(fname, "wb") as fout,\
            aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url) as resp:
            chunk = await resp.content.read(CHUNK_LIMIT)
            while chunk:
                await fout.write(chunk)
                chunk = await resp.content.read(CHUNK_LIMIT)

    print("Downloaded to", fname)
    if sort and fname.endswith(".json"):
        await a_jq_post_process(fname)


def fetch_all(*, sort=True):
    """
    Synchronous function call that will update all eddb files to import.

    Args:
        sort: Use jq to sort the JSON files post download when True.
    """
    try:
        os.makedirs(EDDB_D)
    except OSError:
        pass

    # Cleanup files before writing
    to_remove = glob.glob(os.path.join(EDDB_D, '*.json'))
    if sort:
        to_remove += glob.glob(os.path.join(EDDB_D, '*.jsonl'))
        to_remove += glob.glob(os.path.join(EDDB_D, '*per_line'))
    for fname in to_remove:
        try:
            os.remove(fname)
        except OSError:
            print(f"Could not remove: {fname}")

    jobs = [fetch(url, os.path.join(EDDB_D, os.path.basename(url)), sort) for url in EDDB_URLS]
    asyncio.get_event_loop().run_until_complete(asyncio.gather(*jobs))
    print("\n\nAll files updated in", EDDB_D)


def load_commodities(fname, preload=True):
    """ Parse standard eddb dump commodities.json and enter into database. """
    # High level mapppings direct data flow by path in json
    # Mappings should be mutually exclusive
    # Format prefix, [(target_dictionary, key_in_dict), (target_dictionary, key_in_dict), ...]
    mappings = {
        'item.id': [('commodity', 'id')],
        'item.name': [('commodity', 'name')],
        'item.average_price': [('commodity', 'average_price')],
        'item.max_buy_price': [('commodity', 'max_buy_price')],
        'item.max_sell_price': [('commodity', 'max_sell_price')],
        'item.min_buy_price': [('commodity', 'min_buy_price')],
        'item.min_sell_price': [('commodity', 'min_sell_price')],
        'item.is_non_marketable': [('commodity', 'is_non_marketable')],
        'item.is_rare': [('commodity', 'is_rare')],
        'item.category.id': [('commodity', 'category_id'), ('commodity_cat', 'id')],
        'item.category.name': [('commodity_cat', 'name')],
    }

    print(f"Parsing commodities in {fname}")
    commodity, commodity_cat = {}, {}
    with open(fname, 'rb') as fin, cogdb.session_scope(cogdb.EDDBSession, autoflush=False) as eddb_session:
        for prefix, the_type, value in ijson.parse(fin):
            #  print(prefix, the_type, value)
            if (prefix, the_type, value) == ('item', 'end_map', None):
                # JSON Item terminated

                if not preload:
                    try:
                        eddb_session.query(CommodityCat).\
                            filter(CommodityCat.id == commodity_cat['id']).\
                            one()
                    except sqla_orm.exc.NoResultFound:  # Handle case of not existing record
                        commodity_cat_db = CommodityCat(**commodity_cat)
                        eddb_session.add(commodity_cat_db)
                        eddb_session.commit()

                try:
                    found = eddb_session.query(Commodity).\
                        filter(Commodity.id == commodity['id']).\
                        one()
                    found.update(**commodity)
                except sqla_orm.exc.NoResultFound:  # Handle case of not existing record
                    commodity_db = Commodity(**commodity)
                    eddb_session.add(commodity_db)

                #  Debug
                #  print('Commodity', commodity_db)
                #  print('Commodity Category', commodity_cat_db)

                commodity.clear()
                commodity_cat.clear()
                continue

            try:
                for dic, key in mappings[prefix]:
                    locals()[dic][key] = value
            except KeyError:
                pass

    print(f"FIN: Parsing commodities in {fname}")


def load_modules(fname, preload=True):
    """ Parse standard eddb dump modules.json and enter into database. """
    # High level mapppings direct data flow by path in json
    # Mappings should be mutually exclusive
    # Format prefix, [(target_dictionary, key_in_dict), (target_dictionary, key_in_dict), ...]
    mappings = {
        'item.id': [('module', 'id')],
        'item.name': [('module', 'name')],
        'item.rating': [('module', 'rating')],
        'item.price': [('module', 'price')],
        'item.ship': [('module', 'ship')],
        'item.weapon_mode': [('module', 'weapon_mode')],
        'item.class': [('module', 'size')],
        'item.mass': [('module', 'mass')],
        'item.group.id': [('module', 'group_id'), ('module_group', 'id')],
        'item.group.name': [('module_group', 'name')],
        'item.group.category': [('module_group', 'category')],
        'item.group.category_id': [('module_group', 'category_id')],
    }

    print(f"Parsing modules in {fname}")
    module_base = {'size': None, 'mass': None}
    module_group, module = {}, copy.deepcopy(module_base)
    with open(fname, 'rb') as fin, cogdb.session_scope(cogdb.EDDBSession, autoflush=False) as eddb_session:
        for prefix, the_type, value in ijson.parse(fin):
            #  print(prefix, the_type, value)
            if (prefix, the_type, value) == ('item', 'end_map', None):
                # JSON Item terminated

                if not preload:
                    try:
                        eddb_session.query(ModuleGroup).\
                            filter(ModuleGroup.id == module_group['id']).\
                            one()
                    except sqla_orm.exc.NoResultFound:  # Handle case of not existing record
                        module_group_db = ModuleGroup(**module_group)
                        eddb_session.add(module_group_db)
                        eddb_session.commit()

                try:
                    found = eddb_session.query(Module).\
                        filter(Module.id == module['id']).\
                        one()
                    found.update(**module)
                except sqla_orm.exc.NoResultFound:  # Handle case of not existing record
                    module_db = Module(**module)
                    eddb_session.add(module_db)

                # Debug
                #  print('Module', module_db)
                #  print('Module Group', module_group_db)

                module = copy.deepcopy(module_base)
                module_group.clear()
                continue

            try:
                for dic, key in mappings[prefix]:
                    locals()[dic][key] = value
            except KeyError:
                pass

    print(f"FIN: Parsing modules in {fname}")


def load_factions(fname, preload=True):
    """ Parse standard eddb dump modules.json and enter into database. """
    # High level mapppings direct data flow by path in json
    # Mappings should be mutually exclusive
    # Format prefix, [(target_dictionary, key_in_dict), (target_dictionary, key_in_dict), ...]
    mappings = {
        'item.id': [('faction', 'id')],
        'item.name': [('faction', 'name')],
        'item.home_system_id': [('faction', 'home_system_id')],
        'item.is_player_faction': [('faction', 'is_player_faction')],
        'item.updated_at': [('faction', 'updated_at')],
        'item.government_id': [('faction', 'government_id'), ('government', 'id')],
        'item.government': [('government', 'text')],
        'item.allegiance_id': [('faction', 'allegiance_id'), ('allegiance', 'id')],
        'item.allegiance': [('allegiance', 'text')],
    }

    print(f"Parsing factions in {fname}")
    faction, allegiance, government = {}, {}, {}
    with open(fname, 'rb') as fin, cogdb.session_scope(cogdb.EDDBSession, autoflush=False) as eddb_session:
        for prefix, the_type, value in ijson.parse(fin):
            #  print(prefix, the_type, value)
            if (prefix, the_type, value) == ('item', 'end_map', None):
                # JSON Item terminated, create/update

                if not preload:
                    try:
                        eddb_session.query(Allegiance).\
                            filter(Allegiance.id == allegiance['id']).\
                            one()
                    except sqla_orm.exc.NoResultFound:  # Handle case of not existing record
                        eddb_session.add(Allegiance(**allegiance))
                        eddb_session.commit()
                    try:
                        eddb_session.query(Government).\
                            filter(Government.id == government['id']).\
                            one()
                    except sqla_orm.exc.NoResultFound:  # Handle case of not existing record
                        eddb_session.add(Government(**government))
                        eddb_session.commit()

                try:
                    found = eddb_session.query(Faction).\
                        filter(Faction.id == faction['id']).\
                        one()
                    found.update(**faction)
                except sqla_orm.exc.NoResultFound:  # Handle case of not existing record
                    faction_db = Faction(**faction)
                    eddb_session.add(faction_db)

                # Debug
                #  print('Faction', faction_db)
                #  print('Allegiance', allegiance_db)
                #  print('Government', government_db)

                eddb_session.flush()
                faction.clear()
                allegiance.clear()
                government.clear()
                continue

            try:
                for dic, key in mappings[prefix]:
                    locals()[dic][key] = value
            except KeyError:
                pass

    print(f"FIN: Parsing factions in {fname}")


def load_systems(fname, power_ids):
    """ Parse standard eddb dump populated_systems.json and enter into database. """
    # High level mapppings direct data flow by path in json
    # Mappings should be mutually exclusive
    # Format prefix, [(target_dictionary, key_in_dict), (target_dictionary, key_in_dict), ...]
    mappings = {
        'item.id': [('system', 'id')],
        'item.ed_system_address': [('system', 'ed_system_id')],
        'item.updated_at': [('system', 'updated_at')],
        'item.name': [('system', 'name')],
        'item.population': [('system', 'population')],
        'item.needs_permit': [('system', 'needs_permit')],
        'item.edsm_id': [('system', 'edsm_id')],
        'item.security_id': [('system', 'security_id')],
        'item.primary_economy_id': [('system', 'primary_economy_id')],
        'item.power': [('system', 'power')],
        'item.power_state_id': [('system', 'power_state_id')],
        'item.controlling_minor_faction_id': [('system', 'controlling_minor_faction_id')],
        'item.x': [('system', 'x')],
        'item.y': [('system', 'y')],
        'item.z': [('system', 'z')],
    }

    print(f"Parsing systems in {fname}")
    system = {}
    with open(fname, 'rb') as fin, cogdb.session_scope(cogdb.EDDBSession, autoflush=False) as eddb_session:
        for prefix, the_type, value in ijson.parse(fin):
            #  print(prefix, the_type, value)

            if (prefix, the_type, value) == ('item', 'end_map', None):
                # JSON Item terminated
                system['power_id'] = power_ids[system.pop('power')]

                try:
                    found = eddb_session.query(System).\
                        filter(System.id == system['id']).\
                        one()
                    found.update(**system)
                except sqla_orm.exc.NoResultFound:  # Handle case of not existing record
                    system_db = System(**system)
                    eddb_session.add(system_db)

                # Debug
                #  print('System', system_db)

                eddb_session.flush()
                system.clear()
                continue

            try:
                for dic, key in mappings[prefix]:
                    locals()[dic][key] = value
            except KeyError:
                pass

    print(f"FIN: Parsing systems in {fname}")


def load_influences(fname, power_ids):
    """ Parse standard eddb dump populated_systems.json and enter into database. """
    # High level mapppings direct data flow by path in json
    # Mappings should be mutually exclusive
    # Format prefix, [(target_dictionary, key_in_dict), (target_dictionary, key_in_dict), ...]
    mappings = {
        'item.id': [('system', 'id')],
        'item.ed_system_address': [('system', 'ed_system_id')],
        'item.updated_at': [('system', 'updated_at')],
        'item.name': [('system', 'name')],
        'item.population': [('system', 'population')],
        'item.needs_permit': [('system', 'needs_permit')],
        'item.edsm_id': [('system', 'edsm_id')],
        'item.security_id': [('system', 'security_id')],
        'item.primary_economy_id': [('system', 'primary_economy_id')],
        'item.power': [('system', 'power')],
        'item.power_state_id': [('system', 'power_state_id')],
        'item.controlling_minor_faction_id': [('system', 'controlling_minor_faction_id')],
        'item.x': [('system', 'x')],
        'item.y': [('system', 'y')],
        'item.z': [('system', 'z')],
        'item.minor_faction_presences.item.influence': [('faction', 'influence')],
        'item.minor_faction_presences.item.minor_faction_id': [('faction', 'faction_id')],
        'item.minor_faction_presences.item.happiness_id': [('faction', 'happiness_id')],
    }

    print(f"Parsing influences in {fname}")
    faction_base = {'active_states': [], 'pending_states': [], 'recovering_states': []}
    system, factions, faction = {}, [], copy.deepcopy(faction_base)
    with open(fname, 'rb') as fin, cogdb.session_scope(cogdb.EDDBSession, autoflush=False) as eddb_session:
        for prefix, the_type, value in ijson.parse(fin):
            #  print(prefix, the_type, value)

            if (prefix, the_type, value) == ('item', 'map_key', 'minor_faction_presences'):
                factions = []
            elif (prefix, the_type) == ('item.minor_faction_presences.item', 'start_map'):
                faction = copy.deepcopy(faction_base)
            elif (prefix, the_type) == ('item.minor_faction_presences.item', 'end_map'):
                factions += [faction]

            elif (prefix, the_type) == ('item.minor_faction_presences.item.active_states.item.id', 'number'):
                faction['active_states'] += [value]
            elif (prefix, the_type) == ('item.minor_faction_presences.item.pending_states.item.id', 'number'):
                faction['pending_states'] += [value]
            elif (prefix, the_type) == ('item.minor_faction_presences.item.recovering_states.item.id', 'number'):
                faction['recovering_states'] += [value]

            elif (prefix, the_type, value) == ('item', 'end_map', None):
                # JSON Item terminated
                system['power_id'] = power_ids[system.pop('power')]

                states = []
                for faction in factions:
                    faction['is_controlling_faction'] = system['controlling_minor_faction_id'] == faction['faction_id']
                    faction['system_id'] = system['id']
                    faction['updated_at'] = system['updated_at']

                    for state_key, state_cls in FACTION_STATE_PAIRS:
                        for val in faction.pop(state_key):
                            states += [state_cls(system_id=system['id'], faction_id=faction['faction_id'], state_id=val, updated_at=system['updated_at'])]

                    try:
                        found = eddb_session.query(Influence).\
                            filter(
                                Influence.system_id == faction['system_id'],
                                Influence.faction_id == faction['faction_id']).\
                            one()
                        found.update(**faction)
                    except sqla_orm.exc.NoResultFound:  # Handle case of not existing record
                        influence_db = Influence(**faction)
                        eddb_session.add(influence_db)

                    # Debug
                    #  print('Influences', faction)

                eddb_session.add_all(states)
                eddb_session.flush()
                system.clear()
                continue

            try:
                for dic, key in mappings[prefix]:
                    locals()[dic][key] = value
            except KeyError:
                pass

    print(f"FIN: Parsing influences in {fname}")


def load_stations(fname, economy_ids, preload=True, refresh_all=False):
    """ Parse standard eddb dump stations.json and enter into database. """
    # High level mapppings direct data flow by path in json
    # Mappings should be mutually exclusive
    # Format prefix, [(target_dictionary, key_in_dict), (target_dictionary, key_in_dict), ...]
    mappings = {
        'item.id': [('station', 'id'), ('st_features', 'id')],
        'item.name': [('station', 'name')],
        'item.type_id': [('station', 'type_id'), ('st_type', 'id')],
        'item.distance_to_star': [('station', 'distance_to_star')],
        'item.is_planetary': [('station', 'is_planetary')],
        'item.max_landing_pad_size': [('station', 'max_landing_pad_size')],
        'item.controlling_minor_faction_id': [('station', 'controlling_minor_faction_id')],
        'item.system_id': [('station', 'system_id')],
        'item.updated_at': [('station', 'updated_at')],
        'item.has_blackmarket': [('st_features', 'blackmarket')],
        'item.has_carrier_administration': [('st_features', 'carrier_administration')],
        'item.has_carrier_vendor': [('st_features', 'carrier_vendor')],
        'item.has_commodities': [('st_features', 'commodities')],
        'item.has_docking': [('st_features', 'dock')],
        'item.has_interstellar_factors': [('st_features', 'interstellar_factors')],
        'item.has_market': [('st_features', 'market')],
        'item.has_material_trader': [('st_features', 'material_trader')],
        'item.has_outfitting': [('st_features', 'outfitting')],
        'item.has_rearm': [('st_features', 'rearm')],
        'item.has_refuel': [('st_features', 'refuel')],
        'item.has_repair': [('st_features', 'repair')],
        'item.has_shipyard': [('st_features', 'shipyard')],
        'item.has_technology_broker': [('st_features', 'technology_broker')],
        'item.has_universal_cartographics': [('st_features', 'universal_cartographics')],
        'item.type': [('st_type', 'text')],
    }

    print(f"Parsing stations in {fname}")
    station, st_features, st_type, st_econs = {}, {}, {}, []
    with open(fname, 'rb') as fin, cogdb.session_scope(cogdb.EDDBSession, autoflush=False) as eddb_session:
        for prefix, the_type, value in ijson.parse(fin):
            #  print(prefix, the_type, value)
            if (prefix, the_type) == ('item.economies.item', 'string'):
                st_econs += [value]

            if (prefix, the_type, value) == ('item', 'end_map', None):
                # JSON Item terminated

                try:
                    found = eddb_session.query(Station).\
                        filter(Station.id == station['id']).\
                        one()
                    found.update(**station)
                except sqla_orm.exc.NoResultFound:  # Handle case of not existing record
                    station_db = Station(**station)
                    eddb_session.add(station_db)

                try:
                    found = eddb_session.query(StationFeatures).\
                        filter(StationFeatures.id == station['id']).\
                        one()
                    found.update(**st_features)
                except sqla_orm.exc.NoResultFound:  # Handle case of not existing record
                    st_features_db = StationFeatures(**st_features)
                    eddb_session.add(st_features_db)

                if not preload:
                    try:
                        eddb_session.query(StationType).\
                            filter(StationType.id == st_type["id"]).\
                            one()
                    except sqla_orm.exc.NoResultFound:  # Handle case of not existing record
                        st_type_db = StationType(**st_type)
                        eddb_session.add(st_type_db)

                if refresh_all or preload:
                    primary = True
                    for econ in st_econs:
                        eddb_session.add(StationEconomy(id=station['id'], economy_id=economy_ids[econ], primary=primary))
                        primary = False

                # Debug
                #  print('Station', station_db)
                #  print('Station Features', st_features_db)
                #  print('Station Type', st_type_db)

                eddb_session.flush()
                station.clear()
                st_features.clear()
                st_type.clear()
                st_econs.clear()
                continue

            try:
                for dic, key in mappings[prefix]:
                    locals()[dic][key] = value
            except KeyError:
                pass

    print(f"FIN: Parsing stations in {fname}")


def make_parser():
    """
    Make the parser for command line usage.
    Designed to fetch and preload database before first run of bot.
    """
    parser = argparse.ArgumentParser(description="EDDB Importer")
    parser.add_argument('--yes', '-y', action="store_true",
                        help='Skip confirmation.')
    parser.add_argument('--dump', '-d', action="store_true",
                        help='Dump existing database to /tmp/eddb_dump')
    parser.add_argument('--jobs', '-j', type=int, default=os.cpu_count(),
                        help='The max number of jobs to run.')
    parser.add_argument('--no-preload', '-n', dest='preload', default=False, action="store_false",
                        help='Preload required database entries. Default: True')
    parser.add_argument('--empty', '-e', dest="empty", action="store_true",
                        help='Only empty out the tables of data. Implies preload.')
    parser.add_argument('--no-fetch', dest="fetch", action="store_false",
                        help='DO NOT fetch latest eddb dumps. Will use current.')
    parser.add_argument('--recreate-tables, -r', dest="recreate", action="store_true",
                        help='Recreate all EDDB tables and spy tables. Default: False')
    parser.add_argument('--full', dest="full", action="store_true",
                        help='Do a full reimport of slow changing info.')

    return parser


def chunk_jobs(initial_jobs, *, limit=5000):  # pragma: no cover
    """
    Take a list of jobs being prepared and take each fname inside the list
    and chunk that file out in the existing directory with limit lines per file.

    Args:
        initial_jobs: A list that contains a description of functions to be mapped onto args. It is of
        the form:
            [[func, fname, arg1, arg2],
             [func2, fname2, arg3,],
             ]

    Kwargs:
        limit: The limit of lines to chunk into each file.
    """
    jobs = {}
    for tup in initial_jobs:
        fname = tup[1]
        cog.util.chunk_file(fname, limit=limit)
        for globbed in sorted(glob.glob(fname + '_*')):
            jobs[globbed] = [tup[0], globbed] + tup[2:]

    return jobs


def pool_loader(args, job_limit=os.cpu_count()):  # pragma: no cover
    """
    Use a ProcessPoolExecutor to run jobs that parse parts of the json files
    and return objects to be inserted into the db.

    Assuming extras.fetch_eddb has been run with 'sort' option, then:
        - Chunk the large json files to create units of work.
        - Submit jobs to ProcessPoolExecutor to be completed.
        - Jobs return db objects to be inserted into the database.
        - Multiple rounds allow different parts to be processed once all parts in last
        round completed.

    Args:
        preload: The preload has already been done.
    """
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        classes = [FactionActiveState, FactionPendingState, FactionRecoveringState]
        if args.full:
            classes += [StationFeatures, StationEconomy]

        # Some classes are just easier to alwadefault=False, ys replace rather than update
        for cls in classes:
            eddb_session.query(cls).delete()

        # Map eceonomies back onto ids
        economy_ids = {x.text: x.id for x in eddb_session.query(Economy).all()}
        economy_ids[None] = economy_ids['None']
        # Map power names onto their ids
        power_ids = {x.text: x.id for x in eddb_session.query(Power).all()}
        power_ids[None] = power_ids["None"]

    # Top level map of functions, to the files that take them.
    # Things in later rounds have foreign keys to previous ones.
    rounds = {
        1: [
            [load_commodities, cog.util.rel_to_abs("data", "eddb", "commodities.json_per_line"), args.preload],
            [load_modules, cog.util.rel_to_abs("data", "eddb", "modules.json_per_line"), args.preload],
            [load_factions, cog.util.rel_to_abs("data", "eddb", "factions.json_per_line"), args.preload],
        ],
        2: [
            [load_systems, cog.util.rel_to_abs("data", "eddb", "systems_populated.json_per_line"), power_ids],
        ],
        3: [
            [load_influences, cog.util.rel_to_abs("data", "eddb", "systems_populated.json_per_line"), power_ids],
        ],
        4: [
            [load_stations, cog.util.rel_to_abs("data", "eddb", "stations.json_per_line"), economy_ids, args.preload, args.full],
        ],
    }

    try:
        with cfut.ProcessPoolExecutor(max_workers=job_limit) as pool:
            for key, limit in [(1, 4000), (2, 3000), (3, 2500), (4, 2500)]:
                jobs = chunk_jobs(rounds.get(key), limit=limit)
                futures = [pool.submit(*job) for job in jobs.values()]
                cfut.wait(futures)
    finally:
        match = cog.util.rel_to_abs('data', 'eddb', '*_line_0*')
        for fname in glob.glob(match):
            try:
                os.remove(fname)
            except OSError:
                print(f"Could not remove: {fname}")


def check_eddb_base_subclass(obj):
    """ Simple predicate, select sublasses of Base. """
    return inspect.isclass(obj) and obj.__name__ not in ["Base", "hybrid_method", "hybrid_property"]


def import_eddb(eddb_session):  # pragma: no cover
    """Confirm user choice and process any args early before pool loading.

    Args:
        eddb_session: A session onto the db.
    """
    args = make_parser().parse_args()

    if not args.yes:
        confirm = input("Reimport EDDB Database? (y/n) ").strip().lower()
        if not confirm.startswith('y'):
            print("Aborting.")
            sys.exit(0)

    if args.dump:
        fname = '/tmp/eddb_dump'
        print("Dumping to: " + fname)
        classes = [x[1] for x in inspect.getmembers(sys.modules[__name__], check_eddb_base_subclass)]
        cogdb.eddb.dump_db(eddb_session, classes, fname)
        sys.exit(0)

    if args.fetch:
        fetch_all(sort=True)

    if args.recreate:
        args.preload = True
        cogdb.eddb.recreate_tables()
        print('EDDB tables recreated.')
    elif args.empty:
        args.preload = True
        cogdb.eddb.empty_tables()
        print('EDDB tables empties.')

    if args.preload:
        cogdb.eddb.preload_tables(eddb_session)
        print('EDDB tables preloaded.')

    pool_loader(args)


def main():  # pragma: no cover
    """ Main entry. """
    start = datetime.datetime.utcnow()

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        import_eddb(eddb_session)
        #  Manually compute post import the initial SystemControl table
        cogdb.eddb.populate_system_controls(eddb_session)

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        print("Module count:", eddb_session.query(Module).count())
        print("Commodity count:", eddb_session.query(Commodity).count())
        obj_count = eddb_session.query(Faction).count()
        print("Faction count:", obj_count)
        print("Faction States count:",
              eddb_session.query(FactionActiveState).count()
              + eddb_session.query(FactionPendingState).count()
              + eddb_session.query(FactionRecoveringState).count())
        print("Influence count:", eddb_session.query(Influence).count())
        assert obj_count > 77500

        obj_count = eddb_session.query(System).count()
        print("Populated System count:", obj_count)
        assert obj_count > 20500
        obj_count = eddb_session.query(Station).count()
        print("Station count:", obj_count)
        assert obj_count > 200000
        print("Contested count:", eddb_session.query(SystemContestedV).count())
        print("Time taken:", datetime.datetime.utcnow() - start)
        #  main_test_area(eddb_session)


if __name__ == "__main__":  # pragma: no cover
    print('CLI is deprecated. Please use: python -m cogdb.dbi')
    sys.exit(0)

    # Tell user when not using most efficient backend.
    if ijson.backend != 'yajl2_c':
        print("Failed to set backend to yajl2_c. Please check that yajl is installed. Parsing may slow down.")
        print(f"Selected: {ijson.backend}")

    main()
