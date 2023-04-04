"""
Test importer for spansh data

1. Spansh data doesn't have fixed "keys", in order to be compatible will need
lookup ability to map the names of stations, factions and systems => EDDB IDs that are fixed.
2. Each load function here will operate on a "complete" loaded json object, no ijson.
3. Achieve parallelism by doing data decomposition and striping data to up to M files for M processes.
   Each process will open it's own session onto the db.

System Keys
['allegiance', 'bodies', 'bodyCount', 'controllingFaction', 'coords', 'date', 'factions', 'government',
'id64', 'name', 'population', 'powerState', 'powers', 'primaryEconomy', 'secondaryEconomy', 'security', 'stations']

Factions
['allegiance', 'government', 'influence', 'name', 'state']

Stations
['controllingFaction', 'controllingFactionState', 'distanceToArrival', 'government', 'id', 'landingPads',
'market', 'name', 'outfitting', 'primaryEconomy', 'services', 'shipyard', 'type', 'updateTime']
"""
import json
import logging
import subprocess as sub

from cogdb.eddb import (Faction, System, Station, StationType, StationEconomy, StationFeatures)
import cog.util


FACTION_MAPF = cog.util.rel_to_abs('data', 'factionMap.json')
SYSTEM_MAPF = cog.util.rel_to_abs('data', 'systemMap.json')
STATION_MAPF = cog.util.rel_to_abs('data', 'stationMap.json')
SYSTEMS_CSV = "/media/starcraftman/8TBStorage/EDDBBackup/systems.csv"


def load_commodities(session, data):
    pass


def load_modules(session, date):
    pass


def load_factions(session, data):
    pass


def load_system(session, data):
    pass


def load_stations(session, data):
    pass


def update_system_map(missing_systems):
    known_systems = None
    with open(SYSTEM_MAPF, 'r', encoding='utf-8') as fin:
        known_systems = json.load(fin)

    for system_name in missing_systems:
        try:
            found = sub.check_output(['grep', system_name, SYSTEMS_CSV]).decode()
            system_id = int(found[found.index(':') + 1:].split(',')[0])
            known_systems[system_name] = system_id
        except (sub.CalledProcessError, IndexError, ValueError):
            logging.getLogger(__name__).error("System not found in systems.csv")

    with open(SYSTEM_MAPF, 'w', encoding='utf-8') as fout:
        json.dump(known_systems, fout)


def update_station_map(missing_stations):
    known_stations = None
    with open(STATION_MAPF, 'r', encoding='utf-8') as fin:
        known_stations = json.load(fin)
    known_ids = list(sorted(known_stations.values()))
    available_ids = set(range(1,

    for system_name in missing_stations:
        pass

    with open(SYSTEM_MAPF, 'w', encoding='utf-8') as fout:
        json.dump(known_systems, fout)


def verify_galaxy_ids(dump_path):
    """
    """
    missing_systems, missing_factions, missing_stations = [], [], []
    with open(SYSTEM_MAPF, 'r', encoding='utf-8') as fin:
        known_systems = json.load(fin)
    with open(FACTION_MAPF, 'r', encoding='utf-8') as fin:
        known_factions = json.load(fin)
    with open(STATION_MAPF, 'r', encoding='utf-8') as fin:
        known_stations = json.load(fin)

    with open(dump_path, 'r', encoding='utf-8') as fin:
        for line in fin:
            try:
                data = json.loads(line)
                if data['name'] not in known_systems:
                    missing_systems += [data['name']]
                for faction in data.get('factions', []):
                    if faction['name'] not in known_factions:
                        missing_factions += [faction['name']]
                for station in data.get('stations', []):
                    if station['name'] not in known_stations:
                        missing_stations += [station['name']]
                for body in data.get('bodies', []):
                    for station in body.get('stations', []):
                        if station['name'] not in known_stations:
                            missing_stations += [station['name']]
            except json.JSONDecodeError:
                pass

    return missing_factions, missing_systems, missing_stations
