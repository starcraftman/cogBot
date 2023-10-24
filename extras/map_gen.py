"""
This is a tool to seed the initial system, faction and station maps.
The initial ids are assigned based on data assembled from eddb dumps prior to closure.
These are the internal ids that will be assigned to created objects to help parallelize the processing of spansh.

Files in the IDS_ROOT folder ending in '.eddb' are seed information for the equivalent 'Map.json' files.
"""
import json

import cogdb.spansh
import cog.util
from cogdb.spansh import SEP, IDS_ROOT


def gen_system_faction_maps():
    """
    Based on the systems.eddb and factions.eddb files create initial maps
    assigning the system names and faction names onto their object IDs.
    Note that these assignments are ONTO, no name can have same ID.

    Returns: The system_map created as it was written out.
    """
    system_map = {}
    with open(f'{IDS_ROOT}/systems.eddb', 'r', encoding='utf-8') as fin:
        for line in fin:
            name, sid = line.rstrip().split(SEP)
            system_map[name] = int(sid)

        with open(f'{IDS_ROOT}/systemMap.json', 'w', encoding='utf-8') as fout:
            json.dump(system_map, fout, indent=2, sort_keys=True)

    faction_map = {}
    with open(f'{IDS_ROOT}/factions.eddb', 'r', encoding='utf-8') as fin:
        for line in fin:
            name, fid = line.rstrip().split(SEP)
            faction_map[name] = int(fid)

        with open(f'{IDS_ROOT}/factionMap.json', 'w', encoding='utf-8') as fout:
            json.dump(faction_map, fout, indent=2, sort_keys=True)

    return system_map


def gen_station_map(system_map):
    """
    Following the same logic as gen_system_faction_maps, using the stations.eddb
    base as input create a mapping dictionary that is ONTO that maps the station keys
    to their ID. Here a station key follows the logic of station_key function.

    Note: Two maps are created, one with just carriers called "carrierMap.json"
          the other with "stationMap.json" includes all stations AND carriers

    Args:
        system_map: The system_map returned by gen_system_faction_maps
    """
    reverse_systems = {value: key for key, value in system_map.items()}

    station_map, carrier_map = {}, {}
    with open(f'{IDS_ROOT}/stations.eddb', 'r', encoding='utf-8') as fin:
        for line in fin:
            sys_id, stat_name, stat_id = line.rstrip().split(SEP)
            sys_id = int(sys_id)
            stat_id = int(stat_id)

            try:
                key, carrier = station_key(system_id=sys_id, station_name=stat_name, reverse_systems=reverse_systems)
            except KeyError:
                print(f"Not found in reverse map: {stat_id} {SEP} {stat_name} {SEP} {sys_id}")

            if carrier:
                carrier_map[key] = stat_id
            else:
                station_map[key] = stat_id

    with open(f'{IDS_ROOT}/carrierMap.json', 'w', encoding='utf-8') as fout:
        json.dump(carrier_map, fout, indent=2, sort_keys=True)

    station_map.update(carrier_map)
    with open(f'{IDS_ROOT}/stationMap.json', 'w', encoding='utf-8') as fout:
        json.dump(station_map, fout, indent=2, sort_keys=True)


def station_key(*, system_id, station_name, reverse_systems):
    """
    A station key works as follows:
        If the station is a player carrier, key is the name.
        If the station is not a player carrier, key is system_name||station_name.

    Args:
        system_id: The id of the system the station is in, used for non-carrier stations.
        station_name: The name of the station.
        reverse_systems: A reverse map of ids onto system names.

    Returns: A unique string identifying the station generated based on system id and station name.
    """
    is_carrier = cog.util.is_a_carrier(station_name)
    key = station_name
    if not is_carrier:
        system_name = reverse_systems[system_id]
        key = f"{system_name}{SEP}{station_name}"

    return key, is_carrier


def merge_prison_stations(known_systems):
    """
    Merge in the extra prison data that is not contained within eddb data.

    Args:
        known_systems: The map of known systems onto their ids.
    """
    with open(f'{IDS_ROOT}/stationMap.json', 'r', encoding='utf-8') as fin:
        known_stations = eval(fin.read())

    known_system_ids = list(known_systems.values())
    known_station_ids = list(sorted(known_systems.values()))
    next_id = known_station_ids[-1] + 1
    with open(f'{IDS_ROOT}/prisons.eddb', 'r', encoding='utf-8') as fin:
        first = True
        for line in fin:
            if first:
                first = False
                continue

            _, station_name, sys_id, sys_name = line.rstrip().split(SEP)
            sys_id = int(sys_id)
            if sys_name not in known_systems:
                if sys_id not in known_system_ids:
                    known_systems[sys_name] = sys_id
                    known_system_ids += [sys_id]
                    print(f'System: {sys_name} => {sys_id}')
                else:
                    print(f"Error: {sys_id} for {sys_name} already assigned")

            key = f"{sys_name}{SEP}{station_name}"
            station_id = next_id
            next_id += 1
            print(f'Station assigned: {key} => {station_id}')
            known_stations[key] = station_id

    with open(f'{IDS_ROOT}/systemMap.json', 'w', encoding='utf-8') as fout:
        json.dump(known_systems, fout, indent=2, sort_keys=True)
    with open(f'{IDS_ROOT}/stationMap.json', 'w', encoding='utf-8') as fout:
        json.dump(known_stations, fout, indent=2, sort_keys=True)


def init_eddb_maps():
    """
    Initialize the faction, system and station maps from static information
    based on eddb.io.
    Then merge in the specific prison station information.
    """
    system_map = gen_system_faction_maps()
    gen_station_map(system_map)
    merge_prison_stations(system_map)


def check_maps_for_collisions():
    """
    Verify there are no collions in the IDs assigned to the keys of the maps.
    These maps should be bidirectional essentially.
    Prints errors and collisions to stdout.
    """
    seen = set()
    for name in ['station', 'carrier', 'faction', 'system']:
        print("Examining:", name)
        with open(f'data/ids/{name}Map.json', 'r', encoding='utf-8') as fin:
            found = eval(fin.read())
            size_diff = len(list(found.values())) - len(set(found.values()))

            if size_diff:
                print("Dupes found", size_diff)
                for key in found:
                    if found[key] in seen:
                        print(f"Duplicate id collision: {key} => {found[key]}")
                    else:
                        seen.add(found[key])
            else:
                print("No dupes")


if __name__ == "__main__":
    check_maps_for_collisions()
