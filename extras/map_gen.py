"""
This is an extra tool to use a series of initial eddb maps
to see the normal faction system and stations maps.
Bit of a niche case.
"""
import json
import re

import cogdb.spansh
import cog.util
from cogdb.spansh import SEP, IDS_ROOT, is_a_carrier


def clean_eddb_maps():
    """
    Create clean base systemMap and factionMap files based on initial
    static maps based on eddb data.
    """
    system_map = {}
    with open(f'{IDS_ROOT}/systems.eddb', 'r', encoding='utf-8') as fin:
        for line in fin:
            name, id = line.rstrip().split(SEP)
            system_map[name] = int(id)

    with open(f'{IDS_ROOT}/systemMap.json', 'w', encoding='utf-8') as fout:
        json.dump(system_map, fout, indent=2, sort_keys=True)
    with open(f'{IDS_ROOT}/systemNewMap.json', 'w', encoding='utf-8') as fout:
        json.dump({}, fout, indent=2, sort_keys=True)

    faction_map = {}
    with open(f'{IDS_ROOT}/factions.eddb', 'r', encoding='utf-8') as fin:
        for line in fin:
            name, id = line.rstrip().split(SEP)
            faction_map[name] = int(id)

    with open(f'{IDS_ROOT}/factionMap.json', 'w', encoding='utf-8') as fout:
        json.dump(faction_map, fout, indent=2, sort_keys=True)
    with open(f'{IDS_ROOT}/factionNewMap.json', 'w', encoding='utf-8') as fout:
        json.dump({}, fout, indent=2, sort_keys=True)

    return system_map


def clean_station_map(system_map):
    """
    Create clean base station, carrier and onlyStation maps.

    Args:
        system_map: The system_map returned by clean_eddb_maps
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
    with open(f'{IDS_ROOT}/carrierNewMap.json', 'w', encoding='utf-8') as fout:
        json.dump({}, fout, indent=2, sort_keys=True)
    with open(f'{IDS_ROOT}/onlyStationMap.json', 'w', encoding='utf-8') as fout:
        json.dump(station_map, fout, indent=2, sort_keys=True)
    with open(f'{IDS_ROOT}/onlyStationNewMap.json', 'w', encoding='utf-8') as fout:
        json.dump({}, fout, indent=2, sort_keys=True)

    station_map.update(carrier_map)
    with open(f'{IDS_ROOT}/stationMap.json', 'w', encoding='utf-8') as fout:
        json.dump(station_map, fout, indent=2, sort_keys=True)
    with open(f'{IDS_ROOT}/stationNewMap.json', 'w', encoding='utf-8') as fout:
        json.dump({}, fout, indent=2, sort_keys=True)


def station_key(*, system_id, station_name, reverse_systems):
    """
    A station key works as follows:
        If the station is a player carrier, key is the name.
        If the station is not a player carrier, key is system_name||station_name.
    """
    is_carrier = is_a_carrier(station_name)
    key = station_name
    if not is_carrier:
        system_name = reverse_systems[system_id]
        key = f"{system_name}{SEP}{station_name}"

    return key, is_carrier


def merge_prison():
    """
    Merge in the prison data fixed ids for systems and stations.
    """
    with open(f'{IDS_ROOT}/systemMap.json', 'r', encoding='utf-8') as fin:
        known_systems = eval(fin.read())
    with open(f'{IDS_ROOT}/stationMap.json', 'r', encoding='utf-8') as fin:
        known_stations = eval(fin.read())
    with open(f'{IDS_ROOT}/onlyStationMap.json', 'r', encoding='utf-8') as fin:
        known_only_stations = eval(fin.read())

    known_system_ids = list(known_systems.values())
    known_station_ids = list(sorted(known_systems.values()))
    available = set(range(1, known_station_ids[-1] + 1000)) - set(known_station_ids)
    available = list(sorted(available, reverse=True))
    with open(f'{IDS_ROOT}/prisons.eddb', 'r', encoding='utf-8') as fin:
        first = True
        for line in fin:
            if first:
                first = False
                continue

            station_id, station_name, sys_id, sys_name = line.rstrip().split(SEP)
            station_id = int(station_id)
            sys_id = int(sys_id)
            if sys_name not in known_systems:
                if sys_id not in known_system_ids:
                    known_systems[sys_name] = sys_id
                    known_system_ids += [sys_id]
                    print(f'System: {sys_name} => {sys_id}')
                else:
                    print(f"Error: {sys_id} for {sys_name} already assigned")

            #
            key = f"{sys_name}{SEP}{station_name}"
            if key not in known_stations:
                if station_id not in known_station_ids:
                    print(f'Station: {key} => {station_id}')
                    known_stations[key] = station_id
                    known_station_ids += [station_id]
                    known_only_stations[key] = station_id
                else:
                    nextid = available.pop()
                    print(f'Station: {key} => {nextid}')
                    known_stations[key] = nextid
                    known_station_ids += [nextid]
                    known_only_stations[key] = nextid

    with open(f'{IDS_ROOT}/systemMap.json', 'w', encoding='utf-8') as fout:
        json.dump(known_systems, fout, indent=2, sort_keys=True)
    with open(f'{IDS_ROOT}/stationMap.json', 'w', encoding='utf-8') as fout:
        json.dump(known_stations, fout, indent=2, sort_keys=True)
    with open(f'{IDS_ROOT}/onlyStationMap.json', 'w', encoding='utf-8') as fout:
        json.dump(known_only_stations, fout, indent=2, sort_keys=True)


def main():
    """ Main entry. """
    sys_map = clean_eddb_maps()
    clean_station_map(sys_map)
    merge_prison()


if __name__ == "__main__":
    main()
