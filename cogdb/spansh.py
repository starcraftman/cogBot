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

Factions objects
['allegiance', 'government', 'influence', 'name', 'state']

Stations objects
['controllingFaction', 'controllingFactionState', 'distanceToArrival', 'government', 'id', 'landingPads',
'market', 'name', 'outfitting', 'primaryEconomy', 'services', 'shipyard', 'type', 'updateTime']
"""
import datetime
import json
import os

from cogdb.eddb import (
    Allegiance, Economy, Faction, Influence, FactionState, Government, Power, PowerState,
    Security, System, Station, StationType, StationEconomy, StationFeatures
)
import cog.util


TIME_STRP = "%Y-%m-%d %H:%M:%S"
FACTION_MAPF = cog.util.rel_to_abs('data', 'factionMap.json')
SYSTEM_MAPF = cog.util.rel_to_abs('data', 'systemMap.json')
STATION_MAPF = cog.util.rel_to_abs('data', 'stationMap.json')
STATION_NEW_MAPF = cog.util.rel_to_abs('data', 'stationMapNew.json')
SYSTEMS_CSV = os.path.join(cog.util.CONF.paths.eddb_store, 'systems.csv')
GALAXY_JSON = os.path.join(cog.util.CONF.paths.eddb_store, 'galaxy_stations.json')
SYSTEMS_CSV_INFO = [
    {'key': 'id', 'type': 'int'},
    {'key': 'edsm_id', 'type': 'int'},
    {'key': 'name', 'type': 'string'},
    {'key': 'x', 'type': 'float'},
    {'key': 'y', 'type': 'float'},
    {'key': 'z', 'type': 'float'},
    {'key': 'population', 'type': 'int'},
    False,
    False,
    False,
    False,
    False,
    {'key': 'security_id', 'type': 'int'},
    False,
    {'key': 'primary_economy_id', 'type': 'int'},
    False,
    {'key': 'power', 'type': 'string'},
    False,
    {'key': 'power_state_id', 'type': 'int'},
    {'key': 'needs_permit', 'type': 'int'},
    {'key': 'updated_at', 'type': 'str'},
    False,
    False,
    {'key': 'controlling_minor_faction_id', 'type': 'int'},
    False,
    False,
    False,
    {'key': 'ed_system_id', 'type': 'int'},
]


def date_to_timestamp(text):
    """ Parse a given UTC date from spansh dumps to timestamp. """
    try:
        if text[-3:] == '+00':
            text = text[:-3]
        parsed_time = datetime.datetime.strptime(text, TIME_STRP)
        return parsed_time.replace(tzinfo=datetime.timezone.utc).timestamp()
    except ValueError:
        return None

## Possible Station Features beyond existing
#  ['Apex Interstellar',
 #  'Autodock',
 #  'Bartender',
 #  'Black Market',
 #  'Contacts',
 #  'Crew Lounge',
 #  'Dock',
 #  'Fleet Carrier Administration',
 #  'Fleet Carrier Fuel',
 #  'Fleet Carrier Management',
 #  'Fleet Carrier Vendor',
 #  'Flight Controller',
 #  'Frontline Solutions',
 #  'Interstellar Factors Contact',
 #  'Livery',
 #  'Market',
 #  'Material Trader',
 #  'Missions',
 #  'Missions Generated',
 #  'On Dock Mission',
 #  'Outfitting',
 #  'Pioneer Supplies',
 #  'Powerplay',
 #  'Redemption Office',
 #  'Refuel',
 #  'Repair',
 #  'Restock',
 #  'Search and Rescue',
 #  'Shipyard',
 #  'Shop',
 #  'Social Space',
 #  'Station Menu',
 #  'Station Operations',
 #  'Technology Broker',
 #  'Tuning',
 #  'Universal Cartographics',
 #  'Vista Genomics',
 #  'Workshop']
def parse_station_features(features, *, station_id, updated_at):
    """
    Parse and return a StationFeatures object based on features found in
    spansh specific station services section.

    Args:
        features: The list of station services from spansh.
        station_id: The actual id of the station in question.
        update_at: The timestamp to assign to this feature set.

    Returns: A StationFeatures object.
    """
    kwargs = {
        'id': station_id,
        'carrier_administration': 'Fleet Carrier Administration' in features,
        'carrier_vendor': 'Fleet Carrier Vendor' in features,
        'blackmarket': 'Black Market' in features,
        'material_trader': 'Material Trader' in features,
        'commodities': 'Shop' in features,  # Unsure of mapping
        'dock': 'Dock' in features,
        'interstellar_factors': 'Interstellar Factors Contact' in features,
        'market': 'Market' in features,
        'outfitting': 'Outfitting' in features,
        'shipyard': 'Shipyard' in features,
        'rearm': 'Restock' in features,
        'refuel': 'Refuel' in features,
        'repair': 'Repair' in features,
        'technology_broker': 'Technology Broker' in features,
        'universal_cartographics': 'Universal Cartographics' in features,
        'updated_at': updated_at,
    }

    return StationFeatures(**kwargs)


# TODO: Low priority
def load_commodities(session, data):
    pass


# TODO: Low priority
def load_modules(session, date):
    pass


def load_system(*, data, mapped):
    """
    Load the system information in a single data object from spansh.
    To be specific, data should be a complete line from the galaxy_stations.json

    Args:
        data: A complex dictionary nesting all the information of a single system from spansh.
        mapped: A dictionary of mappings to map down constants to their integer IDs.

    Raises:
        KeyError - The System in question is not of interest, stop parsing.

    Returns: A cogdb.eddb.System object representing the system in question.
    """
    data['updated_at'] = date_to_timestamp(data['date'])
    power_id = None
    if len(data['powers']) == 1:
        power_id = mapped['power'][data['powers'][0]]

    controlling_faction = data['controllingFaction']['name']
    kwargs = {
        'id': mapped['systems'][data['name']],
        'ed_system_id': data['id64'],
        'controlling_minor_faction_id': mapped['factions'][controlling_faction],
        'name': data['name'],
        'population': data['population'],
        'power_id': power_id,
        'power_state_id': mapped['power_state'][data['powerState']],
        'security_id': mapped['security'][data['security']],
        'primary_economy_id': mapped['economy'][data['primaryEconomy']],
        'secondary_economy_id': mapped['economy'][data['secondaryEconomy']],
        'x': data['coords']['x'],
        'y': data['coords']['y'],
        'z': data['coords']['z'],
        'updated_at': data['updated_at'],
    }

    return System(**kwargs)


# TODO: Missing Happiness
def load_factions(*, data, mapped, system_id):
    """
    Load the faction and influence information in a single data object from spansh.
    To be specific, data should be a complete line from the galaxy_stations.json

    Args:
        data: A complex dictionary nesting all the information of a single system from spansh.
        mapped: A dictionary of mappings to map down constants to their integer IDs.
        system_id: The ID of the system of the factions.

    Raises:
        KeyError - The System in question is not of interest, stop parsing.

    Returns: (factions, infs)
        factions - A list of cogdb.eddb.Faction objects
        infs - A list of cogdb.eddb.Influence objects
    """
    if 'factions' not in data or not data['factions']:
        return

    if 'updated_at' not in data:
        data['updated_at'] = date_to_timestamp(data['date'])

    factions = []
    infs = []
    for kwargs in data['factions']:
        kwargs['id'] = mapped['factions'][kwargs['name']]
        kwargs['updated_at'] = data['updated_at']
        for key in ['allegiance', 'government']:
            try:
                value = kwargs[key]
                kwargs[f'{key}_id'] = mapped[key][value]
            except KeyError:
                kwargs[f'{key}_id'] = None

        try:
            controlling_faction = kwargs['name'] == data['controllingFaction']['name']
        except KeyError:
            controlling_faction = None
        infs += [Influence(
            faction_id=kwargs['id'],
            system_id=system_id,
            happiness_id=None,
            influence=kwargs['influence'],
            updated_at=kwargs['updated_at'],
            is_controlling_faction=controlling_faction,
        )]
        del kwargs['influence']
        factions += [Faction(**kwargs)]

    return factions, infs


def load_stations(*, data, mapped, system_id):
    """
    Load the station information in a single data object from spansh.
    To be specific, data should be a complete line from the galaxy_stations.json

    Args:
        data: A complex dictionary nesting all the information of a single system from spansh.
        mapped: A dictionary of mappings to map down constants to their integer IDs.
        system_id: The ID of the system of the factions.

    Returns: (stations, station_fts, station_econs, station_types)
        stations - A list of cogdb.eddb.Station objects.
        station_fts - A list of cogdb.eddb.StationFeatures objects.
        station_econs - A list of cogdb.eddb.StationEconomy objects.
    """
    stations = []
    station_fts = []
    station_econs = []

    if 'updated_at' not in data:
        data['updated_at'] = date_to_timestamp(data['date'])

    for station in data['stations']:
        updated_at = date_to_timestamp(station['updateTime'])

        max_pad = 'S'
        if 'landingPads' in station and 'large' in station['landingPads']:
            max_pad = 'L'
        elif 'landingPads' in station and 'medium' in station['landingPads']:
            max_pad = 'M'

        station_econs += [
            StationEconomy(
                id=station['id'],
                economy_id=mapped['economy'][station['primaryEconomy']],
                primary=True
            )
        ]
        station_fts += [parse_station_features(station['services'], station_id=station['id'], updated_at=updated_at)]
        kwargs = {
            'id': station['id'],
            'type_id': mapped['station_type'][station['type']],
            'system_id': system_id,
            'name': station['name'],
            'controlling_minor_faction_id': mapped['factions'].get(station['controllingFaction']),
            'distance_to_star': station['distanceToArrival'],
            'max_landing_pad_size': max_pad,
            'updated_at': updated_at,
        }
        stations += [Station(**kwargs)]

    return stations, station_fts, station_econs


def load_bodies(*, data, mapped, system_id):
    """
    Load the station information attached to bodies in a single data object from spansh.
    To be specific, data should be a complete line from the galaxy_stations.json

    Args:
        data: A complex dictionary nesting all the information of a single system from spansh.
        mapped: A dictionary of mappings to map down constants to their integer IDs.
        system_id: The ID of the system of the factions.

    Returns: (stations, station_fts, station_econs, station_types)
        stations - A list of cogdb.eddb.Station objects.
        station_fts - A list of cogdb.eddb.StationFeatures objects.
        station_econs - A list of cogdb.eddb.StationEconomy objects.
    """
    stations, station_fts, station_econs = [], [], []

    if 'updated_at' not in data:
        data['updated_at'] = date_to_timestamp(data['date'])

    for body in data['bodies']:
        if body['stations']:
            body['updated_at'] = data['updated_at']
            parts = load_stations(data=body, mapped=mapped, system_id=system_id)
            stations += parts[0]
            station_fts += parts[1]
            station_econs += parts[2]

    return stations, station_fts, station_econs


def split_csv_line(line):
    """
    Strings can have commas in them so can't safely split unless outside a double quote string.
    Otherwise split on every comma and empty values will be set to empty string ('')

    Args:
        line: A line of text.

    Returns: A list of the fields of the CSV line. List is guaranteed to match num columns.
    """
    parts = []
    part = ''
    in_string = False
    for chara in line:
        if not in_string and chara == ',':
            parts += [part]
            part = ''
            continue
        if chara == '"':
            in_string = not in_string

        part += chara

    if part:
        parts += [part]

    return parts


def systems_csv_importer(eddb_session, csv_path):
    """
    A generator that will parse every line in EDDB.io systems.csv.
    For each line return a fully parsed cogdb.eddb.System object ready for database.

    Args:
        eddb_session: A session onto the EDDB.
        csv_path: The path to the systems.csv file.

    Returns: Continues to yield individual System objects until file exhausted.
    """
    power_map = {x.text: x.id for x in eddb_session.query(Power)}
    with open(csv_path, 'r', encoding='utf-8') as fin:
        for line in fin:
            items = list(reversed(split_csv_line(line)))
            if items[-1] == 'id':  # Guard for first line
                continue

            kwargs = {}
            for info in SYSTEMS_CSV_INFO:
                item = items.pop()
                if info:
                    if info['type'] == 'int':
                        try:
                            item = int(item)
                        except ValueError:
                            item = 0
                    elif info['type'] == 'float':
                        item = float(item)
                    elif info['type'] == 'string':
                        item = item.replace('"', '')

                    kwargs[info['key']] = item

            try:
                kwargs['power_id'] = power_map[kwargs['power']]
            except KeyError:
                kwargs['power_id'] = 0
            finally:
                del kwargs['power']

            yield System(**kwargs)


def update_name_map(missing_names, *, known_fname, new_fname):
    """
    Provide a means of updating the static name -> id assignments for
    systems, stations and factions. These maps are needed as spansh does not assign internal static IDs.

    Args:
        missing_names: The names that are missing from the known_fname json mapping names -> IDs
        known_fname: The filename contining the map of all names -> IDs, based on EDDB.io
                     This file will be updated with new names beyond those from eddb
        known_fname: The filename contining the map of all new names -> IDs, based on additions
                     This file will show only those added after eddb.io
    """
    known_names = None
    with open(known_fname, 'r', encoding='utf-8') as fin:
        known_names = json.load(fin)
    with open(new_fname, 'r', encoding='utf-8') as fin:
        new_names = json.load(fin)
    known_ids = list(sorted(known_names.values()))
    available = set(range(1, int(known_ids[-1]) + 2 * len(missing_names))) - set(known_ids)
    available = list(sorted(available))

    for name in missing_names:
        new_id = available[0]
        available = available[1:]
        new_names[name] = new_id
        known_names[name] = new_id

    with open(known_fname, 'w', encoding='utf-8') as fout:
        json.dump(known_names, fout)
    with open(new_fname, 'w', encoding='utf-8') as fout:
        json.dump(new_names, fout)


def verify_galaxy_ids(dump_path):
    """
    Verify the IDs of systems, factions and stations in galaxy_stations.json file.

    Args:
        dump_path: The file path to the json.

    Returns: (missing_systems, missing_factions, missing_stations)
        missing_systems: The names of systems that need static IDs
        missing_factions: The names of factions that need static IDs
        missing_stations: The names of stations that need static IDs
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


def eddb_maps(eddb_session):
    """
    Create static mappings of the names of constants onto their IDs.
    This will also populate mappings for systems, stations and factions based on EDDB IDs.
    This will also store some extra specific mappings from names onto eddb names.

    Args:
        eddb_session: A session onto EDDB.

    Returns: A dictionary of mappings, the keys are the type of constant i.e. 'power' or 'security'.
    """
    mapped_cls = [
        ['allegiance', Allegiance],
        ['faction_state', FactionState],
        ['government', Government],
        ['economy', Economy],
        ['power', Power],
        ['power_state', PowerState],
        ['security', Security],
        ['station_type', StationType],
    ]
    mapped = {}
    for key, cls in mapped_cls:
        mapped[key] = {x.text: x.id for x in eddb_session.query(cls)}
        mapped[key]['None'] = None
        mapped[key][None] = None

    # Specific spansh name aliases
    mapped['power_state']['Controlled'] = mapped['power_state']['Control']
    mapped['station_type']['Drake-Class Carrier'] = mapped['station_type']['Fleet Carrier']
    mapped['station_type']['Mega ship'] = mapped['station_type']['Megaship']
    mapped['station_type']['Outpost'] = mapped['station_type']['Civilian Outpost']
    mapped['station_type']['Settlement'] = mapped['station_type']['Odyssey Settlement']

    with open(SYSTEM_MAPF, 'r', encoding='utf-8') as fin:
        mapped['systems'] = json.load(fin)
    with open(STATION_MAPF, 'r', encoding='utf-8') as fin:
        mapped['stations'] = json.load(fin)
    with open(FACTION_MAPF, 'r', encoding='utf-8') as fin:
        mapped['factions'] = json.load(fin)

    return mapped


def collect_types():
    """
    Collect a set of constants that might differ from those used by EDDB.io
    Prints out the found information directly to stdout.
    """
    pstates, ships, faction_states = set(), set(), set()
    station_types, station_services = set(), set()
    with open(GALAXY_JSON, 'r', encoding='utf-8') as fin:
        for line in fin:
            if line.startswith('[') or line.startswith(']'):
                continue

            line = line.strip()
            if line[-1] == ',':
                line = line[:-1]
            data = json.loads(line)

            if 'powerState' in data:
                pstates.add(data['powerState'])
            if 'stations' in data:
                for station in data['stations']:
                    station_types.add(station['type'])
                    if 'shipyard' in station:
                        if 'ships' in station['shipyard']:
                            for ship in station['shipyard']['ships']:
                                ships.add(ship['name'])
                    for service in station['services']:
                        station_services.add(service)
            else:
                print('Missing stations')

            if 'factions' in data:
                for faction in data['factions']:
                    faction_states.add(faction['state'])
            else:
                pass

    print("Found services")
    __import__('pprint').pprint(list(sorted(station_services)))
    print("Found types")
    __import__('pprint').pprint(list(sorted(station_types)))
    print("Found faction states")
    __import__('pprint').pprint(list(sorted(faction_states)))
    print("Found ships")
    __import__('pprint').pprint(list(sorted(ships)))
    print("Found power states")
    __import__('pprint').pprint(list(sorted(pstates)))


# DO NOT DELETE, cached information from above collect_types
#  Found services
#  ['Apex Interstellar',
 #  'Autodock',
 #  'Bartender',
 #  'Black Market',
 #  'Contacts',
 #  'Crew Lounge',
 #  'Dock',
 #  'Fleet Carrier Administration',
 #  'Fleet Carrier Fuel',
 #  'Fleet Carrier Management',
 #  'Fleet Carrier Vendor',
 #  'Flight Controller',
 #  'Frontline Solutions',
 #  'Interstellar Factors Contact',
 #  'Livery',
 #  'Market',
 #  'Material Trader',
 #  'Missions',
 #  'Missions Generated',
 #  'On Dock Mission',
 #  'Outfitting',
 #  'Pioneer Supplies',
 #  'Powerplay',
 #  'Redemption Office',
 #  'Refuel',
 #  'Repair',
 #  'Restock',
 #  'Search and Rescue',
 #  'Shipyard',
 #  'Shop',
 #  'Social Space',
 #  'Station Menu',
 #  'Station Operations',
 #  'Technology Broker',
 #  'Tuning',
 #  'Universal Cartographics',
 #  'Vista Genomics',
 #  'Workshop']
#  Found types
#  ['Asteroid base',
 #  'Coriolis Starport',
 #  'Drake-Class Carrier',
 #  'Mega ship',
 #  'Ocellus Starport',
 #  'Orbis Starport',
 #  'Outpost',
 #  'Planetary Outpost',
 #  'Planetary Port',
 #  'Settlement']
#  Found faction states
#  ['Blight',
 #  'Boom',
 #  'Bust',
 #  'Civil Liberty',
 #  'Civil Unrest',
 #  'Civil War',
 #  'Drought',
 #  'Election',
 #  'Expansion',
 #  'Famine',
 #  'Infrastructure Failure',
 #  'Investment',
 #  'Lockdown',
 #  'Natural Disaster',
 #  'None',
 #  'Outbreak',
 #  'Pirate Attack',
 #  'Public Holiday',
 #  'Retreat',
 #  'Terrorist Attack',
 #  'War']
#  Found ships
#  ['Adder',
 #  'Alliance Challenger',
 #  'Alliance Chieftain',
 #  'Alliance Crusader',
 #  'Anaconda',
 #  'Asp Explorer',
 #  'Asp Scout',
 #  'Beluga Liner',
 #  'Cobra MkIII',
 #  'Cobra MkIV',
 #  'Diamondback Explorer',
 #  'Diamondback Scout',
 #  'Dolphin',
 #  'Eagle',
 #  'Federal Assault Ship',
 #  'Federal Corvette',
 #  'Federal Dropship',
 #  'Federal Gunship',
 #  'Fer-de-Lance',
 #  'Hauler',
 #  'Imperial Clipper',
 #  'Imperial Courier',
 #  'Imperial Cutter',
 #  'Imperial Eagle',
 #  'Keelback',
 #  'Krait MkII',
 #  'Krait Phantom',
 #  'Mamba',
 #  'Orca',
 #  'Python',
 #  'Sidewinder',
 #  'Type-10 Defender',
 #  'Type-6 Transporter',
 #  'Type-7 Transporter',
 #  'Type-9 Heavy',
 #  'Viper MkIII',
 #  'Viper MkIV',
 #  'Vulture']
 ## Found power states
#  ['Contested',
 #  'Controlled',
 #  'Exploited',
 #  'HomeSystem',
 #  'InPrepareRadius',
 #  'Prepared',
 #  'Turmoil']


def main():
    """ Main function. """
    now = datetime.datetime.utcnow()
    with open(SYSTEMS_CSV.replace('.csv', '.ids.json'), 'r', encoding='utf-8') as fin:
        system_ids = json.load(fin)
        print(f"Number of total systems: {len(system_ids)}")
    print("Took", datetime.datetime.utcnow() - now)

    ## Big map of system ids
    #  sys_ids = {}
    #  cnt = 0
    #  with open(SYSTEMS_CSV.replace('.csv', '.parsed.txt'), 'r', encoding='utf-8') as fin,\
         #  open(SYSTEMS_CSV.replace('.csv', '.ids.json'), 'w', encoding='utf-8') as fout:
        #  for line in fin:
            #  cnt += 1
            #  system = eval(line)
            #  sys_ids[system.name] = system.id

            #  if cnt == 100000:
                #  cnt = 0
                #  print('len keys', len(sys_ids.keys()))

        #  json.dump(sys_ids, fout, indent=2)

    #  with open(SYSTEMS_CSV.replace('.csv', '.parsed.txt'), 'w', encoding='utf-8') as fout,\
         #  cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        #  for sys in systems_csv_importer(eddb_session, SYSTEMS_CSV):
            #  fout.write(repr(sys) + '\n')


if __name__ == "__main__":
    main()
