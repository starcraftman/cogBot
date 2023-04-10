"""
Test importer for spansh data, dumped daily
Site: https://spansh.co.uk/dumps
Link: https://downloads.spansh.co.uk/galaxy_stations.json.gz

1. Spansh data doesn't have fixed "keys", in order to be compatible will need
lookup ability to map the names of stations, factions and systems => EDDB IDs that are fixed.
2. Each load function here will operate on a "complete" loaded json object, no ijson.
3. Achieve parallelism by reading from galaxy_json in a striped fashion.
   Each process will open it's own session onto the db.
4. Speed up update and insert of data by making use of bulk_insert_mappings and bulk_update_mappings
Compute for each object a list of kwargs to push in.
https://towardsdatascience.com/how-to-perform-bulk-inserts-with-sqlalchemy-efficiently-in-python-23044656b97d

System Keys
['allegiance', 'bodies', 'bodyCount', 'controllingFaction', 'coords', 'date', 'factions', 'government',
'id64', 'name', 'population', 'powerState', 'powers', 'primaryEconomy', 'secondaryEconomy', 'security', 'stations']

Factions objects
['allegiance', 'government', 'influence', 'name', 'state']

Stations objects
['controllingFaction', 'controllingFactionState', 'distanceToArrival', 'government', 'id', 'landingPads',
'market', 'name', 'outfitting', 'primaryEconomy', 'services', 'shipyard', 'type', 'updateTime']
"""
import asyncio
import concurrent.futures as cfut
import datetime
import json
import logging
import math
import os
import pprint
import time
from pathlib import Path

import sqlalchemy as sqla
from sqlalchemy.schema import UniqueConstraint

import cogdb
from cogdb.eddb import (
    Base, LEN, Allegiance, Economy, Faction, Influence, FactionState, FactionActiveState, Government,
    Power, PowerState, Security, System, Station, StationType, StationEconomy, StationFeatures,
)
from cogdb.spy_squirrel import SpyShip
import cog.util
from cog.util import ReprMixin, UpdatableMixin


JSON_INDENT = 2
TIME_STRP = "%Y-%m-%d %H:%M:%S"
FACTION_MAPF = cog.util.rel_to_abs('data', 'factionMap.json')
SYSTEM_MAPF = cog.util.rel_to_abs('data', 'systemMap.json')
STATION_MAPF = cog.util.rel_to_abs('data', 'stationMap.json')
SYSTEMS_CSV = os.path.join(cog.util.CONF.paths.eddb_store, 'systems.csv')
GALAXY_JSON = os.path.join(cog.util.CONF.paths.eddb_store, 'galaxy_stations.json')
SPANSH_COMMODITIES = cog.util.rel_to_abs('data', 'commodities.spansh')
SPANSH_MODULES = cog.util.rel_to_abs('data', 'modules.spansh')
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
# Mapping of spansh naming of station services, bit unsure of some
SPANSH_STATION_SERVICES = {
    'Fleet Carrier Administration': 'carrier_administration',
    'Fleet Carrier Vendor': 'carrier_vendor',
    'Black Market': 'blackmarket',
    'Material Trader': 'material_trader',
    'Shop': 'commodities',  # Unsure of mapping
    'Dock': 'dock',
    'Interstellar Factors Contact': 'interstellar_factors',
    'Market': 'market',
    'Outfitting': 'outfitting',
    'Shipyard': 'shipyard',
    'Restock': 'rearm',
    'Refuel': 'refuel',
    'Repair': 'repair',
    'Technology Broker': 'technology_broker',
    'Universal Cartographics': 'universal_cartographics',
}


class SCommodity(ReprMixin, UpdatableMixin, Base):
    """ A spansh commodity sold at a station. """
    __tablename__ = 'spansh_commodities'
    _repr_keys = ['id', 'group_id', "name"]

    id = sqla.Column(sqla.Integer, primary_key=True)  # commodityId
    group_id = sqla.Column(sqla.Integer, sqla.ForeignKey("spansh_commodity_groups.id"), nullable=False)
    name = sqla.Column(sqla.String(LEN["commodity"]))

    @property
    def text(self):
        """ Alias for name. """
        return self.name

    def __eq__(self, other):
        return (isinstance(self, SCommodity) and isinstance(other, SCommodity)
                and hash(self) == hash(other))

    def __hash__(self):
        return self.id


class SCommodityGroup(ReprMixin, Base):
    """ The spansh group for a commodity """
    __tablename__ = "spansh_commodity_groups"
    _repr_keys = ['id', 'name']

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN["commodity_group"]))

    @property
    def text(self):
        """ Alias for name. """
        return self.name

    def __eq__(self, other):
        return (isinstance(self, SCommodityGroup) and isinstance(other, SCommodityGroup)
                and hash(self) == hash(other))

    def __hash__(self):
        return self.id


class SCommodityPricing(ReprMixin, UpdatableMixin, Base):
    """ The spansh pricing of a commodity sold at the station indicated. """
    __tablename__ = 'spansh_commodity_pricing'
    __table_args__ = (
        UniqueConstraint('station_id', 'commodity_id', name='spansh_station_commodity_unique'),
    )
    _repr_keys = [
        'id', 'station_id', 'commodity_id', "demand", "supply", "buy_price", "sell_price", "updated_at"
    ]

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    station_id = sqla.Column(sqla.Integer, sqla.ForeignKey("stations.id"), nullable=False)
    commodity_id = sqla.Column(sqla.Integer, sqla.ForeignKey("spansh_commodities.id"), nullable=False)

    demand = sqla.Column(sqla.Integer, default=0)
    supply = sqla.Column(sqla.Integer, default=0)
    buy_price = sqla.Column(sqla.Integer, default=0)
    sell_price = sqla.Column(sqla.Integer, default=0)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    @property
    def text(self):
        """ Alias for name. """
        return self.name

    def __eq__(self, other):
        return (isinstance(self, SCommodityPricing) and isinstance(other, SCommodityPricing)
                and hash(self) == hash(other))

    def __hash__(self):
        return hash(f'{self.station_id}_{self.commodity_id}')


class SModule(ReprMixin, UpdatableMixin, Base):
    """ A spansh module sold in a shipyard. """
    __tablename__ = 'spansh_modules'
    _repr_keys = [
        'id', 'group_id', "ship_id", "name", "symbol", "mod_class", "rating"
    ]

    id = sqla.Column(sqla.Integer, primary_key=True)  # moduleId
    group_id = sqla.Column(sqla.Integer, sqla.ForeignKey("spansh_module_groups.id"), nullable=False)
    ship_id = sqla.Column(sqla.Integer)

    name = sqla.Column(sqla.String(LEN["module"]))
    symbol = sqla.Column(sqla.String(200))
    mod_class = sqla.Column(sqla.Integer, default=1)
    rating = sqla.Column(sqla.String(5), default=1)

    @property
    def text(self):
        """ Alias for name. """
        return self.name

    def __eq__(self, other):
        return (isinstance(self, SModule) and isinstance(other, SModule)
                and hash(self) == hash(other))

    def __hash__(self):
        return self.id


class SModuleGroup(ReprMixin, Base):
    """ The spansh group for a module """
    __tablename__ = "spansh_module_groups"
    _repr_keys = ['id', 'name']

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN["module_group"]))

    @property
    def text(self):
        """ Alias for name. """
        return self.name

    def __eq__(self, other):
        return (isinstance(self, SModuleGroup) and isinstance(other, SModuleGroup)
                and hash(self) == hash(other))

    def __hash__(self):
        return self.id


class SModuleSold(ReprMixin, UpdatableMixin, Base):
    """ The spansh module is sold at the station indicted. """
    __tablename__ = 'spansh_modules_sold'
    __table_args__ = (
        UniqueConstraint('station_id', 'module_id', name='spansh_station_module_unique'),
    )
    _repr_keys = [
        'id', 'station_id', 'module_id', "updated_at"
    ]

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    station_id = sqla.Column(sqla.Integer, sqla.ForeignKey("stations.id"), nullable=False)
    module_id = sqla.Column(sqla.Integer, sqla.ForeignKey("spansh_modules.id"), nullable=False)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    def __eq__(self, other):
        return (isinstance(self, SModuleSold) and isinstance(other, SModuleSold)
                and hash(self) == hash(other))

    def __hash__(self):
        return hash(f'{self.station_id}_{self.module_id}')


def preload_tables(eddb_session):  # pragma: no cover
    """
    Preload tables with fairly constant group information for modules and commodities.

    Args:
        eddb_session: A session onto EDDB.
    """
    eddb_session.add_all([
        SModuleGroup(id=1, name='hardpoint'),
        SModuleGroup(id=2, name='internal'),
        SModuleGroup(id=3, name='standard'),
        SModuleGroup(id=4, name='utility'),
        SCommodityGroup(id=1, name='Chemicals'),
        SCommodityGroup(id=2, name='Consumer Items'),
        SCommodityGroup(id=3, name='Foods'),
        SCommodityGroup(id=4, name='Industrial Materials'),
        SCommodityGroup(id=5, name='Legal Drugs'),
        SCommodityGroup(id=6, name='Machinery'),
        SCommodityGroup(id=7, name='Medicines'),
        SCommodityGroup(id=8, name='Metals'),
        SCommodityGroup(id=9, name='Minerals'),
        SCommodityGroup(id=10, name='Salvage'),
        SCommodityGroup(id=11, name='Slavery'),
        SCommodityGroup(id=12, name='Technology'),
        SCommodityGroup(id=13, name='Textiles'),
        SCommodityGroup(id=14, name='Waste'),
        SCommodityGroup(id=15, name='Weapons'),
    ])
    eddb_session.flush()
    with open(SPANSH_COMMODITIES, 'r', encoding='utf-8') as fin:
        comms = eval(fin.read())
        eddb_session.add_all(comms)
    with open(SPANSH_MODULES, 'r', encoding='utf-8') as fin:
        modules = eval(fin.read())
        eddb_session.add_all(modules)


def date_to_timestamp(text):
    """
    Parse a given UTC date from spansh dumps to timestamp.

    Args:
        text: A date and time string matching the format of TIME_STRP. Trailing +00 will be trimmed.

    Returns: A timestamp of the given date.
    """
    try:
        if text[-3:] == '+00':
            text = text[:-3]
        parsed_time = datetime.datetime.strptime(text, TIME_STRP)
        return parsed_time.replace(tzinfo=datetime.timezone.utc).timestamp()
    except ValueError:
        return None


def parse_station_features(features, *, station_id, updated_at):
    """
    Parse and return a StationFeatures object based on features found in
    spansh specific station services section.

    Args:
        features: The list of station services from spansh.
        station_id: The actual id of the station in question.
        update_at: The timestamp to assign to this feature set.

    Returns: The kwargs to create a cogdb.eddb.StationFeatures object
    """
    kwargs = {
        'id': station_id,
        'updated_at': updated_at,
        'carrier_administration': False,
        'carrier_vendor': False,
        'blackmarket': False,
        'material_trader': False,
        'commodities': False,
        'dock': False,
        'interstellar_factors': False,
        'market': False,
        'outfitting': False,
        'shipyard': False,
        'rearm': False,
        'refuel': False,
        'repair': False,
        'technology_broker': False,
        'universal_cartographics': False,
    }
    for feature in features:
        key = SPANSH_STATION_SERVICES.get(feature)
        if key:
            kwargs[key] = True

    return kwargs


def transform_commodities(*, station, mapped):
    """
    Load all commodity types at a station.

    Args:
        station: The dictionary object rooted at station.
        mapped: A dictionary of mappings to map down constants to their integer IDs.

    Returns: The list of all SCommodity found at the station.
    """
    commodities = set()
    if "market" in station and "commodities" in station['market']:
        for comm in station['market'].get('commodities', []):
            commodities.add(SCommodity(
                id=comm['commodityId'],
                group_id=mapped['commodity_group'][comm['category']],
                name=comm['name'],
            ))

    return commodities


def transform_commodity_pricing(*, station, station_id):
    """
    Load all commodity pricing at a station.
    Pricing will be deduped by enforcing station-commodityId uniqueness.
    Note: updated_at time can be determined by station object. Omitted for space saving.

    Args:
        station: The dictionary object rooted at station.
        mapped: A dictionary of mappings to map down constants to their integer IDs.
        station_id: The id of the station in question.

    Returns: The list of all SCommodityPricing found at the station.
    """
    comm_pricings = {}
    if "market" in station and "commodities" in station['market']:
        for comm in station['market'].get('commodities', []):
            comm_pricings[f"{station_id}_{comm['commodityId']}"] = {
                'station_id': station_id,
                'commodity_id': comm['commodityId'],
                'demand': comm['demand'],
                'supply': comm['supply'],
                'buy_price': comm['buyPrice'],
                'sell_price': comm['sellPrice'],
            }

    return list(comm_pricings.values())


def transform_modules(*, station, mapped):
    """
    Load all modules types at a station.

    Args:
        station: The dictionary object rooted at station.
        mapped: A dictionary of mappings to map down constants to their integer IDs.

    Returns: A list of SModules found at the station
    """
    modules = set()
    if "outfitting" in station and "modules" in station['outfitting']:
        for mod in station['outfitting'].get('modules', []):
            ship_id = None
            if 'ship' in mod:
                ship_id = mapped['ship'].get(mod['ship'])

            modules.add(SModule(
                id=mod['moduleId'],
                group_id=mapped['module_group'][mod['category']],
                ship_id=ship_id,
                name=mod['name'],
                symbol=mod['symbol'],
                mod_class=int(mod['class']),
                rating=mod['rating'],
            ))

    return modules


def transform_modules_sold(*, station, station_id):
    """
    Load all modules sold at a station.
    Note: updated_at time can be determined by station object. Omitted for space saving.

    Args:
        station: The dictionary object rooted at station.
        mapped: A dictionary of mappings to map down constants to their integer IDs.
        station_id: The id of the station in question.

    Returns: The list of SModuleSold found
    """
    module_sales = []
    if "outfitting" in station and "modules" in station['outfitting']:
        for mod in station['outfitting'].get('modules', []):
            module_sales += [{
                'station_id': station_id,
                'module_id': mod['moduleId'],
            }]

    return module_sales


def transform_system(*, data, mapped):
    """
    Load the system information in a single data object from spansh.
    To be specific, data should be a complete line from the galaxy_stations.json

    Args:
        data: A complex dictionary nesting all the information of a single system from spansh.
        mapped: A dictionary of mappings to map down constants to their integer IDs.

    Raises:
        KeyError - The System in question is not of interest, stop parsing.

    Returns: The kwargs to create A cogdb.eddb.System object representing the system in question.
    """
    data['updated_at'] = date_to_timestamp(data['date'])
    system_id = mapped['systems'].get(data['name'])
    if not system_id:
        logging.getLogger(__name__).error("SPANSH: Missing ID for system: %s", data['name'])
        raise KeyError

    controlling_faction = None
    if 'controlling_faction' in data:
        controlling_faction = data['controllingFaction']['name']
    power_id = None
    if 'powers' in data and len(data['powers']) == 1:
        power_id = mapped['power'][data['powers'][0]]
    kwargs = {
        'id': system_id,
        'ed_system_id': data['id64'],
        'controlling_minor_faction_id': mapped['factions'][controlling_faction],
        'name': data['name'],
        'population': data.get('population'),
        'power_id': power_id,
        'power_state_id': mapped['power_state'][data.get('powerState')],
        'security_id': mapped['security'][data.get('security')],
        'primary_economy_id': mapped['economy'][data.get('primaryEconomy')],
        'secondary_economy_id': mapped['economy'][data.get('secondaryEconomy')],
        'x': data['coords']['x'],
        'y': data['coords']['y'],
        'z': data['coords']['z'],
        'updated_at': data['updated_at'],
    }

    return kwargs


# Missing Happiness, I think only means will be EDDN updates. Unless it somewhere in data?
def transform_factions(*, data, mapped, system_id):
    """
    Load the faction and influence information in a single data object from spansh.
    To be specific, data should be a complete line from the galaxy_stations.json

    Args:
        data: A complex dictionary nesting all the information of a single system from spansh.
        mapped: A dictionary of mappings to map down constants to their integer IDs.
        system_id: The ID of the system of the factions.

    Returns: {faction_id: {'faction': ..., 'influence': ..., 'state'}: ...}
        faction_id: The id of the given faction.
        faction: This key represents the information to create a cogdb.eddb.Faction object.
        influence: This key represents the information to create a cogdb.eddb.Influence object.
        state: This key represents the information to create a cogdb.eddb.FactionActiveState object.
    """
    if 'factions' not in data or not data['factions']:
        return {}

    if 'updated_at' not in data:
        data['updated_at'] = date_to_timestamp(data['date'])

    results = {}
    for faction_info in data['factions']:
        faction_id = mapped['factions'].get(faction_info['name'])
        if not faction_id:
            logging.getLogger(__name__).error("SPANSH: Missing ID for faction: %s", data['name'])
            continue

        try:
            controlling_faction = faction_info['name'] == data['controllingFaction']['name']
        except KeyError:
            controlling_faction = None
        results[faction_id] = {
            'faction': {
                'id': faction_id,
                'allegiance_id': mapped['allegiance'][faction_info['allegiance']],
                'government_id': mapped['government'][faction_info['government']],
                'state_id': mapped['faction_state'][faction_info['state']],
                'name': faction_info['name'],
                'updated_at': data['updated_at']
            },
            'influence': {
                'faction_id': faction_id,
                'system_id': system_id,
                'happiness_id': None,
                'influence': faction_info['influence'],
                'is_controlling_faction': controlling_faction,
                'updated_at': data['updated_at']
            },
            'state': {
                'system_id': system_id,
                'faction_id': faction_id,
                'state_id': mapped['faction_state'][faction_info['state']],
                'updated_at': data['updated_at']
            },
        }

    return results


def transform_stations(*, data, mapped, system_id):
    """
    Load the station information in a single data object from spansh.
    To be specific, data should be a complete line from the galaxy_stations.json

    Args:
        data: A complex dictionary nesting all the information of a single system from spansh.
        mapped: A dictionary of mappings to map down constants to their integer IDs.
        system_id: The ID of the system of the factions.

    Returns: {station_id: {'station': ..., 'features': ..., 'economy': ..., 'modules_sold': ..., 'commodity_pricing': ...}, ...}
        station_id: The id of the given station
        station: The information to create a cogdb.eddb.Station object
        features: The information to create a cogdb.eddb.StationFeatures object
        economy: The information to create a cogdb.eddb.StationEconomy object
        modules_sold: The information to create a list of cogdb.eddb.SModuleSold objects for the station
        commodity_pricing: The information to create a list of cogdb.eddb.SCommodityPricing objects for the station
    """
    results = {}

    for station in data['stations']:
        station_id = mapped['stations'].get(station['name'])
        if not station_id or not station.get('type'):
            continue  # Ignore stations that aren't typed or mapped to IDs

        max_pad = 'S'
        if 'landingPads' in station and 'large' in station['landingPads']:
            max_pad = 'L'
        elif 'landingPads' in station and 'medium' in station['landingPads']:
            max_pad = 'M'

        controlling_minor_faction_id = None
        if 'controllingFaction' in station:
            controlling_minor_faction_id = mapped['factions'].get(station['controllingFaction'])

        updated_at = date_to_timestamp(station['updateTime'])
        kwargs = {
            'id': station_id,
            'type_id': mapped['station_type'][station['type']],
            'system_id': system_id,
            'name': station['name'],
            'controlling_minor_faction_id': controlling_minor_faction_id,
            'distance_to_star': station['distanceToArrival'],
            'max_landing_pad_size': max_pad,
            'updated_at': updated_at,
        }
        results[station_id] = {
            'station': kwargs,
            'features': parse_station_features(station['services'], station_id=station_id, updated_at=updated_at),
            'modules_sold': transform_modules_sold(station=station, station_id=station_id),
            'commodity_pricing': transform_commodity_pricing(station=station, station_id=station_id)
        }
        if 'primaryEconomy' in station:
            results[station_id]['economy'] = {
                'id': station_id,
                'economy_id': mapped['economy'][station['primaryEconomy']],
                'primary': True
            }

    return results


def transform_bodies(*, data, mapped, system_id):
    """
    Load the station information attached to bodies in a single data object from spansh.
    To be specific, data should be a complete line from the galaxy_stations.json

    Args:
        data: A complex dictionary nesting all the information of a single system from spansh.
        mapped: A dictionary of mappings to map down constants to their integer IDs.
        system_id: The ID of the system of the factions.

    Returns: A dictionary with all the parsed information of the bodies in the data.
             See transform_station for keys.
    """
    results = {}

    if 'updated_at' not in data:
        data['updated_at'] = date_to_timestamp(data['date'])

    for body in data['bodies']:
        if body['stations']:
            body['updated_at'] = data['updated_at']
            results.update(transform_stations(data=body, mapped=mapped, system_id=system_id))

    return results


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


def generate_name_maps_from_eddb(eddb_session, *, path, clean=False):
    """
    Generate initial name to ID mappings for factions, systems and stations
    based on initial EDDB IDs that are imported into EDDB traditionally.
    This will mainly provide IDs for populated space.

    Files written to data folder: factionMap.json, systemMap.json, stationMap.json

    Args:
        eddb_session: A session onto the EDDB.
        path: The folder to write the files into.
        clean: Zero out the "new" map tracking non EDDB IDs added when True
    """
    pairings = [
        ['factionMap.json', {x.name: x.id for x in eddb_session.query(cogdb.eddb.Faction)}],
        ['systemMap.json', {x.name: x.id for x in eddb_session.query(cogdb.eddb.System)}],
        ['stationMap.json', {x.name: x.id for x in eddb_session.query(cogdb.eddb.Station)}]
    ]
    for fname, themap in pairings:
        fpath = os.path.join(path, fname)
        with open(fpath, 'w', encoding='utf-8') as fout:
            json.dump(themap, fout, indent=JSON_INDENT, sort_keys=True)

        if clean:
            with open(fpath.replace('Map.json', 'NewMap.json'), 'w', encoding='utf-8') as fout:
                json.dump({}, fout)


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
    available = set(range(1, int(known_ids[-1]) + len(missing_names))) - set(known_ids)
    available = list(sorted(available, reverse=True))

    for name in missing_names:
        new_id = available.pop()
        new_names[name] = new_id
        known_names[name] = new_id

    with open(known_fname, 'w', encoding='utf-8') as fout:
        json.dump(known_names, fout, indent=JSON_INDENT, sort_keys=True)
    with open(new_fname, 'w', encoding='utf-8') as fout:
        json.dump(new_names, fout, indent=JSON_INDENT, sort_keys=True)


def update_all_name_maps(galaxy_json_path):  # pragma: no cover
    """
    Update the name maps based on information found in the galaxy_json.

    Args:
        galaxy_json_path: Path to the complete galaxy_json file from spansh.

    Raises:
        FileNotFoundError - Missing a required file.
    """
    factions, systems, stations = verify_galaxy_ids(galaxy_json_path)

    mapped = [
        [factions, 'faction'],
        [systems, 'system'],
        [stations, 'station'],
    ]
    for missing, name in mapped:
        if not missing:
            continue

        known_fname = Path(cog.util.rel_to_abs('data', f'{name}Map.json'))
        new_fname = Path(cog.util.rel_to_abs('data', f'{name}NewMap.json'))
        if not known_fname.exists():
            raise FileNotFoundError(f"Missing: {known_fname}")
        if not new_fname.exists():
            raise FileNotFoundError(f"Missing: {new_fname}")

        update_name_map(missing, known_fname=known_fname, new_fname=new_fname)


def verify_galaxy_ids(dump_path):  # pragma: no cover
    """
    Verify the IDs of systems, factions and stations in galaxy_stations.json file.

    Args:
        dump_path: The file path to the galaxy_stations.json file.

    Returns: (missing_systems, missing_factions, missing_stations)
        missing_systems: The names of systems that need static IDs
        missing_factions: The names of factions that need static IDs
        missing_stations: The names of stations that need static IDs
    """
    missing_systems, missing_factions, missing_stations = set(), set(), set()
    with open(SYSTEM_MAPF, 'r', encoding='utf-8') as fin:
        known_systems = json.load(fin)
    with open(FACTION_MAPF, 'r', encoding='utf-8') as fin:
        known_factions = json.load(fin)
    with open(STATION_MAPF, 'r', encoding='utf-8') as fin:
        known_stations = json.load(fin)

    with open(dump_path, 'r', encoding='utf-8') as fin:
        for line in fin:
            if '{' not in line or '}' not in line:
                continue

            line = line.strip()
            if line[-1] == ',':
                line = line[:-1]
            data = json.loads(line)

            if data['name'] not in known_systems:
                missing_systems.add(data['name'])
            for faction in data.get('factions', []):
                if faction['name'] not in known_factions:
                    missing_factions.add(faction['name'])
            for station in data.get('stations', []):
                if station['name'] not in known_stations:
                    missing_stations.add(station['name'])
            for body in data.get('bodies', []):
                for station in body.get('stations', []):
                    if station['name'] not in known_stations:
                        missing_stations.add(station['name'])

    return list(sorted(missing_factions)), list(sorted(missing_systems)), list(sorted(missing_stations))


def eddb_maps(eddb_session):
    """
    Create static mappings of the names of constants onto their IDs.
    This will also populate mappings for systems, stations and factions based on EDDB IDs.
    This will also store some extra specific aliases from spansh names onto eddb names.

    Args:
        eddb_session: A session onto EDDB.

    Returns: A dictionary of mappings, the keys are the type of constant i.e. 'power' or 'security'.
    """
    mapped_cls = [
        ['allegiance', Allegiance],
        ['commodity_group', SCommodityGroup],
        ['economy', Economy],
        ['faction_state', FactionState],
        ['government', Government],
        ['module_group', SModuleGroup],
        ['power', Power],
        ['power_state', PowerState],
        ['security', Security],
        ['ship', SpyShip],
        ['station_type', StationType],
    ]

    if not eddb_session.query(SpyShip).all():
        cogdb.spy_squirrel.preload_spy_tables(eddb_session)
    mapped = {}
    for key, cls in mapped_cls:
        mapped[key] = {x.text: x.id for x in eddb_session.query(cls)}
        mapped[key]['None'] = None
        mapped[key][None] = None

    # Specific spansh name aliases
    mapped['power_state']['Controlled'] = mapped['power_state']['Control']
    mapped['power'].update({x.eddn: x.id for x in eddb_session.query(Power)})
    # No valid None for faction state
    mapped['faction_state']['None'] = 80
    mapped['faction_state'][None] = 80

    station_map = mapped['station_type']
    station_map['Drake-Class Carrier'] = station_map['Fleet Carrier']
    station_map['Mega ship'] = station_map['Megaship']
    station_map['Asteroid base'] = station_map['Asteroid Base']
    station_map['Outpost'] = station_map['Civilian Outpost']
    station_map['Settlement'] = station_map['Odyssey Settlement']

    ship_map = mapped['ship']
    ship_map['Sidewinder'] = ship_map['Sidewinder Mk. I']
    ship_map['Eagle'] = ship_map['Eagle Mk. II']
    ship_map['Cobra MkIII'] = ship_map["Cobra Mk. III"]
    ship_map['Cobra MkIV'] = ship_map["Cobra MK IV"]
    ship_map['Viper MkIII'] = ship_map["Viper Mk III"]
    ship_map['Viper MkIV'] = ship_map["Viper MK IV"]

    with open(SYSTEM_MAPF, 'r', encoding='utf-8') as fin:
        mapped['systems'] = json.load(fin)
    with open(STATION_MAPF, 'r', encoding='utf-8') as fin:
        mapped['stations'] = json.load(fin)
    with open(FACTION_MAPF, 'r', encoding='utf-8') as fin:
        mapped['factions'] = json.load(fin)
        mapped['factions'][None] = None

    return mapped


def collect_types(eddb_session):  # pragma: no cover
    """
    Collect a set of constants that might differ from those used by EDDB.io
    Prints out the found information directly to stdout.

    Args:
        eddb_session: A session onto the EDDB.
    """
    power_states, power_names, faction_states = set(), set(), set()
    station_types, station_services, ships = set(), set(), set()
    with open(GALAXY_JSON, 'r', encoding='utf-8') as fin:
        for line in fin:
            if line.startswith('[') or line.startswith(']'):
                continue

            line = line.strip()
            if line[-1] == ',':
                line = line[:-1]
            data = json.loads(line)

            if 'powerState' in data:
                power_states.add(data['powerState'])
            for power in data.get('powers', []):
                power_names.add(power)
            for faction in data.get('factions', []):
                faction_states.add(faction['state'])

            for station in data.get('stations', []):
                station_types.add(station['type'])
                if 'shipyard' in station and 'ships' in station['shipyard']:
                    for ship in station['shipyard']['ships']:
                        ships.add(ship['name'])
                for service in station['services']:
                    station_services.add(service)

            for body in data.get('bodies', []):
                for station in body.get('stations', []):
                    try:
                        station_types.add(station['type'])
                        if 'shipyard' in station and 'ships' in station['shipyard']:
                            for ship in station['shipyard']['ships']:
                                ships.add(ship['name'])
                        for service in station['services']:
                            station_services.add(service)
                    except KeyError:
                        pass  # Ignore all stations without typing

    mapped = eddb_maps(eddb_session)
    types = eddb_session.query(FactionState).all()
    known = [x.text for x in types] + [x.eddn for x in types]
    print("missing faction states")
    print(faction_states - set(known))

    print("missing power states")
    print(power_states - set(mapped['power_state']))

    print("missing powers")
    print(power_names - set(mapped['power']))

    print("missing ships")
    print(ships - set(mapped['ship'].keys()))

    print("missing station types")
    print(station_types - set(mapped['station_type'].keys()))

    print("Found station services, manually compare")
    __import__('pprint').pprint(list(sorted(station_services)))


def collect_modules_and_commodities(eddb_session):
    """
    Collect information on constants in the modules and commodities names and groups among the data.

    Args:
        eddb_session: A session onto the EDDB.
    """
    mods, comms = {}, {}
    with open(GALAXY_JSON, 'r', encoding='utf-8') as fin:
        for line in fin:
            if line.startswith('[') or line.startswith(']'):
                continue

            line = line.strip()
            if line[-1] == ',':
                line = line[:-1]
            data = json.loads(line)

            for station in data.get('stations', []):
                if 'market' in station and 'commodities' in station['market']:
                    for comm in station['market']['commodities']:
                        del comm['buyPrice']
                        del comm['sellPrice']
                        del comm['demand']
                        del comm['supply']
                        comms[comm['commodityId']] = comm
                if 'outfitting' in station and 'modules' in station['outfitting']:
                    for module in station['outfitting']['modules']:
                        mods[module['moduleId']] = module

            for body in data.get('bodies', []):
                for station in body.get('stations', []):
                    if 'market' in station and 'commodities' in station['market']:
                        for comm in station['market']['commodities']:
                            del comm['buyPrice']
                            del comm['sellPrice']
                            del comm['demand']
                            del comm['supply']
                            comms[comm['commodityId']] = comm
                    if 'outfitting' in station and 'modules' in station['outfitting']:
                        for module in station['outfitting']['modules']:
                            mods[module['moduleId']] = module

    return mods, comms


def collect_modules_and_commodity_groups():
    """
    Collect information on constants in the modules and commodities names and groups among the data.

    Returns: (mod_groups, comm_groups)
        mod_groups: A list of names of module groups.
        comm_groups: A list of names of commodity groups.
    """
    mod_groups, comm_groups = set(), set()
    with open(GALAXY_JSON, 'r', encoding='utf-8') as fin:
        for line in fin:
            if line.startswith('[') or line.startswith(']'):
                continue

            line = line.strip()
            if line[-1] == ',':
                line = line[:-1]
            data = json.loads(line)

            for station in data.get('stations', []):
                if 'market' in station and 'commodities' in station['market']:
                    for comm in station['market']['commodities']:
                        comm_groups.add(comm['category'])
                if 'outfitting' in station and 'modules' in station['outfitting']:
                    for module in station['outfitting']['modules']:
                        mod_groups.add(module['category'])

            for body in data.get('bodies', []):
                for station in body.get('stations', []):
                    if 'market' in station and 'commodities' in station['market']:
                        for comm in station['market']['commodities']:
                            comm_groups.add(comm['category'])
                    if 'outfitting' in station and 'modules' in station['outfitting']:
                        for module in station['outfitting']['modules']:
                            mod_groups.add(module['category'])

    return mod_groups, comm_groups


def generate_module_commodities_caches(eddb_session):
    """
    Generate the commodities.spansh and modules.spansh cache filse.
    They will be written out to data folder.

    Args:
        eddb_session: A session onto the EDDB.
    """
    mod_dict, comm_dict = collect_modules_and_commodities(eddb_session)
    mapped = eddb_maps(eddb_session)

    comms = [SCommodity(id=x['commodityId'], name=x['name'], group_id=mapped['commodity_group'][x['category']]) for x in comm_dict.values()]
    with open(SPANSH_COMMODITIES, 'w', encoding='utf-8') as fout:
        pprint.pprint(comms, fout)

    mods = []
    for mod in mod_dict.values():
        ship_id = None
        if 'ship' in mod:
            ship_id = mapped['ship'].get(mod['ship'])
        mods += [SModule(
            id=mod['moduleId'],
            group_id=mapped['module_group'][mod['category']],
            ship_id=ship_id,
            name=mod['name'],
            symbol=mod['symbol'],
            mod_class=int(mod['class']),
            rating=mod['rating'],
        )]
    with open(SPANSH_MODULES, 'w', encoding='utf-8') as fout:
        pprint.pprint(mods, fout)


def transform_galaxy_json(number, total, galaxy_json, out_fname):
    """
    Process number lines in the galaxy_json, skip all lines you aren't assigned.

    Args:
        number: Number assigned to worker, in range [0, total).
        total: The total number of jobs started.
        galaxy_json: The spansh galaxy_json
        out_fname: A fname to write out all work as parsing.

    Returns: out_fname, where the result was written to.
    """
    cnt = -1
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session,\
         open(galaxy_json, 'r', encoding='utf-8') as fin,\
         open(out_fname, 'w', encoding='utf-8') as fout:
        mapped = eddb_maps(eddb_session)

        for line in fin:
            cnt += 1
            if cnt == total:
                cnt = 0
            if cnt != number:  # Only process every numberth line
                continue
            if '{' not in line or '}' not in line:
                continue

            line = line.strip()
            if line[-1] == ',':
                line = line[:-1]
            data = json.loads(line)

            try:
                system = transform_system(data=data, mapped=mapped)
                factions = transform_factions(data=data, mapped=mapped, system_id=system['id'])
                stations = transform_stations(data=data, mapped=mapped, system_id=system['id'])
                stations.update(transform_bodies(data=data, mapped=mapped, system_id=system['id']))
                fout.write(str({
                    'system': system,
                    'factions': factions,
                    'stations': stations,
                }) + '\n')
            except KeyError:
                logging.getLogger(__name__).error("SPANSH: Missing system ID: %s", data['name'])

        return out_fname


def update_system(eddb_session, *, system_kwargs):
    """
    Upsert a system in the EDDB.

    Args:
        eddb_session: A session onto the EDDB.
        kwargs: The kwargs for a cogdb.eddb.System object.

    Returns: The updated cogdb.eddb.System object
    """
    try:
        system = eddb_session.query(System).filter(System.id == system_kwargs['id']).one()
        system.update(**system_kwargs)
    except sqla.exc.NoResultFound:
        system = System(**system_kwargs)
        eddb_session.add(system)

    return system


def update_factions(eddb_session, *, factions):
    """
    Upsert the factions for a system in the EDDB.

    Args:
        eddb_session: A session onto the EDDB.
        factions: A dictionary of kwargs for a series of factions and their respective factions, influence and state objects.

    Returns: A dictionary with the updated database objects.
    """
    updated = {}

    for faction_info in factions.values():
        faction_kwargs = faction_info['faction']
        try:
            faction = eddb_session.query(Faction).\
                filter(Faction.id == faction_kwargs['id']).\
                one()
            faction.update(**faction_kwargs)
        except sqla.exc.NoResultFound:
            faction = Faction(**faction_kwargs)
            eddb_session.add(faction)

        inf_kwargs = faction_info['influence']
        try:
            influence = eddb_session.query(Influence).\
                filter(Influence.faction_id == inf_kwargs['faction_id'],
                       Influence.system_id == inf_kwargs['system_id']).\
                one()
            influence.update(**inf_kwargs)
        except sqla.exc.NoResultFound:
            influence = Influence(**inf_kwargs)
            eddb_session.add(influence)

        state_kwargs = faction_info['state']
        eddb_session.query(FactionActiveState).\
            filter(FactionActiveState.faction_id == state_kwargs['faction_id'],
                   FactionActiveState.system_id == state_kwargs['system_id']).\
            delete()
        state = FactionActiveState(**state_kwargs)
        eddb_session.add(state)

        updated[faction_kwargs['id']] = {
            'faction': faction,
            'influence': influence,
            'state': state_kwargs,
        }

    return updated


def update_commodity_pricing(eddb_session, *, pricings, updated_at):
    """
    Update the modules sold at a station. All existing entries for station will be deleted.

    Args:
        eddb_session: A session onto the EDDB.
        modules: A dictionary of kwargs for a series of SModuleSold objects.
        updated_at: The timestamp of the station update.

    Returns: A dictionary with the updated database objects.
    """
    station_id = pricings[0]['station_id']
    eddb_session.query(SCommodityPricing).\
        filter(SCommodityPricing.station_id == station_id).\
        delete()

    updated = []
    for kwargs in pricings:
        kwargs['updated_at'] = updated_at
        module = SCommodityPricing(**kwargs)
        updated += [module]

    eddb_session.add_all(updated)
    return updated


def update_sold_modules(eddb_session, *, modules, updated_at):
    """
    Update the modules sold at a station. All existing entries for station will be deleted.

    Args:
        eddb_session: A session onto the EDDB.
        modules: A dictionary of kwargs for a series of SModuleSold objects.
        updated_at: The timestamp of the station update.

    Returns: A list of all update SModuleSold objects at the station.
    """
    station_id = modules[0]['station_id']
    eddb_session.query(SModuleSold).\
        filter(SModuleSold.station_id == station_id).\
        delete()

    updated = []
    for kwargs in modules:
        kwargs['updated_at'] = updated_at
        module = SModuleSold(**kwargs)
        updated += [module]

    eddb_session.add_all(updated)
    return updated


def update_stations(eddb_session, *, stations):
    """
    Upsert the factions for a system in the EDDB.

    Args:
        eddb_session: A session onto the EDDB.
        factions: A dictionary of kwargs for a series of factions and their respective factions, influence and state objects.

    Returns: A dictionary with the updated database objects.
    """
    updated = {}

    for station_info in stations.values():
        station_kwargs = station_info['station']
        try:
            station = eddb_session.query(Station).\
                filter(Station.id == station_kwargs['id']).\
                one()
            station.update(**station_kwargs)
        except sqla.exc.NoResultFound:
            station = Station(**station_kwargs)
            eddb_session.add(station)

        features_kwargs = station_info['features']
        try:
            features = eddb_session.query(StationFeatures).\
                filter(StationFeatures.id == features_kwargs['id']).\
                one()
            features.update(**features_kwargs)
        except sqla.exc.NoResultFound:
            features = StationFeatures(**features_kwargs)
            eddb_session.add(features)

        updated[station_kwargs['id']] = {
            'station': station,
            'features': features,
        }

        #  if station_info.get('modules_sold'):
            #  update_sold_modules(eddb_session, modules=station_info['modules_sold'],
                                #  updated_at=station_kwargs['updated_at'])
        #  if station_info.get('commodity_pricing'):
            #  update_commodity_pricing(eddb_session, pricings=station_info['commodity_pricing'],
                                     #  updated_at=station_kwargs['updated_at'])
        if station_info.get('economy'):
            economy_kwargs = station_info['economy']
            try:
                economy = eddb_session.query(StationEconomy).\
                    filter(StationEconomy.id == economy_kwargs['id'],
                           StationEconomy.primary).\
                    one()
                economy.economy_id = economy_kwargs['economy_id']
            except sqla.exc.NoResultFound:
                economy = StationEconomy(**economy_kwargs)
                eddb_session.add(economy)
            updated[economy_kwargs['id']]['economy'] = economy

    return updated


def import_galaxy_objects(fname):
    """
    Process number lines in the galaxy_json, skip all lines you aren't assigned.

    Args:
        fname: The fname of a file written out from transform_galaxy_json.
    """
    folder = Path(fname).parent
    #  with open(fname, 'r', encoding='utf-8') as fin,\
         #  open(folder / 'systems.json', 'a', encoding='utf-8') as sys_out,\
         #  open(folder / 'factions.json', 'a', encoding='utf-8') as sys_outRuuu
         #  open(folder / 'factions.json', 'a', encoding='utf-8') as sys_out:

        #  for line in fin:
            #  info = eval(line)

            #  update_system(eddb_session, system_kwargs=info['system'])
            #  update_factions(eddb_session, factions=info['factions'])
            #  update_stations(eddb_session, stations=info['stations'])


async def parallel_process(galaxy_json, *, jobs):
    """
    Parallel process the galaxy_json.
    Start jobs number of processes, each with own portion of file.
    Then parse and process all information in those filess.
    For each process, output will be written back out to a file in galaxy directory.

    Args:
        galaxy_json: The path to the galaxy_json from spansh.
        jobs: The number of jobs to start.
    """
    print(f"parallel_process: Starting {jobs} jobs to process {galaxy_json}")
    loop = asyncio.get_event_loop()

    with cfut.ProcessPoolExecutor(jobs) as pool:
        futs = []
        for num in range(0, jobs):
            futs += [loop.run_in_executor(
                pool,
                transform_galaxy_json, num, jobs, GALAXY_JSON,
                galaxy_json.replace('.json', f'.json.{num:02}')
            )]

        await asyncio.wait(futs)
        print('parallel_process: Results written out to:')
        fnames = [x.result() for x in futs]
        pprint.pprint(fnames)

        futs = []
        for num in range(0, jobs):
            await loop.run_in_executor(
                pool,
                import_galaxy_objects, galaxy_json.replace('.json', f'.json.{num:02}')
            )
        print("Database update complete.")


def drop_tables():  # pragma: no cover | destructive to test
    """
    Drop all tables related to this module.
    """
    sqla.orm.session.close_all_sessions()

    for table in SPANSH_TABLES:
        try:
            table.__table__.drop(cogdb.eddb_engine)
        except sqla.exc.OperationalError:
            pass


def empty_tables():
    """
    Empty all tables related to this module.
    """
    sqla.orm.session.close_all_sessions()

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        for table in SPANSH_TABLES:
            eddb_session.query(table).delete()


def recreate_tables():
    """
    Recreate all tables in the spansh module, mainly for schema changes and testing.
    """
    sqla.orm.session.close_all_sessions()
    drop_tables()
    Base.metadata.create_all(cogdb.eddb_engine)


def main():
    """ Main function. """
    start = datetime.datetime.utcnow()
    #  recreate_tables()
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        #  preload_tables(eddb_session)
        mapped = eddb_maps(eddb_session)
        print('Missing systems')
        print(len(set(mapped['systems'].values()) - {x[0] for x in eddb_session.query(System.id)}))
        pprint.pprint(set(mapped['systems'].values()) - {x[0] for x in eddb_session.query(System.id)})
        print('Missing factions')
        print(set(mapped['factions'].values()) - {x[0] for x in eddb_session.query(Faction.id)})
        print('Missing stations')
        print(len(set(mapped['stations'].values()) - {x[0] for x in eddb_session.query(Station.id)}))

    #  asyncio.new_event_loop().run_until_complete(parallel_process(GALAXY_JSON, jobs=math.floor(os.cpu_count() * 1.5)))
    #  print("Time taken", datetime.datetime.utcnow() - start)

    # Mainly for experimenting
    #  with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        #  generate_module_commodities_caches(eddb_session)
        #  collect_other_types(eddb_session)

    #  with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        #  data = cog.util.rel_to_abs('data')
        #  generate_name_maps_from_eddb(eddb_session, path=data, clean=True)
        #  update_all_name_maps(GALAXY_JSON)

    #  now = datetime.datetime.utcnow()
    #  with open(SYSTEMS_CSV.replace('.csv', '.ids.json'), 'r', encoding='utf-8') as fin:
        #  system_ids = json.load(fin)
        #  print(f"Number of total systems: {len(system_ids)}")
    #  print("Took", datetime.datetime.utcnow() - now)

    # Big map of system ids
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


SPANSH_TABLES = [SCommodityPricing, SModuleSold, SCommodity, SModule, SCommodityGroup, SModuleGroup]


if __name__ == "__main__":
    main()
