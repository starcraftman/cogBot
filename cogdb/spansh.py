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
import functools
import glob
import gzip
import json
import logging
import math
import os
import pprint
import sys
from pathlib import Path

import argparse
import requests
import sqlalchemy as sqla
from sqlalchemy.schema import UniqueConstraint
import tqdm

import cogdb
import cogdb.eddb
import cogdb.spy_squirrel
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
SPLIT_FILENAMES = [
    'systems', 'factions', 'influences', 'faction_states',
    'stations', 'controlling_factions'
]
STATION_KEYS = ['stations', 'features', 'economies', 'modules']
COMMODITY_STATION_LIMIT = 30000
CLEANUP_GLOBS = [f"{x}.json.*" for x in SPLIT_FILENAMES] + ['*.correct', '*.uniqu*']
GALAXY_URL = "https://downloads.spansh.co.uk/galaxy_stations.json.gz"
GALAXY_JSON = os.path.join(cog.util.CONF.paths.eddb_store, 'galaxy_stations.json')
GALAXY_COMPRESSION_RATE = 6.35


class SpanshParsingError(Exception):
    """
    Error happened during processing spansh data.
    """


@functools.total_ordering
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

    def __lt__(self, other):
        return (isinstance(self, SCommodity) and isinstance(other, SCommodity)
                and self.name < other.name)

    def __hash__(self):
        return self.id


@functools.total_ordering
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

    def __lt__(self, other):
        return (isinstance(self, SCommodityGroup) and isinstance(other, SCommodityGroup)
                and self.name < other.name)

    def __hash__(self):
        return self.id


class SCommodityPricing(ReprMixin, UpdatableMixin, Base):
    """
    The spansh pricing of a commodity sold at the station indicated.
    Updated_at can be found on station.
    """
    __tablename__ = 'spansh_commodity_pricing'
    __table_args__ = (
        UniqueConstraint('station_id', 'commodity_id', name='spansh_station_commodity_unique'),
    )
    _repr_keys = [
        'id', 'station_id', 'commodity_id', "demand", "supply", "buy_price", "sell_price"
    ]

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    station_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey("stations.id"), nullable=False)
    commodity_id = sqla.Column(sqla.Integer, sqla.ForeignKey("spansh_commodities.id"), nullable=False)

    demand = sqla.Column(sqla.Integer, default=0)
    supply = sqla.Column(sqla.Integer, default=0)
    buy_price = sqla.Column(sqla.Integer, default=0)
    sell_price = sqla.Column(sqla.Integer, default=0)

    @property
    def text(self):
        """ Alias for name. """
        return self.name

    def __eq__(self, other):
        return (isinstance(self, SCommodityPricing) and isinstance(other, SCommodityPricing)
                and hash(self) == hash(other))

    def __hash__(self):
        return hash(f'{self.station_id}_{self.commodity_id}')


@functools.total_ordering
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

    def __lt__(self, other):
        return (isinstance(self, SModule) and isinstance(other, SModule)
                and self.name < other.name)

    def __hash__(self):
        return self.id


@functools.total_ordering
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

    def __lt__(self, other):
        return (isinstance(self, SModuleGroup) and isinstance(other, SModuleGroup)
                and self.name < other.name)

    def __hash__(self):
        return self.id


class SModuleSold(ReprMixin, UpdatableMixin, Base):
    """
    The spansh module is sold at the station indicted.
    Updated_at can be found on station.
    """
    __tablename__ = 'spansh_modules_sold'
    __table_args__ = (
        UniqueConstraint('station_id', 'module_id', name='spansh_station_module_unique'),
    )
    _repr_keys = [
        'id', 'station_id', 'module_id',
    ]

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    station_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey("stations.id"), nullable=False)
    module_id = sqla.Column(sqla.Integer, sqla.ForeignKey("spansh_modules.id"), nullable=False)

    def __eq__(self, other):
        return (isinstance(self, SModuleSold) and isinstance(other, SModuleSold)
                and hash(self) == hash(other))

    def __hash__(self):
        return hash(f'{self.station_id}_{self.module_id}')


def preload_tables(eddb_session, only_groups=False):  # pragma: no cover
    """
    Preload tables with fairly constant information for commodities and modules.
    Meant to be used to seed a clean database.
    Includes:
        SModuleGroup
        SCommodityGroup
        SModule
        SCommodity

    Args:
        eddb_session: A session onto EDDB.
        only_groups: When True, only SModuleGroup and SCommodityGroup will be loaded.
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

    if not only_groups:
        with open(SPANSH_COMMODITIES, 'r', encoding='utf-8') as fin:
            comms = eval(fin.read())
            eddb_session.add_all(comms)
        with open(SPANSH_MODULES, 'r', encoding='utf-8') as fin:
            modules = eval(fin.read())
            eddb_session.add_all(modules)


def print_no_newline(text):
    """
    Print text with no new line at end.
    Then guarantee it flushes immediately to stdout.

    Args:
        text: The text to write to stdout.
    """
    print(text, end='')
    sys.stdout.flush()


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


def station_key(*, system, station):
    """
    Provide a unique key for a given station.
    Player Fleet Carriers are unique across the entire system by name.
    Otherwise, station names are not unique globally and will
    be paired with system.

    Args:
        system: The name of the system.
        station: The station information object.

    Returns: A string to uniquely identify station.
    """
    key = f"{system}_{station['name']}"

    try:
        if station['type'] == "Drake-Class Carrier":
            key = station['name']
    except KeyError:
        pass

    return key


def parse_station_features(features, *, station_id, updated_at):
    """
    Parse and return a StationFeatures object based on features found in
    spansh specific station services section.

    Args:
        features: The list of station services from spansh.
        station_id: The actual id of the station in question.
        update_at: The timestamp to assign to this feature set.

    Returns: The kwargs to create a StationFeatures object
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
        SpanshParsingError - The System is not mapped statically to an ID.

    Returns: The kwargs to create A cogdb.eddb.System object representing the system in question.
    """
    data['updated_at'] = date_to_timestamp(data['date'])
    system_id = mapped['systems'].get(data['name'])
    if not system_id:
        logging.getLogger(__name__).error("SPANSH: Missing ID for system: %s", data['name'])
        raise SpanshParsingError("SPANSH: Missing ID for system: " + data['name'])

    controlling_faction = None
    if 'controlling_faction' in data:
        controlling_faction = data['controllingFaction']['name']
    power_id = None
    if 'powers' in data and len(data['powers']) == 1:
        power_id = mapped['power'][data['powers'][0]]

    return {
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
        station: The information to create a Station object
        features: The information to create a StationFeatures object
        economy: The information to create a StationEconomy object
        modules_sold: The information to create a list of SModuleSold objects for the station
        commodity_pricing: The information to create a list of SCommodityPricing objects for the station
    """
    results = {}
    controlling_factions = {}

    for station in data['stations']:
        key = station_key(system=data['name'], station=station)
        try:
            station_id = mapped['stations'][key]
        except KeyError:
            # Still missing mapping some stations in bodies
            logging.getLogger(__name__).error("SPANSH: failed to ID station: %s", key)
            station_id = None

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
            if controlling_minor_faction_id not in controlling_factions:
                controlling_factions[controlling_minor_faction_id] = {
                    'id': controlling_minor_faction_id,
                    'name': station['controllingFaction']
                }

        economy_id = 10
        if 'primaryEconomy' in station:
            economy_id = mapped['economy'][station['primaryEconomy']]

        updated_at = date_to_timestamp(station['updateTime'])
        results[station_id] = {
            'station': {
                'id': station_id,
                'type_id': mapped['station_type'][station['type']],
                'system_id': system_id,
                'name': station['name'],
                'controlling_minor_faction_id': controlling_minor_faction_id,
                'distance_to_star': station['distanceToArrival'],
                'max_landing_pad_size': max_pad,
                'updated_at': updated_at,
            },
            'features': parse_station_features(station['services'], station_id=station_id, updated_at=updated_at),
            'economy': {
                'id': station_id,
                'economy_id': economy_id,
                'primary': True
            },
            'modules_sold': transform_modules_sold(station=station, station_id=station_id),
            'commodity_pricing': transform_commodity_pricing(station=station, station_id=station_id),
            'controlling_factions': list(controlling_factions.values())
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
    For each line return a fully parsed System object ready for database.

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
    last_num = known_ids[-1] if known_ids else 2
    available = set(range(1, last_num + len(missing_names) + 10)) - set(known_ids)
    available = list(sorted(available, reverse=True))

    for name in missing_names:
        new_id = available.pop()
        new_names[name] = new_id
        known_names[name] = new_id

    with open(known_fname, 'w', encoding='utf-8') as fout:
        json.dump(known_names, fout, indent=JSON_INDENT, sort_keys=True)
    with open(new_fname, 'w', encoding='utf-8') as fout:
        json.dump(new_names, fout, indent=JSON_INDENT, sort_keys=True)


def update_all_name_maps(factions, systems, stations):  # pragma: no cover
    """
    Update the name maps based on information found in the galaxy_json.

    Args:
        galaxy_json_path: Path to the complete galaxy_json file from spansh.
    """
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
        for fname in (known_fname, new_fname):
            if not fname.exists():
                with open(fname, 'w', encoding='utf-8') as fout:
                    fout.write('{}')

        update_name_map(missing, known_fname=known_fname, new_fname=new_fname)


def collect_unique_names(galaxy_json):  # pragma: no cover
    """
    Find all unique names in the provided galaxy_json and return a list of all
    unique system, faction and station names
    same directory as galaxy_json.
    Note that station names are not unique globally, just within their system.

    Args:
        galaxy_json: The file path to the galaxy_stations.json file.

    Returns: (systems, factions, stations)
        factions: A sorted list of all faction names found.
        systems: A sorted list of all system names found.
        station: A sorted list of all station keys found, see station_key function.
    """
    systems, factions, stations = set(), set(), set()

    with open(galaxy_json, 'r', encoding='utf-8') as fin:
        for line in fin:
            if '{' not in line or '}' not in line:
                continue

            line = line.strip()
            if line[-1] == ',':
                line = line[:-1]
            data = json.loads(line)

            systems.add(data['name'])
            for faction in data.get('factions', []):
                factions.add(faction['name'])
            for station in data.get('stations', []):
                stations.add(station_key(system=data['name'], station=station))
            for body in data.get('bodies', []):
                for station in body.get('stations', []):
                    stations.add(station_key(system=data['name'], station=station))

    return list(sorted(factions)), list(sorted(systems)), list(sorted(stations))


def collect_missing_galaxy_names(galaxy_json):  # pragma: no cover
    """
    Collect all system, faction and station unique names from the galaxy file missing from MAPF files.
    Stations are uniquely identified per station_key func as station names are
    not gauranteed unique unless they are a fleet carrier.

    Args:
        galaxy_json: The file path to the galaxy_stations.json file.

    Returns: (missing_systems, missing_factions, missing_stations)
        missing_factions: The sorted names of factions that need static IDs
        missing_systems: The sorted names of systems that need static IDs
        missing_stations: The sorted names of station_keys that need static IDs
    """
    missing_systems, missing_factions, missing_stations = set(), set(), set()
    with open(SYSTEM_MAPF, 'r', encoding='utf-8') as fin:
        known_systems = json.load(fin)
    with open(FACTION_MAPF, 'r', encoding='utf-8') as fin:
        known_factions = json.load(fin)
    with open(STATION_MAPF, 'r', encoding='utf-8') as fin:
        known_stations = json.load(fin)

    with open(galaxy_json, 'r', encoding='utf-8') as fin:
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
                key = station_key(system=data['name'], station=station)
                if key not in known_stations:
                    missing_stations.add(key)
            for body in data.get('bodies', []):
                for station in body.get('stations', []):
                    key = station_key(system=data['name'], station=station)
                    if key not in known_stations:
                        missing_stations.add(key)

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


def collect_modules_and_commodities():
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
    mod_dict, comm_dict = collect_modules_and_commodities()
    mapped = eddb_maps(eddb_session)

    comms = [
        SCommodity(
            id=x['commodityId'],
            name=x['name'],
            group_id=mapped['commodity_group'][x['category']]
        ) for x in sorted(comm_dict.values(), key=lambda x: x['commodityId'])
    ]
    with open(SPANSH_COMMODITIES, 'w', encoding='utf-8') as fout:
        pprint.pprint(comms, fout, indent=2)

    mods = []
    for mod in sorted(mod_dict.values(), key=lambda x: x['moduleId']):
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
        pprint.pprint(mods, fout, indent=2)


def transform_galaxy_json(number, total, galaxy_json):
    """
    Process number lines in the galaxy_json, skip all lines you aren't assigned.
    The output of this function is written to a series of files in the same folder
    as galaxy_json. See SPLIT_FILENAMES for the files written out.
    Every worker will write out to a separate file ending in it's number, example systems.json.09

    Args:
        number: Number assigned to worker, in range [0, total).
        total: The total number of jobs started.
        galaxy_json: The spansh galaxy_json
    """
    cnt = -1
    parent_dir = Path(galaxy_json).parent
    out_streams = {x: open(parent_dir / f'{x}.json.{number:02}', 'w', encoding='utf-8') for x in SPLIT_FILENAMES}
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session,\
         open(galaxy_json, 'r', encoding='utf-8') as fin:
        mapped = eddb_maps(eddb_session)

        try:
            for stream in out_streams.values():
                stream.write('[\n')

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

                system = transform_system(data=data, mapped=mapped)
                factions = transform_factions(data=data, mapped=mapped, system_id=system['id'])
                stations = transform_stations(data=data, mapped=mapped, system_id=system['id'])
                # FIXME: Need to better handle this
                #  stations.update(transform_bodies(data=data, mapped=mapped, system_id=system['id']))

                out_streams['systems'].write(str(system) + ',\n')

                for info in factions.values():
                    for data, output in [
                        (info.get('faction'), out_streams['factions']),
                        (info.get('influence'), out_streams['influences']),
                        (info.get('state'), out_streams['faction_states']),
                    ]:
                        if data:
                            output.write(str(data) + ',\n')

                for info in stations.values():
                    controlling_factions = info.get('controlling_factions', [])
                    if controlling_factions:
                        for control in controlling_factions:
                            out_streams['controlling_factions'].write(str(control) + ',\n')
                        del info['controlling_factions']

                    out_streams['stations'].write(str(info) + ',\n')
        finally:
            for stream in out_streams.values():
                stream.write('\n]')
                stream.close()


def dedupe_factions(faction_fnames, control_fnames, out_fname):
    """
    Given a list of files with faction information and a collection of controlling faction names,
    generate a single merged file with all unique faction information. Any name found in control factions
    that isn't present in faction information will be stubbed into the data.

    Factions to be put into database will be written out to: out_fname (should be factions.json.unique)
    Factions that are stubbed will be also written out to: factions.json.correct

    Args:
        faction_fnames: A list of filenames that contain extracted Faction information based on those present in system.
        control_fnames: A list of filenames that contain extracted control faction names owning stations.
        out_fname: The filename to write all information out to.

    Returns: The fname that unique factions were written to.
    """
    correct_fname = str(out_fname).replace('unique', 'correct')
    seen_factions = {}
    correct_factions = {}

    for fname in faction_fnames:
        with open(fname, 'r', encoding='utf-8') as fin:
            for faction in eval(fin.read()):
                seen_factions[faction['id']] = faction

    for fname in control_fnames:
        with open(fname, 'r', encoding='utf-8') as fin:
            for faction in eval(fin.read()):
                if faction['id'] not in seen_factions:
                    faction_stub = {
                        'id': faction['id'],
                        'name': faction['name'],
                    }
                    seen_factions[faction_stub['id']] = faction_stub
                    correct_factions[faction_stub['id']] = faction_stub

    with open(out_fname, 'w', encoding='utf-8') as fout:
        fout.write('[\n')
        for faction in seen_factions.values():
            fout.write(str(faction) + ',\n')
        fout.write('\n]')

    with open(correct_fname, 'w', encoding='utf-8') as correct:
        correct.write('[\n')
        for faction in correct_factions.values():
            correct.write(str(faction_stub) + ',\n')
        correct.write('\n]')


def dedupe_stations(fnames, galaxy_folder):
    """
    Possibility of multiple sightings of player fleet carriers in data.
    For each station present in the fnames file, keep only that station with the latest updateTime.
    All older data is discarded.

    Args:
        fnames: The list of filenames with station information.
        galaxy_folder: The folder that we should write out deduped kwargs information to.
    """
    stations = {}

    for fname in fnames:
        with open(fname, 'r', encoding='utf-8') as fin:
            for info in eval(fin.read()):
                try:
                    key = info['station']['id']
                except KeyError:
                    pass
                try:
                    if info['station']['updated_at'] > stations[key]['station']['updated_at']:
                        stations[key] = info
                except KeyError:
                    stations[key] = info

    module_id, commodity_id = 1, 1
    try:
        comm_suffix = 0
        comm_remaining = math.floor(len(stations) / COMMODITY_STATION_LIMIT)
        out_streams = {x: open(galaxy_folder / f'{x}.json.unique', 'w', encoding='utf-8') for x in STATION_KEYS}
        out_streams['commodities'] = open(galaxy_folder / "commodities.json.unique.00", 'w', encoding='utf-8')

        for stream in out_streams.values():
            stream.write('[\n')

        for ind, info in enumerate(stations.values(), start=1):
            out_streams['stations'].write(str(info['station']) + ',\n')
            out_streams['features'].write(str(info['features']) + ',\n')
            out_streams['economies'].write(str(info['economy']) + ',\n')
            for module in info['modules_sold']:
                module['id'] = module_id
                module_id += 1
                out_streams['modules'].write(str(module) + ',\n')
            for commodity in info['commodity_pricing']:
                commodity['id'] = commodity_id
                commodity_id += 1
                out_streams['commodities'].write(str(commodity) + ',\n')

            # Due to the size rollover the commodities file at 25k entries
            if (ind % COMMODITY_STATION_LIMIT) == 0 and comm_remaining:
                comm_remaining -= 1
                comm_suffix += 1
                out_streams['commodities'].write('\n]')
                out_streams['commodities'].close()
                out_streams['commodities'] = open(galaxy_folder / f"commodities.json.unique.{comm_suffix:02}", 'w', encoding='utf-8')
                out_streams['commodities'].write('[\n')

    finally:
        for stream in out_streams.values():
            stream.write('\n]')
            stream.close()


def bulk_insert_from_file(eddb_session, *, fname, cls):
    """
    Bulk insert all objects into database based on information from a file.

    Args:
        eddb_session: A session onto the EDDB.
        fname: The filename with kwargs of the objects, one per line.
        cls: The sqlalchemy database class to intantiate for each object.
    """
    with open(fname, 'r', encoding='utf-8') as fin:
        eddb_session.bulk_insert_mappings(cls, eval(fin.read()))
        eddb_session.commit()


def single_insert_from_file(eddb_session, *, fname, cls):
    """
    Single insert all objects into database based on information from a file.
    This will print every object first and then commit them individually to identify
    issues in bulk import.

    Args:
        eddb_session: A session onto the EDDB.
        fname: The filename with kwargs of the objects, one per line.
        cls: The sqlalchemy database class to intantiate for each object.
    """
    print('fname', fname)
    with open(fname, 'r', encoding='utf-8') as fin:
        rows = eval(fin.read())

        for row in rows:
            db_obj = cls(**row)
            print(db_obj)
            try:
                eddb_session.add(db_obj)
                eddb_session.commit()
            except sqla.exc.IntegrityError as exc:
                eddb_session.rollback()
                print(str(exc))


def import_galaxy_objects(number, folder):
    """
    Bulk import all transformed database objects from their expected files.
    This is the compliment of transform_galaxy_json.
    Note that factions must be imported separately and deduped.

    Args:
        number: The number of this particular process.
        folder: The folder where all temporary files were written out.
    """
    folder = Path(folder)
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        bulk_insert_from_file(eddb_session, fname=folder / f'systems.json.{number:02}', cls=System)
        bulk_insert_from_file(eddb_session, fname=folder / f'influences.json.{number:02}', cls=Influence)
        bulk_insert_from_file(eddb_session, fname=folder / f'faction_states.json.{number:02}', cls=FactionActiveState)


async def parallel_process(galaxy_json, *, jobs):
    """
    Parallel parse and import information from galaxy_json into the EDDB.

    Step 1: Transform the data from the large JSON to many smaller files, each
            file will store only the kwargs for one type of database object (i.e. System).
    Step 2: Collect all unique faction names from the transformed data, create a single file with
            all unique faction information. Load all this in bulk into the database.
    Step 3: Bulk insert all kwargs from systems, influences and faction states.
    Step 4: Reprocess the split stations files and combine entries into one unique dictionary.
            When combining keep only latest updated_at time for each station.
            Write these out to STATION_KEYS files so each bulk insertable.
    Step 5: Bulk insert from the files that were split in previous step.

    Args:
        galaxy_json: The path to the galaxy_json from spansh.
        jobs: The number of jobs to start.
    """
    loop = asyncio.get_event_loop()
    galaxy_folder = Path(galaxy_json).parent

    print_no_newline(f"Starting {jobs} jobs to process {Path(galaxy_json).name} ...")
    with cfut.ProcessPoolExecutor(jobs) as pool:
        futs = []
        for num in range(0, jobs):
            futs += [loop.run_in_executor(
                pool,
                transform_galaxy_json, num, jobs, GALAXY_JSON,
            )]

        await asyncio.wait(futs)

        # Factions have to be sorted into a unique file then bulk inserted early
        # This includes names of factions ONLY appearing in controllingFaction of stations
        print_no_newline(" Done!\nFiltering unique factions ...")
        unique_factions = galaxy_folder / 'factions.json.unique'
        await loop.run_in_executor(
            pool,
            dedupe_factions,
            [galaxy_folder / f'factions.json.{x:02}' for x in range(0, jobs)],
            [galaxy_folder / f'controlling_factions.json.{x:02}' for x in range(0, jobs)],
            unique_factions
        )
        print_no_newline(" Done!\nBulk inserting unique factions to db ...")
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            bulk_insert_from_file(eddb_session, fname=unique_factions, cls=Faction)

        print_no_newline(f" Done!\nStarting {jobs} jobs to import non station data ...")
        futs = []
        for num in range(0, jobs):
            futs += [loop.run_in_executor(
                pool,
                import_galaxy_objects, num, galaxy_folder
            )]

            await asyncio.wait(futs)

        #  Player carriers can be spotted multiple places, only keep oldest data
        print_no_newline(" Done!\nFiltering unique stations present ...")
        await loop.run_in_executor(
            pool,
            dedupe_stations,
            [galaxy_folder / f'stations.json.{x:02}' for x in range(0, jobs)],
            galaxy_folder
        )
        print_no_newline(" Done!\nImporting remaining station data ...")
        await loop.run_in_executor(
            pool,
            import_stations_data,
            galaxy_folder
        )

        print(" Done!\nAll operations complete.")


def import_stations_data(galaxy_folder):  # pragma: no cover
    """
    Do a final pass importing the split stations data in bulk.

    Args:
        galaxy_folder: The folder containing galaxy_json and all scratch files.
    """
    fnames = {x: galaxy_folder / f'{x}.json.unique' for x in STATION_KEYS}
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        bulk_insert_from_file(eddb_session, fname=fnames['stations'], cls=Station)
        bulk_insert_from_file(eddb_session, fname=fnames['features'], cls=StationFeatures)
        bulk_insert_from_file(eddb_session, fname=fnames['economies'], cls=StationEconomy)
        bulk_insert_from_file(eddb_session, fname=fnames['modules'], cls=SModuleSold)
        for fname in glob.glob(str(galaxy_folder / 'commodities.json.unique.*')):
            bulk_insert_from_file(eddb_session, fname=fname, cls=SCommodityPricing)


def cleanup_scratch_files(galaxy_folder):
    """
    Cleans up all temporary files that were created during parsing.

    Args:
        galaxy_folder: The folder used for scratch files during importing galaxy_json.
    """
    for pat in CLEANUP_GLOBS:
        for fname in glob.glob(os.path.join(galaxy_folder, pat)):
            os.remove(fname)


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
    Due to sheer size of SModuleSold and SCommodityPricing, it more efficient to drop.
    """
    sqla.orm.session.close_all_sessions()
    for table in (SCommodityPricing, SModuleSold):
        try:
            table.__table__.drop(cogdb.eddb_engine)
        except sqla.exc.OperationalError:
            pass
    Base.metadata.create_all(cogdb.eddb_engine)

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        for table in [SModule, SCommodity, SModuleGroup, SCommodityGroup]:
            eddb_session.query(table).delete()


def recreate_tables():
    """
    Recreate all tables in the spansh module, mainly for schema changes and testing.
    """
    sqla.orm.session.close_all_sessions()
    drop_tables()
    Base.metadata.create_all(cogdb.eddb_engine)


def determine_missing_keys(factions, systems, stations, *, mapped):
    """
    Compared the lists of found factions, system and station keys to those loaded in eddb_maps.
    Print out to stdout all missing keys.

    Args:
        factions: A list of all unique faction names found
        systems: A list of all unique system names found
        stations: A list of all unique station_key names found, see station_key function
        mapped: An instance of the mapping created by eddb_maps
    """
    missing = set(systems) - set(mapped['systems'].keys())
    pprint.pprint(list(sorted(missing)))

    missing = set(factions) - set(mapped['factions'].keys())
    pprint.pprint(list(sorted(missing)))

    missing = set(stations) - set(mapped['stations'].keys())
    pprint.pprint(list(sorted(missing)))


def make_parser():
    """
    Make the parser for command line usage.

    Returns: An instance of argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser(description="Spansh.co.uk Importer")
    parser.add_argument('--yes', '-y', action="store_true",
                        help='Skip confirmation.')
    parser.add_argument('--jobs', '-j', type=int, default=math.floor(os.cpu_count() * 1.5),
                        help='The number of jobs to run.')
    parser.add_argument('--fetch', '-f', action="store_true",
                        help='Fetch the latest spansh dumps.')
    parser.add_argument('--recreate-tables', '-r', dest="recreate", action="store_true",
                        help='Recreate all EDDB tables, spy_tables and spansh specific tables.')
    parser.add_argument('--caches', '-c', action="store_true",
                        help='Regenerate all caches for commodities, modules and unique ID maps.')
    parser.add_argument('--skip', '-k', action="store_true",
                        help='Skip parsing and importing latest spansh dump.')

    return parser


def download_with_progress(url, destination, *, description=None):  # pragma: no cover
    """
    Download a file with progress bar displayed in CLI.

    Args:
        url: The URL to download.
        destination: Where to put the downloaded file.

    Returns: The total size downloaded.
    """
    if not description:
        description = url

    with requests.get(url, timeout=60, stream=True) as resp:
        total_size_bytes = int(resp.headers.get('Content-Length', 0))
        chunk = 1024

        with tqdm.tqdm(desc=description, total=total_size_bytes, unit="iB", unit_scale=True, unit_divisor=chunk) as progress,\
             open(destination, 'wb') as fout:
            for data in resp.iter_content(chunk_size=chunk):
                progress.update(fout.write(data))

    return total_size_bytes


def extract_with_progress(source, destination, *, description=None, size=0):  # pragma: no cover
    """
    Extract a gzip compressed file with progress

    Args:
        url: The URL to download.
        destination: Where to put the downloaded file.

    Returns: destination
    """
    if not description:
        description = 'Extracting: ' + Path(source).name

    chunk = 1024
    with tqdm.tqdm(desc=description, unit="iB", total=size, unit_scale=True, unit_divisor=chunk) as progress,\
         gzip.open(source, 'rb') as fin,\
         open(destination, 'wb') as fout:

        while True:
            data = fin.read(chunk)
            if not data:
                break
            fout.write(data)
            progress.update(chunk)


def confirm_msg(args, query=True):  # pragma: no cover
    """
    Create a message the explains what will happen based on args.

    Args:
        args: A instance of argparse.Namespace.

    Returns: A string to print to user.
    """
    msg = "The following steps will take place.\n\n"
    if args.fetch:
        msg += """    Preserve the current galaxy_stations.json to a backup
    Download and extract the latest spansh dump
    Update the ID maps for the dump
"""
    if args.caches:
        msg += "    Update the cached modules and commodity information.\n"

    if args.recreate:
        msg += "    Recreate all EDDB, spy and spansh tables and preload data.\n"
    else:
        msg += "    Empty all EDDB, spy and spansh tables and preload data.\n"

    msg += """    Parse all the information present in current galaxy_stations.json
    Then replace the following possibly existing EDDB data with that information:
        cogdb.eddb.{System, Faction, Influence, Station, StationFeatures, StationEconomy, FactionActiveState}
        cogdb.spansh.{SModuleSold, SCommodityPricing}

"""

    if query:
        msg += "Please confirm with yes or no: "

    return msg


def fetch_galaxy_json():
    """
    Fetch and extract the latest dump from spansh.
    """
    if Path(GALAXY_JSON).exists():
        os.rename(GALAXY_JSON, GALAXY_JSON.replace('.json', '.json.bak'))
    compressed_json = f"{GALAXY_JSON}.gz"
    try:
        size = download_with_progress(GALAXY_URL, compressed_json)
    except KeyboardInterrupt:
        os.remove(compressed_json)
        raise
    extract_with_progress(compressed_json, GALAXY_JSON, size=size * GALAXY_COMPRESSION_RATE)
    os.remove(compressed_json)
    print_no_newline("\nUpdating ID maps based on new dump ...")
    update_all_name_maps(*collect_unique_names(GALAXY_JSON))
    print(" Done!")


def refresh_module_commodity_cache():
    """
    Update the module and commodity caches.
    """
    print_no_newline("Regenerating the commodity and module caches ...")
    empty_tables()
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        if not eddb_session.query(cogdb.eddb.PowerState).all():
            cogdb.eddb.preload_tables(eddb_session)
        if not eddb_session.query(cogdb.spy_squirrel.SpyShip).all():
            cogdb.spy_squirrel.preload_spy_tables(eddb_session)
        preload_tables(eddb_session, only_groups=True)
        generate_module_commodities_caches(eddb_session)
    empty_tables()
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        preload_tables(eddb_session, only_groups=False)
    print(" Done!")


def main():
    """ Main function. """
    start = datetime.datetime.utcnow()
    parser = make_parser()
    args = parser.parse_args()

    if args.yes:
        print(confirm_msg(args, query=False))
    else:
        confirm = input(confirm_msg(args)).lstrip().lower()
        print()
        if not confirm.startswith('y'):
            print("Aborting.")
            sys.exit(0)

    if args.fetch:
        fetch_galaxy_json()

    if args.caches:
        refresh_module_commodity_cache()
        return

    if args.recreate:
        print_no_newline("Recreating all EDDB tables ...")
        sys.stdout.flush()
        cogdb.eddb.recreate_tables()
        cogdb.spy_squirrel.recreate_tables()
        recreate_tables()
    else:
        print_no_newline("Emptying existing EDDB tables ...")
        sys.stdout.flush()
        empty_tables()
        cogdb.spy_squirrel.empty_tables()
        cogdb.eddb.empty_tables()

    print_no_newline(" Done!\nPreloading constant EDDB data ...")
    sys.stdout.flush()
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        cogdb.eddb.preload_tables(eddb_session)
        cogdb.spy_squirrel.preload_spy_tables(eddb_session)
        preload_tables(eddb_session)
    print(" Done!")

    if args.skip:
        return

    try:
        asyncio.new_event_loop().run_until_complete(
            parallel_process(
                GALAXY_JSON, jobs=args.jobs
            )
        )

        print("Time taken", datetime.datetime.utcnow() - start)
    finally:
        cleanup_scratch_files(Path(GALAXY_JSON).parent)

        #  mapped = eddb_maps(eddb_session)
        #  determine_missing_ids(eddb_session, mapped=mapped)

    # Mainly for experimenting
    #  with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        #  generate_module_commodities_caches(eddb_session)
        #  collect_other_types(eddb_session)

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
    # Ensure the database is created if first run.
    try:
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            eddb_session.query(SModuleGroup).all()
    except sqla.exc.ProgrammingError:
        Base.metadata.create_all(cogdb.eddb_engine)

    main()
