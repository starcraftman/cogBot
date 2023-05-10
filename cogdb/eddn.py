"""
Work to make a catcher for EDDN monitoring.
Will have the following parts:
    - Connect and monitor eddn messages.
    - Pick out messages we want and parse them
    - Update relevant bits of EDDB database.
"""
import argparse
import datetime
import logging
import os
import pprint
import shutil
import sys
import time
import zlib

import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm
import zmq
try:
    import rapidjson as json
except ImportError:
    import json

import cog.util
from cog.util import TIME_STRP, TIME_STRP_MICRO
import cogdb
import cogdb.eddb
import cogdb.query
import cogdb.spansh
from cogdb.eddb import (Conflict, Faction, Influence, System, Station,
                        StationEconomy, StationFeatures, FactionActiveState, FactionPendingState,
                        FactionRecoveringState)

EDDN_ADDR = "tcp://eddn.edcd.io:9500"
TIMEOUT = 600000
# Keys of form $schemaRef"
SCHEMA_MAP = {
    #  "https://eddn.edcd.io/schemas/blackmarket/1": "BlackmarketMsg",
    #  "https://eddn.edcd.io/schemas/commodity/3": "CommodityMsg",
    "https://eddn.edcd.io/schemas/journal/1": "EDMCJournal",
    #  "https://eddn.edcd.io/schemas/outfitting/2": "OutfitMsg",
    #  "https://eddn.edcd.io/schemas/shipyard/2": "ShipyardMsg",
}
LOG_FILE = "/tmp/eddn_log"
ALL_MSGS = '/tmp/msgs'
JOURNAL_MSGS = '/tmp/msgs_journal'
JOURNAL_CARS = '/tmp/msgs_journal_cars'
STATION_FEATS = [x for x in StationFeatures.__dict__ if x not in ('id', 'station') and not x.startswith('_')]


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
    key = f"{system}{cogdb.spansh.SEP}{station['name']}"
    if station['type_id'] == 24:
        key = station['name']

    return key


class StopParsing(Exception):
    """
    Interrupt any further parsing of the msg.
    """


class SkipDatabaseFlush(Exception):
    """
    No need to perform any updates to database.
    """


class SchemaIgnored(Exception):
    """
    The schema is not supported or intentionally ignored.
    """


class EDMCJournal():
    """
    Parse an EDMC Journal message for pertinent information and update
    the database as possible. Not all elements are guaranteed so parse as possible.
    """
    def __init__(self, session, eddb_session, msg):
        self.msg = msg
        self.session = session
        self.eddb_session = eddb_session
        self.parsed = {}
        self.flushed = []

    @property
    def header(self):
        """ The header of the message. """
        return self.msg['header']

    @property
    def body(self):
        """ The body of the message. """
        return self.msg['message']

    @property
    def date_obj(self):
        """ The UTC timezone AWARE datetime object of the message. """
        try:
            parsed_time = datetime.datetime.strptime(self.body['timestamp'], TIME_STRP_MICRO)
        except ValueError:
            parsed_time = datetime.datetime.strptime(self.body['timestamp'], TIME_STRP)
        return parsed_time.replace(tzinfo=datetime.timezone.utc)

    @property
    def timestamp(self):
        """ The UTC timestamp of the message. """
        return int(self.date_obj.timestamp())

    def parse_msg(self):
        """
        Perform whole message parsing.
        """
        log = logging.getLogger(__name__)
        try:
            if 'Factions' in self.body:
                log.info("System %s: Parsing factions", self.body.get('StarSystem', 'Unknown System'))
                log.debug(pprint.pformat(self.parse_factions()))
                self.flush_factions_to_db()
                self.eddb_session.commit()

            system = self.parse_system()
            log.info("System %s: Parsing system", self.body.get('StarSystem', 'Unknown System'))
            log.info(pprint.pformat(system))

            if 'StationName' in self.body and "StationType" in self.body:
                log.info("System %s: Parsing station", self.body.get('StarSystem', 'Unknown System'))
                log.debug(pprint.pformat(self.parse_station()))

                if self.body["StationType"] == "FleetCarrier":
                    parsed = self.parse_and_flush_carrier()
                    log_msg(self.msg, path=JOURNAL_CARS, fname=log_fname(self.msg))
                    log.info(pprint.pformat(parsed))

            if 'Factions' in self.body:
                log.info("System %s: Parsing influences", self.body.get('StarSystem', 'Unknown System'))
                log.debug(pprint.pformat(self.parse_influence()))
                log_msg(self.msg, path=JOURNAL_MSGS, fname=log_fname(self.msg))
                if 'Conflicts' in self.body:
                    log.info("System %s: Parsing conflicts", self.body.get('StarSystem', 'Unknown System'))
                    log.debug(pprint.pformat(self.parse_conflicts()))

        except StopParsing:
            self.eddb_session.rollback()

        return self.parsed

    def update_database(self):
        """
        Update the database based on parsed information.

        All parts are optional depending on how much was parsed.

        Raises:
            SkipDatabaseFlush: Cancel any further processing of the flush.
        """
        if self.parsed.get('station'):
            self.flush_station_to_db()
        if self.parsed.get('influences'):
            self.flush_influences_to_db()
        if self.parsed.get('conflicts'):
            self.flush_conflicts_to_db()
        self.eddb_session.commit()

    def parse_system(self):
        """
        Parse the system portion of an EDDN message and return anything present in dictionary.

        Raises:
            StopParsing: No system information found.
            SkipDatabaseFlush: System was not of interest, no information required.
        """
        body = self.body
        if 'StarSystem' not in body:
            raise StopParsing("No StarSystem found.")

        try:
            system = {
                'name': body['StarSystem'],
                'id': MAPS['systems'][body['StarSystem']],
                'updated_at': self.timestamp
            }
        except KeyError as exc:
            raise SkipDatabaseFlush(f"Ignoring system: {body['StarSystem']}") from exc

        if "Population" in body:
            system['population'] = body["Population"]
        if "Powers" in body:
            if len(body["Powers"]) != 1:
                system["power_id"] = MAPS['Powers']["None"]
            else:
                system["power_id"] = MAPS['Powers'][body['Powers'][0]]
        if "PowerplayState" in body:
            system["power_state_id"] = MAPS['PowerplayState'][body["PowerplayState"]]
        if "SystemEconomy" in body and "SystemSecondEconomy" in body:
            system['primary_economy_id'] = MAPS['Economy'][body["SystemEconomy"].replace("$economy_", "")[:-1]]
        if "SystemSecondEconomy" in body:
            system['secondary_economy_id'] = MAPS['Economy'][body["SystemSecondEconomy"].replace("$economy_", "")[:-1]]
        if "SystemFaction" in body:
            try:
                system['controlling_minor_faction_id'] = MAPS['factions'][body["SystemFaction"]["Name"]]
            except KeyError:
                system['controlling_minor_faction_id'] = None
        if "SystemSecurity" in body:
            security = body['SystemSecurity'].replace("$SYSTEM_SECURITY_", "").replace("$GAlAXY_MAP_INFO_", "")[:-1]
            system['security_id'] = MAPS["Security"][security]
        if "StarPos" in body:
            for key, dest in [(0, "x"), (1, "y"), (2, "z")]:
                system[dest] = body["StarPos"][key]

        self.parsed['system'] = system
        self.flush_system_to_db()
        return system

    def flush_system_to_db(self):
        """
        Flush the system information to the database.
        Update or insert ANY system that is currently mapped in MAPS.
        """
        system = self.parsed['system']
        try:
            system_db = self.eddb_session.query(System).filter(System.name == system['name']).one()
            system_db.update(**system)
        except sqla_orm.exc.NoResultFound:
            system_db = System(**system)
            self.eddb_session.add(system_db)
        self.eddb_session.commit()
        self.flushed += [system_db]

    def parse_and_flush_carrier(self):
        """
        Parse carrier information if it is available.

        N.B. It will be flushed to db upon return to ensure survival.
        """
        cid = self.body["StationName"]
        system = self.parsed["system"]["name"]
        date = self.date_obj
        ids_dict = {cid: {'id': cid, 'system': system, 'updated_at': date.replace(tzinfo=None)}}

        if cogdb.query.track_ids_check(self.session, cid):
            cogdb.query.track_ids_update(self.session, ids_dict)
        elif cogdb.query.track_systems_computed_check(self.session, system):
            ids_dict[cid]['override'] = False
            cogdb.query.track_ids_update(self.session, ids_dict)
        self.session.commit()

        self.parsed["carriers"] = ids_dict[cid]
        logging.getLogger(__name__).info("Matched carrier: %s", cid)
        return ids_dict

    def parse_station(self):
        """
        Parse the station related information from the message if pressent.

        Side Effect: Will update the System database object as querying it is required.

        Raises:
            SkipDatabaseFlush: Could not parse the station, remaining information irrelevant.
        """
        body = self.body
        system = self.parsed.get('system')
        if not system or 'id' not in system:
            raise SkipDatabaseFlush("Station: system improperly parsed.")

        station = {
            'economies': [],
            'features': None,
            'name': body["StationName"],
            'system_id': system['id'],
            'updated_at': system['updated_at'],
        }

        if "DistanceFromArrivalLS" in body:
            station['distance_to_star'] = round(body['DistanceFromArrivalLS'])
        if "StationEconomy" in body and "StationEconomies" in body:
            for ent in body["StationEconomies"]:
                economy = {
                    'economy_id': MAPS['Economy'][ent["Name"].replace("$economy_", "")[:-1]],
                    'proportion': ent["Proportion"],
                    'primary': ent["Name"] == body["StationEconomy"],
                }
                # Seen on EDDN collisions with multiple same IDs and different proportions, keep first seen.
                if economy['economy_id'] not in [x['economy_id'] for x in station['economies']]:
                    station['economies'] += [economy]
        if "StationFaction" in body:
            try:
                station['controlling_minor_faction_id'] = MAPS['factions'][body["StationFaction"]["Name"]]
            except KeyError:
                station['controlling_minor_faction_id'] = None
        if "StationServices" in body:
            station['features'] = {x: x in body["StationServices"] for x in STATION_FEATS}
            station['features']['updated_at'] = station['updated_at']  # pylint: disable=unsupported-assignment-operation
        if "StationType" in body:
            station['type_id'] = MAPS["StationType"][body['StationType']]

        self.parsed['station'] = station
        return station

    def flush_station_to_db(self):
        """
        Flush the station information to db.
        Several cases:
            Station exists then update.
            Station is a fleet carrier and new, add it.
            Station is not a fleet carrier and new, ignore it.

        Raises:
            SkipDatabaseFlush: The station isn't valid to put in database, remaining info irrelevant.
        """
        station = self.parsed['station']
        station_features, station_economies = None, None
        try:
            station_features = station.pop('features')
        except KeyError:
            pass
        try:
            station_economies = station.pop('economies')
        except KeyError:
            pass

        if 'name' not in station or 'system_id' not in station or 'type_id' not in station:
            raise SkipDatabaseFlush("Station missing: name, type_id or system_id.")

        try:
            station_db = self.eddb_session.query(Station).\
                filter(Station.name == station['name'])

            if station['type_id'] != 24:  # Only filter system when not a fleet carrier
                station_db = station_db.filter(Station.system_id == station['system_id'])

            station_db = station_db.one()
            station_db.update(**station)
            station['id'] = station_db.id

        except sqla_orm.exc.NoResultFound:
            system_name = self.parsed['system']['name'] if self.parsed.get('system') else 'unknown'
            station.update({
                'is_planetary': False,
                'max_landing_pad_size': 'L',
            })
            try:
                key = station_key(system=self.parsed['system']['name'], station=station)
                logging.getLogger(__name__).error("StationKey: %s", key)
                station['id'] = MAPS['stations'][key]

                station_db = Station(**station)
                self.eddb_session.add(station_db)
            except KeyError as exc:
                if station['type_id'] != 24:
                    raise SkipDatabaseFlush(f"Ignoring station: {station['name']} ({system_name})") from exc

                logging.getLogger(__name__).warning("New Fleet Carrier: %s", station['name'])

                added = cogdb.spansh.update_station_map([station['name']], cache=STATION_CACHE)
                station['id'] = added[station['name']]
                MAPS['stations'][key] = added[station['name']]
                cogdb.spansh.write_station_cache(STATION_CACHE)

                station_db = Station(**station)
                self.eddb_session.add(station_db)

        self.eddb_session.commit()
        self.flushed += [station_db]

        try:
            if station_features:
                station_features_db = self.eddb_session.query(StationFeatures).\
                    filter(StationFeatures.id == station['id']).\
                    one()
                station_features_db.update(**station_features)
                station_features['id'] = station['id']

        except sqla_orm.exc.NoResultFound:
            station_features['id'] = station['id']
            station_features_db = StationFeatures(**station_features)
            self.eddb_session.add(station_features_db)
        self.flushed += [station_features_db]

        if station_economies:
            self.eddb_session.query(StationEconomy).filter(StationEconomy.id == station['id']).delete()
            for econ in station_economies:
                econ['id'] = station['id']
                self.eddb_session.add(StationEconomy(**econ))

    def parse_factions(self):
        """
        Parse the factions listed in the EDMC message.
        When new factions encountered, add them to the database.
        Factions are a pre-requisite in the database for systems, stations and influences.
        """
        factions = {}
        for body_faction in self.body.get('Factions', []):
            faction = {
                'name': body_faction['Name'],
                'updated_at': self.timestamp,
            }
            for key in ("Allegiance", "Government"):
                if key in body_faction:
                    faction[f"{key.lower()}_id"] = MAPS[key][body_faction[key]]
            if "FactionState" in body_faction:
                faction["state_id"] = MAPS["FactionState"][body_faction["FactionState"]]

            try:
                faction['id'] = MAPS['factions'][body_faction['Name']]
            except KeyError:
                # Faction not mapped, add it immediately, incurs write out cost
                added = cogdb.spansh.update_faction_map([body_faction['name']], cache=FACTION_CACHE)
                MAPS['factions'][body_faction['Name']] = added[body_faction['name']]
                faction['id'] = added[body_faction['name']]

            factions[faction['name']] = faction

        self.parsed['factions'] = factions
        return factions

    def flush_factions_to_db(self):
        """
        Flush factions information to db.
        """
        factions = self.parsed.get('factions', [])
        for faction in factions.values():
            try:
                faction_db = self.eddb_session.query(Faction).\
                    filter(Faction.id == faction['id']).\
                    one()
                faction_db.update(**faction)
            except sqla_orm.exc.NoResultFound:
                faction_db = Faction(**faction)
                self.eddb_session.add(faction_db)
            finally:
                self.flushed += [faction_db]

    def parse_influence(self):
        """
        Parse the influence and stateslisted in the journal message.
        """
        system = self.parsed.get('system')
        factions = self.parsed.get('factions')
        if not factions or not system or 'id' not in system:
            raise SkipDatabaseFlush("Influences: system or factions improperly parsed.")

        influences = []
        for body_faction in self.body['Factions']:
            faction = factions[body_faction['Name']]
            influence = {
                'system_id': system['id'],
                'faction_id': faction['id'],
                'is_controlling_faction': MAPS['factions'][body_faction['Name']] == system['controlling_minor_faction_id'],
                'updated_at': system['updated_at'],
            }
            if "Happiness" in body_faction and body_faction["Happiness"]:
                influence['happiness_id'] = int(body_faction["Happiness"][-2])
            if "Influence" in body_faction:
                influence["influence"] = body_faction["Influence"]
            influences += [influence]

            for key, cls in [("ActiveStates", FactionActiveState),
                             ("PendingStates", FactionPendingState),
                             ("RecoveringStates", FactionRecoveringState)]:
                if key in body_faction:
                    faction[cog.util.camel_to_c(key)] = [cls(**{
                        'system_id': system['id'],
                        'faction_id': faction['id'],
                        'state_id': MAPS['FactionState'][x['State']]
                    })
                        for x in body_faction[key]
                    ]

        self.parsed['influences'] = influences
        return influences

    def flush_influences_to_db(self):
        """
        Flush influences and states information to db.
        """
        system = self.parsed.get('system')
        influences = self.parsed.get('influences')
        if not influences or not system or 'id' not in system:
            raise SkipDatabaseFlush("Influences flush: system or factions improperly parsed.")

        for faction in self.parsed['factions'].values():
            for cls in (FactionActiveState, FactionPendingState, FactionRecoveringState):
                self.eddb_session.query(cls).\
                    filter(cls.system_id == system['id'],
                           cls.faction_id == faction['id']).\
                    delete()
            self.eddb_session.commit()
            for key in ("active_states", "pending_states", "recovering_states"):
                if key in faction:
                    self.eddb_session.add_all(faction[key])
                    del faction[key]

        for influence in influences:
            try:
                influence_db = self.eddb_session.query(Influence).\
                    filter(Influence.system_id == influence['system_id'],
                           Influence.faction_id == influence['faction_id']).\
                    one()
                influence_db.update(**influence)
            except sqla_orm.exc.NoResultFound:
                influence_db = Influence(**influence)
                self.eddb_session.add(Influence(**influence))
            finally:
                cogdb.eddb.add_history_influence(self.eddb_session, influence_db)
                self.flushed += [influence_db]

    def parse_conflicts(self):
        """
        Parse any conflicts prsent in the message.
        """
        system = self.parsed.get('system')
        factions = self.parsed.get('factions')
        if not system or not factions or 'id' not in system:
            raise SkipDatabaseFlush("Conflicts: system or factions improperly parsed.")

        conflicts = []
        for conflict in self.body['Conflicts']:
            tracker = {
                'system_id': system['id'],
                'status_id': MAPS['ConflictState'][conflict['Status']],
                'type_id': MAPS['ConflictState'][conflict['WarType']],
                'faction1_id': factions[conflict['Faction1']['Name']]['id'],
                'faction1_stake_id': conflict['Faction1']['Stake'],
                'faction1_days': int(conflict['Faction1']['WonDays']),
                'faction2_id': factions[conflict['Faction2']['Name']]['id'],
                'faction2_stake_id': conflict['Faction2']['Stake'],
                'faction2_days': int(conflict['Faction2']['WonDays']),
                'updated_at': system['updated_at'],
            }

            for key in ('faction1_stake_id', 'faction2_stake_id'):
                if tracker[key]:
                    tracker[key] = self.eddb_session.query(Station.id).\
                        filter(Station.system_id == system['id'], Station.name == tracker[key]).\
                        scalar()
                else:
                    tracker[key] = None

            conflicts += [tracker]

        self.parsed['conflicts'] = conflicts
        return conflicts

    def flush_conflicts_to_db(self):
        """
        Flush the information on active conflicts to the db.
        """
        for conflict in self.parsed['conflicts']:
            try:
                conflict_db = self.eddb_session.query(Conflict).\
                    filter(Conflict.system_id == conflict['system_id'],
                           Conflict.faction1_id == conflict['faction1_id'],
                           Conflict.faction2_id == conflict['faction2_id']).\
                    one()
                conflict_db.update(**conflict)
            except sqla_orm.exc.NoResultFound:
                conflict_db = Conflict(**conflict)
                self.eddb_session.add(conflict_db)
            finally:
                self.flushed += [conflict_db]


def create_id_maps(session):
    """
    Create a large universal map of maps that carries common items like
    "Allegiance" or "Government" onto the id expected to be inserted into the db.

    Returns: A adict of dicts that ultimately points to integer ids in the db.
    """
    maps = {
        'Allegiance': {x.eddn: x.id for x in session.query(cogdb.eddb.Allegiance)},
        'ConflictState': {x.eddn: x.id for x in session.query(cogdb.eddb.ConflictState)},
        'Economy': {x.eddn: x.id for x in session.query(cogdb.eddb.Economy)},
        'FactionState': {x.eddn: x.id for x in session.query(cogdb.eddb.FactionState)},
        'Government': {x.eddn: x.id for x in session.query(cogdb.eddb.Government)},
        'Happiness': {x.eddn: x.id for x in session.query(cogdb.eddb.FactionHappiness)},
        'Powers': {x.eddn: x.id for x in session.query(cogdb.eddb.Power)},
        'PowerplayState': {x.eddn: x.id for x in session.query(cogdb.eddb.PowerState)},
        'Security': {x.eddn: x.id for x in session.query(cogdb.eddb.Security)},
        'StationType': {x.eddn: x.id for x in session.query(cogdb.eddb.StationType)},
    }
    try:
        maps['PowerplayState'][''] = maps['PowerplayState']['None']
        maps['PowerplayState']['HomeSystem'] = maps['PowerplayState']['Controlled']
        maps['StationType']['Bernal'] = maps['StationType']['Ocellus']
        maps['Economy']['Engineer'] = maps['Economy']['Engineering']
    except KeyError:
        pass

    with open(cogdb.spansh.SYSTEM_MAPF, 'r', encoding='utf-8') as fin:
        maps['systems'] = json.load(fin)
    with open(cogdb.spansh.STATION_MAPF, 'r', encoding='utf-8') as fin:
        maps['stations'] = json.load(fin)
    with open(cogdb.spansh.FACTION_MAPF, 'r', encoding='utf-8') as fin:
        maps['factions'] = json.load(fin)
        maps['factions']['None'] = None

    return maps


def create_parser(msg):
    """
    Factory to create msg parsers.

    Raises:
        SchemaIgnored: When the schema of the message cannot be handled.

    Returns:
        A parser ready to parse the message.
    """
    key = f"{msg['$schemaRef']}"
    logging.getLogger(__name__).info("Schema Key: %s", key)
    try:
        cls_name = SCHEMA_MAP[key]
    except KeyError as exc:
        raise SchemaIgnored(f"Cannot handle schema: {key}") from exc
    cls = getattr(sys.modules[__name__], cls_name)

    with cogdb.session_scope(cogdb.Session) as session, \
         cogdb.session_scope(cogdb.EDDBSession, autoflush=False) as eddb_session:
        return cls(session, eddb_session, msg)


def log_fname(msg):
    """
    A unique filename for this message.
    """
    try:
        timestamp = msg['message']['timestamp']
    except KeyError:
        timestamp = msg['header']['gatewayTimestamp']

    schema = '_'.join(msg["$schemaRef"].split('/')[-2:])
    fname = f"{schema}_{timestamp}_{msg['header']['softwareName']}".strip()

    return cog.util.clean_fname(fname, replacement='_', replace_spaces=True)


def log_msg(obj, *, path, fname):
    """
    Log a msg to the right directory to track later if required.
    Silently ignore if directory unavailable.
    """
    try:
        with open(os.path.join(path, fname), 'a', encoding='utf-8') as fout:
            pprint.pprint(obj, stream=fout)
    except FileNotFoundError:
        pass


def timestamp_is_recent(msg, window=30):
    """ Returns true iff the timestamp is less than window minutes old."""
    try:
        parsed_time = datetime.datetime.strptime(msg['header']['gatewayTimestamp'], TIME_STRP_MICRO)
    except ValueError:
        try:
            parsed_time = datetime.datetime.strptime(msg['header']['gatewayTimestamp'], TIME_STRP)
        except ValueError:
            return False

    parsed_time = parsed_time.replace(tzinfo=datetime.timezone.utc)
    return (datetime.datetime.now(datetime.timezone.utc) - parsed_time) < datetime.timedelta(minutes=window)


def get_msgs(sub):  # pragma: no cover
    """ Continuously receive messages and log them. """
    while True:
        msg = sub.recv()

        if not msg:
            raise zmq.ZMQError("Sub problem.")

        msg = json.loads(zlib.decompress(msg).decode())
        try:
            # Drop messages with old timestamps
            if not timestamp_is_recent(msg):
                continue

            lfname = log_fname(msg)
            log_msg(msg, path=ALL_MSGS, fname=lfname)
            try:
                print("Message:", lfname)
                parser = create_parser(msg)
                parser.parse_msg()
            except SchemaIgnored:  # Schema not mapped
                continue
            except StopParsing:
                pass

            parser.update_database()
        except SkipDatabaseFlush as exc:
            logging.getLogger(__name__).info("SKIP: %s", exc)


def connect_loop(sub):  # pragma: no cover
    """
    Continuously connect and get messages until user cancels.
    All messages logged to file and printed.
    """
    while True:
        try:
            sub.connect(EDDN_ADDR)
            get_msgs(sub)
        except zmq.ZMQError as exc:
            logging.getLogger(__name__).info("ZMQ Socket error. Reconnecting soon.\n\n%s", exc)
            sub.discconect(EDDN_ADDR)
            time.sleep(5)


def eddn_log(fname, log_level="INFO"):
    """
    Create a simple file and stream logger for eddn separate from main bot's logging.
    """
    log = logging.getLogger(__name__)
    for hand in log.handlers:
        log.removeHandler(hand)
    log.setLevel(log_level)

    log_fmt = logging.Formatter(fmt="[%(levelname)-5.5s] %(asctime)s %(name)s.%(funcName)s()::%(lineno)s | %(message)s")
    handler = logging.handlers.RotatingFileHandler(fname, maxBytes=2 ** 20, backupCount=3, encoding='utf8')
    handler.setFormatter(log_fmt)
    handler.setLevel(log_level)
    handler.doRollover()
    log.addHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(log_fmt)
    handler.setLevel(log_level)
    log.addHandler(handler)


def create_args_parser():  # pragma: no cover
    """
    Return parser for using this component on command line.
    """
    parser = argparse.ArgumentParser(description="EDDN Listener")
    parser.add_argument('--level', '-l', default='DEBUG',
                        help='Set the overall logging level..')
    parser.add_argument('--all', '-a', action='store_true', dest='all_msgs',
                        help='Capture all messages intercepted.')

    return parser


def main():  # pragma: no cover
    """
    Connect to EDDN and begin ....
        accepting messages and parsing the info
        updating database entries based on new information
    """
    args = create_args_parser().parse_args()
    eddn_log(LOG_FILE, args.level)
    if not args.all_msgs:
        try:
            shutil.rmtree(ALL_MSGS)
        except OSError:
            pass

    sub = zmq.Context().socket(zmq.SUB)
    sub.setsockopt(zmq.SUBSCRIBE, b'')
    sub.setsockopt(zmq.RCVTIMEO, TIMEOUT)

    try:
        print(f"connection established, reading messages.\nOutput at: {LOG_FILE}")
        connect_loop(sub)
    except KeyboardInterrupt:
        msg = """Terminating ZMQ connection."""
        print(msg)


# Any time code run, need these dirs to write to
try:
    shutil.rmtree(ALL_MSGS)
except OSError:
    pass
try:
    shutil.rmtree(JOURNAL_MSGS)
except OSError:
    pass
try:
    shutil.rmtree(JOURNAL_CARS)
except OSError:
    pass
os.mkdir(ALL_MSGS)
os.mkdir(JOURNAL_MSGS)
os.mkdir(JOURNAL_CARS)
try:
    with cogdb.session_scope(cogdb.EDDBSession) as init_session:
        MAPS = create_id_maps(init_session)
    FACTION_CACHE = cogdb.spansh.create_faction_cache()
    STATION_CACHE = cogdb.spansh.create_station_cache()
except (sqla_orm.exc.NoResultFound, sqla.exc.ProgrammingError):
    MAPS = None


if __name__ == "__main__":
    main()
