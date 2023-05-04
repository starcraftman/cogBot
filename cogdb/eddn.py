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
# Keys of form "softeareName $schemaRef"
SCHEMA_MAP = {
    #  "https://eddn.edcd.io/schemas/blackmarket/1": "BlackmarketMsg",
    #  "https://eddn.edcd.io/schemas/commodity/3": "CommodityMsg",
    "EDDiscovery https://eddn.edcd.io/schemas/journal/1": "EDMCJournal",
    "E:D Market Connector [Linux] https://eddn.edcd.io/schemas/journal/1": "EDMCJournal",
    "E:D Market Connector [Windows] https://eddn.edcd.io/schemas/journal/1": "EDMCJournal",
    #  "https://eddn.edcd.io/schemas/outfitting/2": "OutfitMsg",
    #  "https://eddn.edcd.io/schemas/shipyard/2": "ShipyardMsg",
}
LOG_FILE = "/tmp/eddn_log"
ALL_MSGS = '/tmp/msgs'
JOURNAL_MSGS = '/tmp/msgs_journal'
JOURNAL_CARS = '/tmp/msgs_journal_cars'
STATION_FEATS = [x for x in StationFeatures.__dict__ if
                 x not in ('id', 'station') and not x.startswith('_')]


class StopParsing(Exception):
    """
    Interrupt any further parsing for a variety of reasons.
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

    @property
    def system_is_useful(self):
        """
        Returns true if the system is in fact useful.
        ID is only set for systems tracked already in DB.
        """
        try:
            return self.parsed['system']['id']
        except KeyError:
            return False

    def parse_msg(self):
        """
        Perform whole message parsing.
        """
        log = logging.getLogger(__name__)
        try:
            log.info(pprint.pformat(self.parse_system()))

            parsed = self.parse_and_flush_carrier()
            if parsed:
                log_msg(self.msg, path=JOURNAL_CARS, fname=log_fname(self.msg))
                log.info(pprint.pformat(parsed))

            log.debug(pprint.pformat(self.parse_station()))
            log.debug(pprint.pformat(self.parse_factions()))
            log_msg(self.msg, path=JOURNAL_MSGS, fname=log_fname(self.msg))
            log.debug(pprint.pformat(self.parse_conflicts()))
        except StopParsing:
            self.eddb_session.rollback()

        return self.parsed

    def update_database(self):
        """
        Update the database based on parsed information.

        All parts are optional depending on how much was parsed.
        """
        if self.parsed.get('station'):
            self.flush_station_to_db()
        if self.parsed.get('factions'):
            self.flush_factions_to_db()
        if self.parsed.get('influences'):
            self.flush_influences_to_db()
        if self.parsed.get('conflicts'):
            self.flush_conflicts_to_db()
        self.eddb_session.commit()

    def parse_system(self):
        """
        Parse the system portion of an EDDN message and return anything present in dictionary.

        Raises:
            StopParsing: Could not parse a system or it was not of interest.
        """
        try:
            body = self.body
            if 'StarSystem' not in body:
                raise StopParsing("No StarSystem found or message malformed.")
        except KeyError as exc:
            raise StopParsing("No StarSystem found or message malformed.") from exc

        system = {
            'name': body['StarSystem'],
            'updated_at': self.timestamp
        }
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
            faction_id = self.eddb_session.query(Faction.id).filter(Faction.name == body["SystemFaction"]["Name"]).scalar()
            system['controlling_minor_faction_id'] = faction_id
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
        Update or insert ANY system that is currently mapped in EDDB_MAPS.

        Raises:
            StopParsing - There is no reason to continue parsing, system not of interest.
        """
        system = self.parsed['system']
        try:
            system['id'] = EDDB_MAPS['systems'][system['name']]
        except KeyError as exc:
            raise StopParsing('Ignoring system') from exc

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
        body = self.body
        if not ("StationType" in body and body["StationType"] == "FleetCarrier"):
            return None

        cid = body["StationName"]
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
            StopParsing: One or more prerequisites were not met to continue parsing.
        """
        try:
            body = self.body
            system = self.parsed['system']
            if 'StationName' not in body or 'id' not in system:
                raise StopParsing("No Station or system not parsed before.")
        except KeyError as exc:
            raise StopParsing("No Station or system not parsed before.") from exc

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
            station['controlling_minor_faction_id'] = self.eddb_session.query(Faction.id).filter(Faction.name == body['StationFaction']['Name']).scalar()
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
        """
        if not self.parsed.get('station'):
            raise StopParsing('Ignoring station.')

        station = self.parsed['station']
        station_features = station.pop('features')
        station_economies = station.pop('economies')
        if 'name' not in station or 'system_id' not in station:
            raise StopParsing('Ignoring station.')

        try:
            station_db = self.eddb_session.query(Station).\
                filter(Station.name == station['name'],
                       Station.system_id == station['system_id']).\
                one()
            station_db.update(**station)
            station['id'] = station_db.id

        except sqla_orm.exc.NoResultFound:
            try:
                station_key = cogdb.spansh.station_key(system=self.parsed['system'], station=station)
                station['id'] = EDDB_MAPS['stations'][station_key]
                station_db = Station(**station)
                self.eddb_session.add(station_db)
                self.eddb_session.flush()
            except KeyError as exc:
                raise StopParsing('Ignoring station.') from exc
        self.flushed += [station_db]

        try:
            if station_features:
                station_features_db = self.eddb_session.query(StationFeatures).\
                    filter(StationFeatures.id == station['id']).\
                    one()
                station_features_db.update(**station_features)
                station_features['id'] = station['id']
                self.eddb_session.flush()

        except sqla_orm.exc.NoResultFound:
            station_features['id'] = station['id']
            station_features_db = StationFeatures(**station_features)
            self.eddb_session.add(station_features_db)
            self.eddb_session.flush()
        self.flushed += [station_features_db]

        if station_economies:
            self.eddb_session.query(StationEconomy).filter(StationEconomy.id == station['id']).delete()
            for econ in station_economies:
                econ['id'] = station['id']
                self.eddb_session.add(StationEconomy(**econ))

        self.eddb_session.flush()

    def parse_factions(self):
        """
        Parse the factions listed in the EDMC message.

        Raises:
            StopParsing: One or more prerequisites were not met to continue parsing.
        """
        try:
            system = self.parsed['system']
            if 'Factions' not in self.body or 'id' not in system:
                raise StopParsing("No Factions or system not parsed before.")
        except KeyError as exc:
            raise StopParsing("No Factions or system not parsed before.") from exc

        influences, factions = [], {}
        for body_faction in self.body['Factions']:
            faction = {
                'id': EDDB_MAPS['factions'][body_faction['Name']],
                'name': body_faction['Name'],
                'updated_at': system['updated_at'],
            }
            for key in ("Allegiance", "Government"):
                if key in body_faction:
                    faction[f"{key.lower()}_id"] = MAPS[key][body_faction[key]]
            if "FactionState" in body_faction:
                faction["state_id"] = MAPS["FactionState"][body_faction["FactionState"]]
            factions[faction['name']] = faction

            influence = {
                'system_id': system['id'],
                'faction_id': faction['id'],
                'is_controlling_faction': EDDB_MAPS['factions'][body_faction['Name']] == system['controlling_minor_faction_id'],
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

        self.parsed['factions'] = factions
        self.parsed['influences'] = influences
        return factions, influences

    def flush_factions_to_db(self):
        """
        Flush factions information to db.
        """
        system = self.parsed['system']
        for faction in self.parsed['factions'].values():
            for cls in (FactionActiveState, FactionPendingState, FactionRecoveringState):
                self.eddb_session.query(cls).\
                    filter(cls.system_id == system['id'],
                           cls.faction_id == faction['id']).\
                    delete()
            self.eddb_session.flush()
            for key in ("active_states", "pending_states", "recovering_states"):
                if key in faction:
                    self.eddb_session.add_all(faction[key])
                    del faction[key]

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

        self.eddb_session.flush()

    def flush_influences_to_db(self):
        """
        Flush influences information to db.
        """
        for influence in self.parsed['influences']:
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

        self.eddb_session.flush()

    def parse_conflicts(self):
        """
        Parse any conflicts prsent in the message.
        """
        try:
            system = self.parsed['system']
            factions = self.parsed['factions']
            if 'Conflicts' not in self.body or 'id' not in system:
                raise StopParsing("No Conflicts or system not parsed before.")
        except KeyError as exc:
            raise StopParsing("No Conflicts or system not parsed before.") from exc

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
        maps['PowerplayState']['HomeSystem'] = maps['PowerplayState']['Controlled']
    except KeyError:
        pass

    return maps


def create_parser(msg):
    """
    Factory to create msg parsers.

    Raises:
        KeyError: When the class or part of msg is not supported.

    Returns:
        A parser ready to parse the message.
    """
    key = f"{msg['header']['softwareName']} {msg['$schemaRef']}"
    logging.getLogger(__name__).info("Schema Key: %s", key)
    cls_name = SCHEMA_MAP[key]
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
        log_msg(msg, path=ALL_MSGS, fname=log_fname(msg))
        try:
            # Drop messages with old timestamps
            if not timestamp_is_recent(msg):
                raise StopParsing

            parser = create_parser(msg)
            parser.parse_msg()

            if parser.system_is_useful:
                # TODO: Is this still needed?
                # Retry if lock timeout throws
                cnt = 3
                while cnt:
                    try:
                        parser.update_database()
                        cnt = 0
                    except sqla.exc.OperationalError:
                        parser.session.rollback()
                        cnt -= 1
        except KeyError:
            pass
            #  logging.getLogger(__name__).info("Exception: %s", str(e))
        except StopParsing:
            pass


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
        EDDB_MAPS = cogdb.spansh.eddb_maps(init_session)
except (sqla_orm.exc.NoResultFound, sqla.exc.ProgrammingError):
    MAPS = None


if __name__ == "__main__":
    main()
