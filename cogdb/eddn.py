"""
Work to make a catcher for EDDN monitoring.
Will have the following parts:
    - Connect and monitor eddn messages.
    - Pick out messages we want and parse them
    - Update relevant bits of EDDB database.
"""
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
import cogdb
import cogdb.eddb
import cogdb.query
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
TIME_STRP = "%Y-%m-%dT%H:%M:%SZ"
TIME_STRP_MICRO = "%Y-%m-%dT%H:%M:%S.%fZ"
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
    pass


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

    @property
    def header(self):
        return self.msg['header']

    @property
    def body(self):
        return self.msg['message']

    @property
    def date_obj(self):
        try:
            parsed_time = datetime.datetime.strptime(self.body['timestamp'], TIME_STRP_MICRO)
        except ValueError:
            parsed_time = datetime.datetime.strptime(self.body['timestamp'], TIME_STRP)
        return parsed_time.replace(tzinfo=datetime.timezone.utc)

    @property
    def timestamp(self):
        return int(self.date_obj.timestamp())

    def parse_msg(self):
        """
        Perform whole message parsing.

        Raises:
            StopParsing - No need to continue parsing.
        """
        log = logging.getLogger(__name__)
        try:
            log.info(pprint.pformat(self.parse_system()))

            parsed = self.parse_carrier()
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

    def parse_carrier(self):
        """
        Parse carrier information if it is available.
        """
        body = self.body
        if not ("StationType" in body and body["StationType"] == "FleetCarrier"):
            return None

        id = body["StationName"]
        system = self.parsed["system"]["name"]
        date = self.date_obj
        ids_dict = {id: {'id': id, 'system': system, 'updated_at': date}}

        if cogdb.query.track_ids_check(self.session, id):
            cogdb.query.track_ids_update(self.session, ids_dict)
        elif cogdb.query.track_systems_computed_check(self.session, system):
            ids_dict[id]['override'] = False
            cogdb.query.track_ids_update(self.session, ids_dict)
        self.session.commit()

        self.parsed["carriers"] = ids_dict[id]
        logging.getLogger(__name__).info("Matched carrier: %s", id)
        return ids_dict

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
        except KeyError as e:
            raise StopParsing("No StarSystem found or message malformed.") from e

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

        try:
            system_db = self.eddb_session.query(System).filter(System.name == system['name']).one()
            system_db.update(system)
            self.eddb_session.commit()
            system['id'] = system_db.id
        except sqla_orm.exc.NoResultFound as e:
            raise StopParsing() from e  # No interest in systems not in db

        self.parsed['system'] = system
        return system

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
        except KeyError as e:
            raise StopParsing("No Station or system not parsed before.") from e

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
            station['features'] = {x: True if x in body["StationServices"] else False for x in STATION_FEATS}
        if "StationType" in body:
            station['type_id'] = MAPS["StationType"][body['StationType']]

        self.parsed['station'] = station
        return station

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
        except KeyError as e:
            raise StopParsing("No Factions or system not parsed before.") from e

        influences, factions = [], {}
        faction_names = [x['Name'] for x in self.body['Factions']]
        faction_dbs = {x.name: x for x in self.eddb_session.query(Faction).filter(Faction.name.in_(faction_names)).all()}
        for body_faction in self.body['Factions']:
            faction = {
                'id': faction_dbs[body_faction['Name']].id,
                'name': body_faction['Name'],
                'updated_at': system['updated_at'],
            }
            for key in ("Allegiance", "Government"):
                if key in body_faction:
                    faction["{}_id".format(key.lower())] = MAPS[key][body_faction[key]]
            if "FactionState" in body_faction:
                faction["state_id"] = MAPS["FactionState"][body_faction["FactionState"]]
            factions[faction['name']] = faction

            influence = {
                'system_id': system['id'],
                'faction_id': faction['id'],
                'is_controlling_faction': faction_dbs[body_faction['Name']].id == system['controlling_minor_faction_id'],
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

    def parse_conflicts(self):
        try:
            system = self.parsed['system']
            factions = self.parsed['factions']
            if 'Conflicts' not in self.body or 'id' not in system:
                raise StopParsing("No Conflicts or system not parsed before.")
        except KeyError as e:
            raise StopParsing("No Conflicts or system not parsed before.") from e

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

    def update_database(self):
        """
        Update the database based on parsed information.

        This is called to finalize flushing the parsed information to the database.
        """
        self.eddb_session.rollback()  # Clear any previous db objects
        system = self.parsed['system']

        try:
            station = self.parsed['station']
            station_features = station.pop('features')
            station_economies = station.pop('economies')
            station_db = self.eddb_session.query(Station).\
                filter(Station.name == station['name'],
                       Station.system_id == station['system_id']).\
                one()
            station_db.update(station)
            station['id'] = station_db.id

            if station_features:
                station_features_db = self.eddb_session.query(StationFeatures).\
                    filter(StationFeatures.id == station_db.id).\
                    one()
                station_features_db.update(station_features)
                station_features['id'] = station_db.id

        except sqla_orm.exc.NoResultFound:
            station_db = Station(**station)
            self.eddb_session.add(station_db)
            self.eddb_session.flush()

            station_features['id'] = station_db.id
            station_features_db = StationFeatures(**station_features)
            self.eddb_session.add(station_features_db)

        except KeyError:
            pass
        self.eddb_session.flush()

        if 'economies' in self.parsed['station'] and station_economies:
            self.eddb_session.query(StationEconomy).filter(StationEconomy.id == station_db.id).delete()
            for econ in station_economies:
                econ['id'] = station_db.id
                self.eddb_session.add(StationEconomy(**econ))
            self.eddb_session.flush()

        if 'factions' in self.parsed and self.parsed['factions']:
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

                try:
                    influence_db = self.eddb_session.query(Influence).\
                        filter(Influence.system_id == system['id'],
                               Influence.faction_id == faction['id']).\
                        one()
                    influence_db.update(faction)
                except sqla_orm.exc.NoResultFound:
                    self.eddb_session.add(Influence(
                        system_id=system['id'],
                        faction_id=faction['id'],
                        happiness_id=faction['happiness_id'],
                        influence=faction['influence'],
                        is_controlling_faction=faction['is_controlling_faction'],
                    ))

        if 'conflicts' in self.parsed and self.parsed['conflicts']:
            for conflict in self.parsed['conflicts']:
                try:
                    conflict_db = self.eddb_session.query(Conflict).\
                        filter(Conflict.system_id == conflict['system_id'],
                               Conflict.faction1_id == conflict['faction1_id'],
                               Conflict.faction2_id == conflict['faction2_id']).\
                        one()
                    conflict_db.update(conflict)
                except sqla_orm.exc.NoResultFound:
                    conflict_db = Conflict(**conflict)
                    self.eddb_session.add(conflict_db)


def create_id_maps(session):
    """
    Create a large universal map of maps that carries common items like
    "Allegiance" or "Government" onto the id expected to be inserted into the db.

    Returns: A adict of dicts that ultimately points to integer ids in the db.
    """
    maps = {
        'Allegiance': {x.eddn: x.id for x in session.query(cogdb.eddb.Allegiance).all()},
        'ConflictState': {x.eddn: x.id for x in session.query(cogdb.eddb.ConflictState).all()},
        'Economy': {x.eddn: x.id for x in session.query(cogdb.eddb.Economy).all()},
        'FactionState': {x.eddn: x.id for x in session.query(cogdb.eddb.FactionState).all()},
        'Government': {x.eddn: x.id for x in session.query(cogdb.eddb.Government).all()},
        'Happiness': {x.eddn: x.id for x in session.query(cogdb.eddb.FactionHappiness).all()},
        'Powers': {x.eddn: x.id for x in session.query(cogdb.eddb.Power).all()},
        'PowerplayState': {x.eddn: x.id for x in session.query(cogdb.eddb.PowerState).all()},
        'Security': {x.eddn: x.id for x in session.query(cogdb.eddb.Security).all()},
        'StationType': {x.eddn: x.id for x in session.query(cogdb.eddb.StationType).all()},
    }
    maps['PowerplayState']['HomeSystem'] = maps['PowerplayState']['Controlled']

    return maps


def create_parser(msg):
    """
    Factory to create msg parsers.

    Raises:
        KeyError: When the class or part of msg is not supported.

    Returns:
        A parser ready to parse the message.
    """
    key = "{} {}".format(msg['header']['softwareName'], msg["$schemaRef"])
    logging.getLogger(__name__).info("Schema Key: %s", key)
    cls_name = SCHEMA_MAP[key]
    cls = getattr(sys.modules[__name__], cls_name)

    return cls(cogdb.Session(), cogdb.EDDBSession(autoflush=False), msg)


def log_fname(msg):
    """
    A unique filename for this message.
    """
    try:
        timestamp = msg['message']['timestamp']
    except KeyError:
        timestamp = msg['header']['gatewayTimestamp']

    schema = '_'.join(msg["$schemaRef"].split('/')[-2:])
    fname = "{}_{}_{}".format(schema, timestamp, msg['header']['softwareName'])

    return cog.util.clean_text(fname)


def log_msg(obj, *, path, fname):
    """
    Log a msg to the right directory to track later if required.
    """
    with open(os.path.join(path, fname), 'a') as fout:
        pprint.pprint(obj, stream=fout)


def timestamp_is_recent(msg, window=30):
    """ Returns true iff the timestamp is less than window minutes old."""
    try:
        parsed_time = datetime.datetime.strptime(msg['header']['gatewayTimestamp'], TIME_STRP_MICRO)
    except ValueError:
        parsed_time = datetime.datetime.strptime(msg['header']['gatewayTimestamp'], TIME_STRP)
    parsed_time = parsed_time.replace(tzinfo=datetime.timezone.utc)
    return (datetime.datetime.now(datetime.timezone.utc) - parsed_time) < datetime.timedelta(minutes=window)


def get_msgs(sub):
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

            # Retry if lock timeout throws
            cnt = 3
            while cnt:
                try:
                    parser.update_database()
                    parser.session.commit()
                    cnt = 0
                except sqla.exc.OperationalError:
                    parser.session.rollback()
                    cnt -= 1
        except KeyError as e:
            pass
            #  logging.getLogger(__name__).info("Exception: %s", str(e))
        except StopParsing:
            pass


def connect_loop(sub):
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


def eddn_log(fname, stream_level="INFO"):
    """
    Create a simple file and stream logger for eddn separate from main bot's logging.
    """
    log = logging.getLogger(__name__)
    for hand in log.handlers:
        log.removeHandler(hand)
    log.setLevel("DEBUG")

    format = logging.Formatter(fmt="[%(levelname)-5.5s] %(asctime)s %(name)s.%(funcName)s()::%(lineno)s | %(message)s")
    handler = logging.handlers.RotatingFileHandler(fname, maxBytes=2 ** 20, backupCount=3, encoding='utf8')
    handler.setFormatter(format)
    handler.setLevel("DEBUG")
    handler.doRollover()
    log.addHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(format)
    handler.setLevel(stream_level)
    log.addHandler(handler)


def main():
    """
    Connect to EDDN and begin ....
        accepting messages and parsing the info
        updating database entries based on new information
    """
    level = "DEBUG" if not len(sys.argv) == 2 else sys.argv[1]
    eddn_log(LOG_FILE, level)

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

    sub = zmq.Context().socket(zmq.SUB)
    sub.setsockopt(zmq.SUBSCRIBE, b'')
    sub.setsockopt(zmq.RCVTIMEO, TIMEOUT)

    try:
        print("connection established, reading messages.\nOutput at: {}".format(LOG_FILE))
        connect_loop(sub)
    except KeyboardInterrupt:
        msg = """Terminating ZMQ connection."""
        print(msg)


try:
    MAPS = create_id_maps(cogdb.EDDBSession())
except (sqla_orm.exc.NoResultFound, sqla.exc.ProgrammingError):
    MAPS = None


if __name__ == "__main__":
    main()
