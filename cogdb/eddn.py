"""
Work to make a catcher for EDDN monitoring.
Will have the following parts:
    - Connect and monitor eddn messages.
    - Pick out messages we want and parse them
    - Update relevant bits of EDDB database.
"""
import logging
import sys
import datetime
import pprint
import time
import zlib

import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm
import zmq
try:
    import rapidjson as json
except ImportError:
    import json

import cogdb
import cogdb.eddb
from cogdb.eddb import (Conflict, ConflictState, Faction, Influence, System, Station,
                        StationEconomy, StationFeatures, FactionActiveState, FactionPendingState,
                        FactionRecoveringState)

EDDN_ADDR = "tcp://eddn.edcd.io:9500"
TIMEOUT = 600000
# Keys of form "softeareName $schemaRef"
SCHEMA_MAP = {
    #  "https://eddn.edcd.io/schemas/blackmarket/1": "BlackmarketMsg",
    #  "https://eddn.edcd.io/schemas/commodity/3": "CommodityMsg",
    "E:D Market Connector [Linux] https://eddn.edcd.io/schemas/journal/1": "EDMCJournal",
    "E:D Market Connector [Windows] https://eddn.edcd.io/schemas/journal/1": "EDMCJournal",
    #  "https://eddn.edcd.io/schemas/outfitting/2": "OutfitMsg",
    #  "https://eddn.edcd.io/schemas/shipyard/2": "ShipyardMsg",
}
TIME_STRP = "%Y-%m-%dT%H:%M:%SZ"
ALL_MSGS = '/tmp/msgs'
EDMC_JOURNAL = '/tmp/msgs_edmc_journal'
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
    def __init__(self, session, msg):
        self.msg = msg
        self.session = session
        self.parsed = {}
        self.db_objs = {}

    @property
    def header(self):
        return self.msg['header']

    @property
    def body(self):
        return self.msg['message']

    def parse_msg(self):
        """
        Perform whole message parsing.
        """
        with open(EDMC_JOURNAL, 'a') as fout:
            try:
                pprint.pprint(self.parse_system(), stream=fout)
                fout.write('#######################\n')
                pprint.pprint(self.parse_station(), stream=fout)
                fout.write('#######################\n')
                pprint.pprint(self.parse_factions(), stream=fout)
                fout.write('#######################\n')
                pprint.pprint(self.parse_conflicts(), stream=fout)
            except StopParsing:
                self.session.rollback()
            finally:
                fout.write('-----------------------\n')

        return self.parsed

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
            'updated_at': int(datetime.datetime.strptime(body['timestamp'], TIME_STRP).
                                                         replace(tzinfo=datetime.timezone.utc).timestamp())
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
            system['primary_economy_id'] =  MAPS['Economy'][body["SystemEconomy"].replace("$economy_", "")[:-1]]
        if "SystemSecondEconomy" in body:
            system['secondary_economy_id'] =  MAPS['Economy'][body["SystemSecondEconomy"].replace("$economy_", "")[:-1]]
        if "SystemFaction" in body:
            faction_id = self.session.query(Faction.id).filter(Faction.name == body["SystemFaction"]["Name"]).scalar()
            system['controlling_minor_faction_id'] = faction_id
        if "SystemSecurity" in body:
            security = body['SystemSecurity'].replace("$SYSTEM_SECURITY_", "").replace("$GAlAXY_MAP_INFO_", "")[:-1]
            system['security_id'] = MAPS["Security"][security]
        if "StarPos" in body:
            for key, dest in [(0, "x"), (1, "y"), (2, "z")]:
                system[dest] = body["StarPos"][key]

        self.parsed['system'] = system
        try:
            system_db = self.session.query(System).filter(System.name == system['name']).one()
            system_db.update(system)
            self.session.flush()
            system['id'] = system_db.id
            self.db_objs['system'] = system_db
        except sqla_orm.exc.NoResultFound as e:
            raise StopParsing() from e  # No interest in systems not in db

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
            'features': None,
            'name': body["StationName"],
            'system_id': system['id'],
            'updated_at': system['updated_at'],
        }
        station_features = None

        if "DistanceFromArrivalLS" in body:
            station['distance_to_star'] = round(body['DistanceFromArrivalLS'])
        if "StationEconomy" in body and "StationEconomies" in body:
            __import__('pprint').pprint(body["StationEconomies"])
            station['economies'] = [{
                'economy_id': MAPS['Economy'][ent["Name"].replace("$economy_", "")[:-1]],
                'proportion': ent["Proportion"],
                'primary': ent["Name"] == body["StationEconomy"],
            } for ent in body["StationEconomies"]]
        if "StationFaction" in body:
            station['controlling_minor_faction_id'] = self.session.query(Faction.id).filter(Faction.name == body['StationFaction']['Name']).scalar()
        if "StationServices" in body:
            station_features = {x: True if x in body["StationServices"] else False for x in STATION_FEATS}
        if "StationType" in body:
            station['type_id'] = MAPS["StationType"][body['StationType']]

        station['features'] = station_features
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
                raise ValueError("No Factions or system not parsed before.")
        except KeyError as e:
            raise ValueError("No Factions or system not parsed before.") from e

        influences, factions = [], {}
        faction_names = [x['Name'] for x in self.body['Factions']]
        faction_dbs = {x.name: x for x in self.session.query(Faction).filter(Faction.name.in_(faction_names)).all()}
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
                    faction[camel_to_c(key)] = [cls(**{
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
                    tracker[key] = self.session.query(Station.id).\
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
        # FIXME: Sort out some key issues remaining.
        self.session.flush()
        system = self.parsed['system']
        station = self.parsed['station']
        station_features = station.pop('features')
        station_economies = station.pop('economies')

        try:
            station_db = self.session.query(Station).\
                filter(Station.name == station['name'],
                       Station.system_id == station['system_id']).\
                one()
            station_db.update(station)
            station['id'] = station_db.id

            if station_features:
                station_features_db = self.session.query(StationFeatures).\
                    filter(StationFeatures.id == station_db.id).\
                    one()
                station_features_db.update(station_features)
                station_features['id'] = station_db.id
        except sqla_orm.exc.NoResultFound:
            __import__('pprint').pprint(station)
            station_db = Station(**station)
            self.session.add(station_db)
            __import__('pprint').pprint(station_db)
            self.session.commit()

            station_features_db['id'] = station_db.id
            station_features_db = StationFeatures(**station_features)
            self.session.add(station_features_db)
            self.session.flush()

        self.session.query(StationEconomy).filter(StationEconomy.id == station_db.id).delete()
        for econ in station_economies:
            econ['id'] = station_db.id
            self.session.add(StationEconomy(**econ))

        for faction in self.parsed['factions'].values():
            self.session.query(FactionActiveState).\
                filter(FactionActiveState.system_id == system['id'],
                       FactionActiveState.faction_id == faction['id']).\
                delete()
            self.session.query(FactionPendingState).\
                filter(FactionPendingState.system_id == system['id'],
                       FactionPendingState.faction_id == faction['id']).\
                delete()
            self.session.query(FactionRecoveringState).\
                filter(FactionRecoveringState.system_id == system['id'],
                       FactionRecoveringState.faction_id == faction['id']).\
                delete()
            for key in ("active_states", "pending_states", "recovering_states"):
                if key in faction:
                    self.session.add_all(faction[key])

            try:
                influence_db = self.session.query(Influence).\
                    filter(Influence.system_id == system['id'],
                           Influence.faction_id == faction['id']).\
                    one()
                influence_db.update(faction)
            except sqla_orm.exc.NoResultFound:
                self.session.add(Influence(
                    system_id=system['id'],
                    faction_id=faction['id'],
                    happiness_id=faction['happiness_id'],
                    influence=faction['influence'],
                    is_controlling_faction=faction['is_controlling_faction'],
                ))

        for conflict in self.parsed['conflicts']:
            try:
                conflict_db = self.session.query(Conflict).\
                    filter(Conflict.system_id == conflict['system_id'],
                           Conflict.faction1_id == conflict['faction1_id'],
                           Conflict.faction2_id == conflict['faction2_id']).\
                    one()
                conflict_db.update(conflict)
            except sqla_orm.exc.NoResultFound:
                conflict_db = Conflict(**conflict)
                self.session.add(conflict_db)


def camel_to_c(word):
    """
    Convert camel case to c case.

    Args:
        word: A string.

    Returns:
        A c case string.
    """
    n_word = word[0]

    for chara in word[1:]:
        if chara.isupper():
            n_word += '_'

        n_word += chara

    return n_word.lower()


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
    cls_name = SCHEMA_MAP[key]
    cls = getattr(sys.modules[__name__], cls_name)

    return cls(cogdb.EDDBSession(), msg)


def get_msgs(sub):
    """ Continuously receive messages and log them. """
    with open(ALL_MSGS, 'w') as fout:
        fout.write('')
    with open(EDMC_JOURNAL, 'w') as fout:
        fout.write('')

    while True:
        msg = sub.recv()

        if not msg:
            raise zmq.ZMQError("Sub problem.")

        msg = json.loads(zlib.decompress(msg).decode())
        with open(ALL_MSGS, 'a') as fout:
            pprint.pprint(msg, stream=fout)
            fout.write('-----------------------\n')
        try:
            parser = create_parser(msg)
            parser.parse_msg()
            parser.update_database()
        except KeyError as e:
            logging.getLogger(__name__).info("Exception: %s", str(e))
        except ValueError as e:
            logging.getLogger(__name__).info("System was not found: %s", str(e))


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


def main():
    """
    Connect to EDDN and begin ....
        accepting messages and parsing the info
        updating database entries based on new information
    """
    sub = zmq.Context().socket(zmq.SUB)
    sub.setsockopt(zmq.SUBSCRIBE, b'')
    sub.setsockopt(zmq.RCVTIMEO, TIMEOUT)

    try:
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
