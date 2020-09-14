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
                        FactionActiveState, FactionPendingState, FactionRecoveringState)

EDDN_ADDR = "tcp://eddn.edcd.io:9500"
TIMEOUT = 600000
SCHEMA_MAP = {
    #  "https://eddn.edcd.io/schemas/commodity/3": "CommodityMsg",
    "https://eddn.edcd.io/schemas/journal/1": "JournalMsg",
    #  "https://eddn.edcd.io/schemas/outfitting/2": "OutfitMsg",
    #  "https://eddn.edcd.io/schemas/shipyard/2": "ShipyardMsg",
}
TIME_STRP = "%Y-%m-%dT%H:%M:%SZ"


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
    Continuously connect and get messages until user cancels.
    """
    maps = {
        'Allegiance': {x.eddn: x.id for x in session.query(cogdb.eddb.Allegiance).all()},
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


class JournalMsg():
    def __init__(self, msg, session):
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

    def parse_system(self):
        """
        Parse the system portion of an EDDN message and return anything present in dictionary.

        Returns: A system dict with parsed info.
        """
        body = self.body
        system = {
            'economies': [],
            'updated_at': datetime.datetime.strptime(body['timestamp'], TIME_STRP)
        }

        for (key, dest) in [("Population", "population"), ("StarSystem", "name")]:
            if key in body:
                system[dest] = body[key]

        if "SystemSecurity" in body:
            security = body['SystemSecurity'].replace("$SYSTEM_SECURITY_", "").replace("$GAlAXY_MAP_INFO_", "")[:-1]
            system['security_id'] = MAPS["Security"][security]
        if "SystemFaction" in body:
            faction_id = self.session.query(Faction.id).filter(Faction.name == body["SystemFaction"]["Name"]).scalar()
            system['controlling_minor_faction_id'] = faction_id
        for key in ("SystemEconomy", "SystemSecondEconomy"):
            if key in body:
                economy = body[key].replace("$economy_", "")[:-1]
                system['economies'] += [MAPS['Economy'][economy]]
        if "PowerplayState" in body:
            system["power_state_id"] = MAPS['PowerplayState'][body["PowerplayState"]]
        if "Powers" in body:
            system["power_id"] = [MAPS['Powers'][x] for x in body["Powers"]]
        if "StarPos" in body:
            for key, dest in [(0, "x"), (1, "y"), (2, "z")]:
                system[dest] = body["StarPos"][key]

        self.parsed['system'] = system
        try:
            system_db = self.session.query(System).filter(System.name == system['name']).one()
            system['id'] = system_db.id
            system_db.update(system)
            self.db_objs['system'] = system_db
        except sqla_orm.exc.NoResultFound:
            logging.getLogger(__name__).error("System not found.")

        return system

    def parse_station(self):
        body = self.body
        system = self.parsed['system']
        if 'Body' not in body or 'id' not in system:
            raise ValueError("Cannot determine station uniquely in db. Please fix.")

        station = {'system_id': system['id']}
        for (key, dest) in [("Body", "name")]:
            if key in body:
                station[dest] = body[key]

        if "StationType" in body:
            station['type_id'] = MAPS["StationType"][body['StationType']]
        if "DistanceFromArrivalLS" in body:
            station['distance_to_star'] = round(body['DistanceFromArrivalLS'])

        self.parsed['station'] = station
        try:
            __import__('pprint').pprint(station)
            station_db = self.session.query(Station).\
                filter(Station.name == station['name'],
                       Station.system_id == station['system_id']).\
                one()
            self.db_objs['station'] = station_db
        except sqla_orm.exc.NoResultFound:
            logging.getLogger(__name__).error("System not found.")

        return station

    def parse_factions(self):
        """
        Parse the factions listed in the EDMC message.
        """
        body = self.body
        factions = {}

        temp = {}
        for faction in body['Factions']:
            for key in ('Name', 'Influence'):
                if key in faction:
                    temp[key.lower()] = faction[key]

            for key in ("Allegiance", "Government"):
                if key in faction:
                    temp["{}_id".format(key.lower())] = MAPS[key][faction[key]]
            if "Happiness" in faction:
                temp['happiness_id'] = int(faction["Happiness"][-2])

            for key, cls in [("ActiveStates", FactionActiveState),
                             ("PendingStates", FactionPendingState),
                             ("RecoveringStates", FactionRecoveringState)]:
                if key in faction:
                    temp[camel_to_c(key)] = faction[key]

            factions[temp['name']] = temp
            temp = {}

        self.parsed['factions'] = factions

        return factions

    def parse_conflicts(self):
        conflicts = []

        if "Conflicts" not in self.body:
            return conflicts

        system_id = self.db_objs['system'].id
        for conflict in self.body['Conflicts']:
            tracker = {
                'system_id': system_id,
                'state_id': self.session.query(ConflictState.id).filter(ConflictState.eddn == conflict['Status']).scalar(),
                'type_id': self.session.query(ConflictState.id).filter(ConflictState.eddn == conflict['WarType']).scalar(),
                'faction1_id': self.session.query(Faction.id).filter(Faction.name == conflict['Faction1']['Name']).scalar(),
                'faction1_stake_id': conflict['Faction1']['Stake'],
                'faction1_days': int(conflict['Faction1']['WonDays']),
                'faction2_id': self.session.query(Faction.id).filter(Faction.name == conflict['Faction2']['Name']).scalar(),
                'faction2_stake_id': conflict['Faction2']['Stake'],
                'faction2_days': int(conflict['Faction2']['WonDays']),
            }

            for key in ('faction1_stake_id', 'faction2_stake_id'):
                if tracker[key]:
                    tracker[key] = self.session.query(Station.id).\
                        filter(Station.system_id == system_id, Station.name == tracker[key]).\
                        scalar()
                else:
                    tracker[key] = None

            conflicts += [tracker]

        self.parsed['conflicts'] = conflicts
        return conflicts


def get_msgs(sub):
    """ Continuously receive messages and log them. """
    while True:
        msg = sub.recv()

        if not msg:
            raise zmq.ZMQError("Sub problem.")

        msg = json.loads(zlib.decompress(msg).decode())
        try:
            func_name = SCHEMA_MAP[msg["$schemaRef"]]
            func = getattr(sys.modules[__name__], func_name)
            func(msg)
            #  msg_str = json.dumps(msg, indent=2, sort_keys=True)
        except KeyError:
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
