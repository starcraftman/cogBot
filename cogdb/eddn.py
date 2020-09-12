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

import zmq
try:
    import rapidjson as json
except ImportError:
    import json

import cogdb
import cogdb.eddb

EDDN_ADDR = "tcp://eddn.edcd.io:9500"
TIMEOUT = 600000
SCHEMA_MAP = {
    #  "https://eddn.edcd.io/schemas/commodity/3": "parse_commodity",
    "https://eddn.edcd.io/schemas/journal/1": "parse_journal",
    #  "https://eddn.edcd.io/schemas/outfitting/2": "parse_outfit",
    #  "https://eddn.edcd.io/schemas/shipyard/2": "parse_shipyard",
}
TIME_STRP = "%Y-%m-%dT%H:%M:%SZ"


def parse_conflicts(msg):
    pass

#  "Factions": [
#  {
#  "Allegiance": "Independent",
#  "FactionState": "None",
#  "Government": "Dictatorship",
#  "Happiness": "$Faction_HappinessBand2;",
#  "Influence": 0.044732,
#  "Name": "Natural Ahemakino Defence Party"
#  }
#  ],
def parse_factions(body):
    """ """
    factions = {}

    for faction in body['Factions']:
        for (key, dest) in [("Name", "name"), ("I", "name"), ("StationType", "tpye_id")]:
            if key in body:
                factions[dest] = body[key]

    return factions


def parse_station(body):
    """ """
    station = {}
    for (key, dest) in [("BodyID", "id"), ("Body", "name"), ("StationType", "tpye_id")]:
        if key in body:
            station[dest] = body[key]

    return station


def parse_system(body):
    """
    Parse the system portion of an EDDN message and return anything present in dictionary.

    Returns: A system dict with parsed info.
    """
    system = {}

    for (key, dest) in [("Population", "population"), ("StarSystem", "name")]:
        if key in body:
            system[dest] = body[key]

    if "StarPos" in body:
        for key, dest in [(0, "x"), (1, "y"), (2, "z")]:
            system[dest] = body["StarPos"][key]

    return system


def parse_journal(msg):
    """
    Parse the information desired from a journal msg.

    Returns:
        Test messages parsed.
    """
    if "E:D Market Connector" not in msg['header']['softwareName']:
        return "Not EDMC"

    body = msg['message']
    timestamp = datetime.datetime.strptime(body['timestamp'], TIME_STRP)

    station = parse_station(body)
    print(station)

    system = parse_system(body)
    print(system)

    return timestamp


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


if __name__ == "__main__":
    main()
