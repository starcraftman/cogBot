"""
Work to make a catcher for EDDN monitoring.
Will have the following parts:
    - Connect and monitor eddn messages.
    - Pick out messages we want and parse them
    - Update relevant bits of EDDB database.
"""
import logging
import sys
import time
import zlib

import zmq
try:
    import simplejson as json
except ImportError:
    import json

import cog.util

EDDN_ADDR = "tcp://eddn.edcd.io:9500"
TIMEOUT = 600000
with open(cog.util.rel_to_abs("tests", "eddn_data", "journal")) as fin:
    JOURNAL = json.loads(fin.read())
SCHEMA_MAP = {
    #  "https://eddn.edcd.io/schemas/commodity/3": "parse_commodity",
    "https://eddn.edcd.io/schemas/journal/1": "parse_journal",
    #  "https://eddn.edcd.io/schemas/outfitting/2": "parse_outfit",
    #  "https://eddn.edcd.io/schemas/shipyard/2": "parse_shipyard",
}
EXAMPLE_JOURNAL = """{
      "$schemaRef": "https://eddn.edcd.io/schemas/journal/1",
      "header": {
        "gatewayTimestamp": "2020-08-03T11:03:25.661784Z",
        "softwareName": "E:D Market Connector [Windows]",
        "softwareVersion": "3.4.6.0",
        "uploaderID": "337ea068329694dde54f7b868cd6bc48e1622753"
      },
      "message": {
        "Body": "GCRV 1568 A",
        "BodyID": 2,
        "BodyType": "Star",
        "Conflicts": [
          {
            "Faction1": {
              "Name": "Future of Lan Gundi",
              "Stake": "Gagnan Hub",
              "WonDays": 0
            },
            "Faction2": {
              "Name": "Silver Bridge PLC",
              "Stake": "",
              "WonDays": 0
            },
            "Status": "active",
            "WarType": "war"
          }
        ],
        "Docked": true,
        "Factions": [
          {
            "ActiveStates": [
              {
                "State": "CivilLiberty"
              }
            ],
            "Allegiance": "Federation",
            "FactionState": "CivilLiberty",
            "Government": "Democracy",
            "Happiness": "$Faction_HappinessBand2;",
            "Influence": 0.086,
            "Name": "Independents of GCRV 1568"
          },
          {
            "ActiveStates": [
              {
                "State": "War"
              }
            ],
            "Allegiance": "Federation",
            "FactionState": "War",
            "Government": "Democracy",
            "Happiness": "$Faction_HappinessBand2;",
            "Influence": 0.121,
            "Name": "Future of Lan Gundi"
          },
          {
            "ActiveStates": [
              {
                "State": "Boom"
              },
              {
                "State": "War"
              }
            ],
            "Allegiance": "Federation",
            "FactionState": "War",
            "Government": "Corporate",
            "Happiness": "$Faction_HappinessBand2;",
            "Influence": 0.121,
            "Name": "Silver Bridge PLC"
          },
          {
            "Allegiance": "Independent",
            "FactionState": "None",
            "Government": "Corporate",
            "Happiness": "$Faction_HappinessBand2;",
            "Influence": 0.05,
            "Name": "GCRV 1568 Incorporated"
          },
          {
            "Allegiance": "Independent",
            "FactionState": "None",
            "Government": "Dictatorship",
            "Happiness": "$Faction_HappinessBand2;",
            "Influence": 0.055,
            "Name": "GCRV 1568 Focus"
          },
          {
            "Allegiance": "Independent",
            "FactionState": "None",
            "Government": "Corporate",
            "Happiness": "$Faction_HappinessBand2;",
            "Influence": 0.065,
            "Name": "GCRV 1568 Natural Interstellar"
          },
          {
            "Allegiance": "Independent",
            "FactionState": "None",
            "Government": "Dictatorship",
            "Happiness": "$Faction_HappinessBand2;",
            "Influence": 0.054,
            "Name": "GCRV 1568 Law Party"
          },
          {
            "ActiveStates": [
              {
                "State": "Boom"
              }
            ],
            "Allegiance": "Independent",
            "FactionState": "Boom",
            "Government": "Cooperative",
            "Happiness": "$Faction_HappinessBand2;",
            "Influence": 0.448,
            "Name": "Aseveljet",
            "RecoveringStates": [
              {
                "State": "PirateAttack",
                "Trend": 0
              }
            ]
          }
        ],
        "MarketID": 3700062976,
        "Population": 377684748,
        "PowerplayState": "Exploited",
        "Powers": [
          "Li Yong-Rui"
        ],
        "StarPos": [
          -33.90625,
          -63,
          -82.875
        ],
        "StarSystem": "GCRV 1568",
        "StationEconomies": [
          {
            "Name": "$economy_Carrier;",
            "Proportion": 1
          }
        ],
        "StationEconomy": "$economy_Carrier;",
        "StationFaction": {
          "Name": "FleetCarrier"
        },
        "StationGovernment": "$government_Carrier;",
        "StationName": "H8X-0VZ",
        "StationServices": [
          "dock",
          "autodock",
          "blackmarket",
          "commodities",
          "contacts",
          "exploration",
          "outfitting",
          "crewlounge",
          "rearm",
          "refuel",
          "repair",
          "shipyard",
          "engineer",
          "flightcontroller",
          "stationoperations",
          "stationMenu",
          "carriermanagement",
          "carrierfuel",
          "voucherredemption"
        ],
        "StationType": "FleetCarrier",
        "SystemAddress": 2862335641955,
        "SystemAllegiance": "Independent",
        "SystemEconomy": "$economy_Agri;",
        "SystemFaction": {
          "FactionState": "Boom",
          "Name": "Aseveljet"
        },
        "SystemGovernment": "$government_Cooperative;",
        "SystemSecondEconomy": "$economy_Industrial;",
        "SystemSecurity": "$SYSTEM_SECURITY_high;",
        "event": "Location",
        "timestamp": "2020-08-03T11:03:24Z"
      }
    }
"""


def parse_journal(msg):
    """
    Parse the information desired from a journal msg.

    Returns:
    """
    if "E:D Market Connector" not in msg['header']['softwareName']:
        return

    print(msg['header'])


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
