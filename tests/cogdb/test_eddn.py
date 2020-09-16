"""
Tests for cogdb.eddn
"""
try:
    import rapidjson as json
except ImportError:
    import json

import cogdb.eddn


EXAMPLE_JOURNAL_CARRIER = """{
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
EXAMPLE_JOURNAL_STATION = """{
    "$schemaRef": "https://eddn.edcd.io/schemas/journal/1",
    "header": {
    "gatewayTimestamp": "2020-08-03T11:04:12.802484Z",
    "softwareName": "E:D Market Connector [Windows]",
    "softwareVersion": "4.0.4",
    "uploaderID": "e0dcd76cabca63a40bb58e97a5d98ce2efe0be10"
    },
    "message": {
    "Body": "Mattingly Port",
    "BodyID": 65,
    "BodyType": "Station",
    "Conflicts": [
        {
        "Faction1": {
            "Name": "Udegobo Silver Power Int",
            "Stake": "Haarsma Keep",
            "WonDays": 1
        },
        "Faction2": {
            "Name": "Revolutionary Mpalans Confederation",
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
        "Allegiance": "Federation",
        "FactionState": "None",
        "Government": "Corporate",
        "Happiness": "$Faction_HappinessBand2;",
        "Influence": 0.102386,
        "Name": "Ochosag Federal Company"
        },
        {
        "ActiveStates": [
            {
            "State": "Boom"
            }
        ],
        "Allegiance": "Federation",
        "FactionState": "Boom",
        "Government": "Democracy",
        "Happiness": "$Faction_HappinessBand2;",
        "Influence": 0.643141,
        "Name": "Social Ahemakino Green Party"
        },
        {
        "ActiveStates": [
            {
            "State": "War"
            }
        ],
        "Allegiance": "Federation",
        "FactionState": "War",
        "Government": "Corporate",
        "Happiness": "$Faction_HappinessBand2;",
        "Influence": 0.078529,
        "Name": "Udegobo Silver Power Int"
        },
        {
        "Allegiance": "Independent",
        "FactionState": "None",
        "Government": "Dictatorship",
        "Happiness": "$Faction_HappinessBand2;",
        "Influence": 0.014911,
        "Name": "Defence Party of Ahemakino"
        },
        {
        "ActiveStates": [
            {
            "State": "War"
            }
        ],
        "Allegiance": "Federation",
        "FactionState": "War",
        "Government": "Confederacy",
        "Happiness": "$Faction_HappinessBand2;",
        "Influence": 0.078529,
        "Name": "Revolutionary Mpalans Confederation"
        },
        {
        "Allegiance": "Independent",
        "FactionState": "None",
        "Government": "Corporate",
        "Happiness": "$Faction_HappinessBand2;",
        "Influence": 0.037773,
        "Name": "Ahemakino Bridge Organisation"
        },
        {
        "Allegiance": "Independent",
        "FactionState": "None",
        "Government": "Dictatorship",
        "Happiness": "$Faction_HappinessBand2;",
        "Influence": 0.044732,
        "Name": "Natural Ahemakino Defence Party"
        }
    ],
    "MarketID": 3229716992,
    "Population": 9165120,
    "PowerplayState": "Controlled",
    "Powers": [
        "Felicia Winters"
    ],
    "StarPos": [
        123.25,
        -3.21875,
        -97.4375
    ],
    "StarSystem": "Ahemakino",
    "StationAllegiance": "Federation",
    "StationEconomies": [
        {
        "Name": "$economy_Industrial;",
        "Proportion": 0.8
        },
        {
        "Name": "$economy_Refinery;",
        "Proportion": 0.2
        }
    ],
    "StationEconomy": "$economy_Industrial;",
    "StationFaction": {
        "FactionState": "Boom",
        "Name": "Social Ahemakino Green Party"
    },
    "StationGovernment": "$government_Democracy;",
    "StationName": "Mattingly Port",
    "StationServices": [
        "dock",
        "autodock",
        "blackmarket",
        "commodities",
        "contacts",
        "exploration",
        "missions",
        "outfitting",
        "crewlounge",
        "rearm",
        "refuel",
        "repair",
        "shipyard",
        "tuning",
        "engineer",
        "missionsgenerated",
        "flightcontroller",
        "stationoperations",
        "powerplay",
        "searchrescue",
        "materialtrader",
        "stationMenu",
        "shop"
    ],
    "StationType": "Coriolis",
    "SystemAddress": 6131367809730,
    "SystemAllegiance": "Federation",
    "SystemEconomy": "$economy_Industrial;",
    "SystemFaction": {
        "FactionState": "Boom",
        "Name": "Social Ahemakino Green Party"
    },
    "SystemGovernment": "$government_Democracy;",
    "SystemSecondEconomy": "$economy_Refinery;",
    "SystemSecurity": "$SYSTEM_SECURITY_high;",
    "event": "Location",
    "timestamp": "2020-08-03T11:04:11Z"
    }
}
"""


def test_camel_to_c():
    assert cogdb.eddn.camel_to_c("CamelCase") == "camel_case"


def test_create_id_maps():
    maps = cogdb.eddn.create_id_maps(cogdb.EDDBSession())

    assert 'Thargoid' in maps['Allegiance']


def test_journal_parse_system():
    msg = json.loads(EXAMPLE_JOURNAL_STATION)
    parser = cogdb.eddn.create_parser(msg)

    __import__('pprint').pprint(parser.parse_system())


def test_journal_parse_station():
    msg = json.loads(EXAMPLE_JOURNAL_STATION)
    parser = cogdb.eddn.create_parser(msg)

    parser.parse_system()
    __import__('pprint').pprint(parser.parse_station())


def test_journal_parse_factions():
    msg = json.loads(EXAMPLE_JOURNAL_STATION)
    parser = cogdb.eddn.create_parser(msg)

    parser.parse_system()
    parser.parse_station()
    __import__('pprint').pprint(parser.parse_factions())


def test_journal_parse_conflicts():
    msg = json.loads(EXAMPLE_JOURNAL_STATION)
    parser = cogdb.eddn.create_parser(msg)

    parser.parse_system()
    parser.parse_station()
    parser.parse_factions()
    __import__('pprint').pprint(parser.parse_conflicts())
    parser.parse_conflicts()[0]['faction2_stake_id'] is None
