"""
Tests for cogdb.eddn
"""
import pathlib
import shutil
import tempfile

try:
    import rapidjson as json
except ImportError:
    import json

import cogdb.eddn
import cogdb.schema
from cogdb.schema import TrackByID
from cogdb.eddb import FactionActiveState


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
EXAMPLE_CARRIER_DISC = """{
  "$schemaRef": "https://eddn.edcd.io/schemas/journal/1",
  "header": {
    "gatewayTimestamp": "2021-05-18T00:34:39.006381Z",
    "softwareName": "EDDiscovery",
    "softwareVersion": "12.0.2.0",
    "uploaderID": "e2e46eabd77f4eea0f8cd655183b4d980fb08338"
  },
  "message": {
    "Body": "Cha Eohm XN-X a69-1",
    "BodyID": 0,
    "BodyType": "Star",
    "Docked": true,
    "MarketID": 3703705600,
    "Population": 0,
    "StarPos": [
      -9207.15625,
      -39.9375,
      58557.125
    ],
    "StarSystem": "Nanomam",
    "StationEconomies": [
      {
        "Name": "$economy_Carrier;",
        "Proportion": 1.0
      }
    ],
    "StationEconomy": "$economy_Carrier;",
    "StationFaction": {
      "Name": "FleetCarrier"
    },
    "StationGovernment": "$government_Carrier;",
    "StationName": "KLG-9TL",
    "StationServices": [
      "dock",
      "autodock",
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
      "carrierfuel"
    ],
    "StationType": "FleetCarrier",
    "SystemAddress": 21970368135760,
    "SystemAllegiance": "",
    "SystemEconomy": "$economy_None;",
    "SystemGovernment": "$government_None;",
    "SystemSecondEconomy": "$economy_None;",
    "SystemSecurity": "$GAlAXY_MAP_INFO_state_anarchy;",
    "event": "Location",
    "timestamp": "2021-05-20T19:03:20.11111Z"
  }
}"""
EXAMPLE_CARRIER_EDMC = """{
  "$schemaRef": "https://eddn.edcd.io/schemas/journal/1",
  "header": {
    "gatewayTimestamp": "2021-05-18T00:34:42.526845Z",
    "softwareName": "E:D Market Connector [Windows]",
    "softwareVersion": "5.0.1",
    "uploaderID": "70787c46bbd4497e1af3c5f04609be60f09d0835"
  },
  "message": {
    "Body": "Prua Phoe EQ-Z b45-7 A",
    "BodyID": 1,
    "BodyType": "Star",
    "Docked": true,
    "MarketID": 3701618176,
    "Population": 0,
    "StarPos": [
      -5497.5625,
      -462.3125,
      11445.25
    ],
    "StarSystem": "Prua Phoe EQ-Z b45-7",
    "StationEconomies": [
      {
        "Name": "$economy_Carrier;",
        "Proportion": 1.0
      }
    ],
    "StationEconomy": "$economy_Carrier;",
    "StationFaction": {
      "Name": "FleetCarrier"
    },
    "StationGovernment": "$government_Carrier;",
    "StationName": "OVE-111",
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
    "SystemAddress": 15990296033161,
    "SystemAllegiance": "",
    "SystemEconomy": "$economy_None;",
    "SystemGovernment": "$government_None;",
    "SystemSecondEconomy": "$economy_None;",
    "SystemSecurity": "$GAlAXY_MAP_INFO_state_anarchy;",
    "event": "Location",
    "odyssey": false,
    "timestamp": "2021-05-20T19:03:20.11111Z"
    }
}"""


def test_create_id_maps(eddb_session):
    maps = cogdb.eddn.create_id_maps(eddb_session)
    assert 'Thargoid' in maps['Allegiance']


def test_edmcjournal_system_is_useful():
    msg = json.loads(EXAMPLE_JOURNAL_STATION)
    parser = cogdb.eddn.create_parser(msg)
    parser.parse_msg()
    assert parser.system_is_useful


def test_edmcjournal_parse_msg():
    msg = json.loads(EXAMPLE_JOURNAL_STATION)
    parser = cogdb.eddn.create_parser(msg)
    result = parser.parse_msg()

    assert result['system']
    assert result['station']
    assert result['factions']
    assert result['influences']
    assert result['conflicts']


def test_edmcjournal_parse_and_flush_carrier_edmc_id(session, f_track_testbed):
    msg = json.loads(EXAMPLE_CARRIER_EDMC)
    parser = cogdb.eddn.create_parser(msg)
    parser.parsed['system'] = {
        "name": "Rana",
        "updated_at": "2021-05-20T19:03:20.11111Z",
    }

    result = parser.parse_and_flush_carrier()

    id = 'OVE-111'
    expected = {
        id: {
            'id': id,
            'system': 'Rana',
            'updated_at': parser.date_obj,
        }
    }
    assert result == expected

    session.commit()
    tracked = session.query(TrackByID).filter(TrackByID.id == id).one()
    assert tracked.system == "Rana"

    parser.session.rollback()
    parser.eddb_session.rollback()


def test_edmcjournal_parse_and_flush_carrier_disc_system(session, f_track_testbed):
    msg = json.loads(EXAMPLE_CARRIER_DISC)
    parser = cogdb.eddn.create_parser(msg)
    parser.parsed['system'] = {
        "name": "Nanomam",
        "updated_at": "2021-05-20 19:03:20",
    }

    result = parser.parse_and_flush_carrier()

    id = 'KLG-9TL'
    expected = {
        id: {
            'id': 'KLG-9TL',
            'override': False,
            'system': 'Nanomam',
            'updated_at': parser.date_obj,
        }
    }
    assert result == expected

    session.commit()
    tracked = session.query(TrackByID).filter(TrackByID.id == id).one()
    assert tracked.system == "Nanomam"

    parser.session.rollback()
    parser.eddb_session.rollback()


def test_edmcjournal_parse_system():
    expected = {
        'controlling_minor_faction_id': 55925,
        'id': 569,
        'name': 'Ahemakino',
        'population': 9165120,
        'power_id': 6,
        'power_state_id': 16,
        'primary_economy_id': 4,
        'secondary_economy_id': 6,
        'security_id': 48,
        'updated_at': 1596452651,
        'x': 123.25,
        'y': -3.21875,
        'z': -97.4375
    }
    msg = json.loads(EXAMPLE_JOURNAL_STATION)
    parser = cogdb.eddn.create_parser(msg)

    result = parser.parse_system()

    #  __import__('pprint').pprint(result)
    assert result == expected
    parser.eddb_session.rollback()


def test_edmcjournal_parse_station():
    expected = {
        'controlling_minor_faction_id': 55925,
        'economies': [{'economy_id': 4, 'primary': True, 'proportion': 0.8},
                      {'economy_id': 6, 'primary': False, 'proportion': 0.2}],
        'features': {'blackmarket': True,
                     'commodities': True,
                     'dock': True,
                     'market': False,
                     'outfitting': True,
                     'rearm': True,
                     'refuel': True,
                     'repair': True,
                     'shipyard': True,
                     'update': False},
        'name': 'Mattingly Port',
        'system_id': 569,
        'type_id': 3,
        'updated_at': 1596452651
    }
    msg = json.loads(EXAMPLE_JOURNAL_STATION)
    parser = cogdb.eddn.create_parser(msg)

    parser.parse_system()
    result = parser.parse_station()

    #  __import__('pprint').pprint(result)
    assert result == expected
    parser.eddb_session.rollback()


def test_edmcjournal_parse_factions():
    expect = ({
        'Ahemakino Bridge Organisation': {'allegiance_id': 4,
                                          'government_id': 64,
                                          'id': 55927,
                                          'name': 'Ahemakino Bridge Organisation',
                                          'state_id': 80,
                                          'updated_at': 1596452651},
        'Defence Party of Ahemakino': {'allegiance_id': 4,
                                       'government_id': 112,
                                       'id': 55926,
                                       'name': 'Defence Party of Ahemakino',
                                       'state_id': 80,
                                       'updated_at': 1596452651},
        'Natural Ahemakino Defence Party': {'allegiance_id': 4,
                                            'government_id': 112,
                                            'id': 55928,
                                            'name': 'Natural Ahemakino Defence Party',
                                            'state_id': 80,
                                            'updated_at': 1596452651},
        'Ochosag Federal Company': {'allegiance_id': 3,
                                    'government_id': 64,
                                    'id': 26800,
                                    'name': 'Ochosag Federal Company',
                                    'state_id': 80,
                                    'updated_at': 1596452651},
        'Revolutionary Mpalans Confederation': {'active_states': [FactionActiveState(system_id=569, faction_id=58194, state_id=73)],
                                                'allegiance_id': 3,
                                                'government_id': 48,
                                                'id': 58194,
                                                'name': 'Revolutionary Mpalans '
                                                        'Confederation',
                                                'state_id': 73,
                                                'updated_at': 1596452651},
        'Social Ahemakino Green Party': {'active_states': [FactionActiveState(system_id=569, faction_id=55925, state_id=16)],
                                         'allegiance_id': 3,
                                         'government_id': 96,
                                         'id': 55925,
                                         'name': 'Social Ahemakino Green Party',
                                         'state_id': 16,
                                         'updated_at': 1596452651},
        'Udegobo Silver Power Int': {'active_states': [FactionActiveState(system_id=569, faction_id=68340, state_id=73)],
                                     'allegiance_id': 3,
                                     'government_id': 64,
                                     'id': 68340,
                                     'name': 'Udegobo Silver Power Int',
                                     'state_id': 73,
                                     'updated_at': 1596452651}
    },
        [
        {'faction_id': 26800,
         'happiness_id': 2,
         'influence': 0.102386,
         'is_controlling_faction': False,
         'system_id': 569,
         'updated_at': 1596452651},
        {'faction_id': 55925,
         'happiness_id': 2,
         'influence': 0.643141,
         'is_controlling_faction': True,
         'system_id': 569,
         'updated_at': 1596452651},
        {'faction_id': 68340,
         'happiness_id': 2,
         'influence': 0.078529,
         'is_controlling_faction': False,
         'system_id': 569,
         'updated_at': 1596452651},
        {'faction_id': 55926,
         'happiness_id': 2,
         'influence': 0.014911,
         'is_controlling_faction': False,
         'system_id': 569,
         'updated_at': 1596452651},
        {'faction_id': 58194,
         'happiness_id': 2,
         'influence': 0.078529,
         'is_controlling_faction': False,
         'system_id': 569,
         'updated_at': 1596452651},
        {'faction_id': 55927,
         'happiness_id': 2,
         'influence': 0.037773,
         'is_controlling_faction': False,
         'system_id': 569,
         'updated_at': 1596452651},
        {'faction_id': 55928,
         'happiness_id': 2,
         'influence': 0.044732,
         'is_controlling_faction': False,
         'system_id': 569,
         'updated_at': 1596452651}
    ])
    msg = json.loads(EXAMPLE_JOURNAL_STATION)
    parser = cogdb.eddn.create_parser(msg)

    parser.parse_system()
    parser.parse_station()
    result = parser.parse_factions()

    #  __import__('pprint').pprint(result)
    assert result == expect
    parser.eddb_session.rollback()


def test_edmcjournal_parse_conflicts():
    expect = [{
        'faction1_days': 1,
        'faction1_id': 68340,
        'faction1_stake_id': 59829,
        'faction2_days': 0,
        'faction2_id': 58194,
        'faction2_stake_id': None,
        'status_id': 2,
        'system_id': 569,
        'type_id': 6,
        'updated_at': 1596452651
    }]
    msg = json.loads(EXAMPLE_JOURNAL_STATION)
    parser = cogdb.eddn.create_parser(msg)

    parser.parse_system()
    parser.parse_station()
    parser.parse_factions()
    result = parser.parse_conflicts()

    #  __import__('pprint').pprint(result)
    assert result == expect
    parser.eddb_session.rollback()


def test_log_fname():
    msg = json.loads(EXAMPLE_JOURNAL_STATION)
    expect = "journal_1_2020_08_03T11_04_11Z_E_D_Market_Connector_Windows_"

    assert cogdb.eddn.log_fname(msg) == expect


def test_log_msg():
    try:
        msg = json.loads(EXAMPLE_JOURNAL_STATION)
        t_dir = tempfile.mkdtemp()
        cogdb.eddn.log_msg(msg, path=t_dir, fname='test.txt')

        pat = pathlib.Path(t_dir)
        assert list(pat.glob('test.*'))
    finally:
        shutil.rmtree(t_dir)
