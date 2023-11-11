# pylint: disable=redefined-outer-name,missing-function-docstring,unused-argument
"""
Tests for cogdb.eddn
"""
import datetime

import pytest
try:
    import rapidjson as json
except ImportError:
    import json

import cogdb.eddn
import cogdb.eddb
import cogdb.schema
from cogdb.eddb import ShipSold
from cogdb.schema import TrackByID
from cogdb.spansh import SCommodityPricing, SModuleSold
import cog.util


with open(cog.util.rel_to_abs('tests', 'eddn_data', 'commodity.json'), 'r', encoding='utf-8') as fin:
    EXAMPLE_COMMODITY = json.load(fin)[0]
with open(cog.util.rel_to_abs('tests', 'eddn_data', 'outfit.json'), 'r', encoding='utf-8') as fin:
    EXAMPLE_OUTFIT = json.load(fin)[0]
with open(cog.util.rel_to_abs('tests', 'eddn_data', 'shipyard.json'), 'r', encoding='utf-8') as fin:
    EXAMPLE_SHIPYARD = json.load(fin)[0]
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
        "shop",
        "techBroker"
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


@pytest.fixture
def mapped(eddb_session):
    yield cogdb.spansh.eddb_maps(eddb_session)


def test_create_id_maps(eddb_session):
    maps = cogdb.eddn.create_id_maps(eddb_session)
    assert 'Thargoid' in maps['Allegiance']


def test_station_key():
    station = {
        'name': 'test',
        'type_id': 24,
    }
    assert 'test' == cogdb.eddn.station_key(system='Rana', station=station)

    station = {
        'name': 'test',
        'type_id': 7,
    }
    assert 'Rana||test' == cogdb.eddn.station_key(system='Rana', station=station)


def test_timestamp_is_recent():
    now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=10)
    msg = {
        "header": {
            "gatewayTimestamp": now.strftime(cog.util.TIME_STRP),
        }
    }
    assert cogdb.eddn.timestamp_is_recent(msg, window=1)

    msg = {
        "header": {
            "gatewayTimestamp": now.strftime(cog.util.TIME_STRP_MICRO),
        }
    }
    assert cogdb.eddn.timestamp_is_recent(msg, window=1)

    now = now - datetime.timedelta(minutes=5)
    msg = {
        "header": {
            "gatewayTimestamp": now.strftime(cog.util.TIME_STRP),
        }
    }
    assert not cogdb.eddn.timestamp_is_recent(msg, window=1)


class MsgParserImpl(cogdb.eddn.MsgParser):
    """
    Used to test shared code in MsgParser base.
    """
    def parse_msg(self):
        pass

    def update_database(self):
        pass


class TestMsgParserImpl:
    """
    Test all base MsgParser methods.
    """
    def test_header(self, session, eddb_session):
        msg = json.loads(EXAMPLE_JOURNAL_STATION)
        parser = MsgParserImpl(session, eddb_session, msg)

        assert parser.header["softwareName"] == "E:D Market Connector [Windows]"

    def test_body(self, session, eddb_session):
        msg = json.loads(EXAMPLE_JOURNAL_STATION)
        parser = MsgParserImpl(session, eddb_session, msg)

        assert parser.body["BodyID"] == 65

    def test_date_obj(self, session, eddb_session):
        msg = json.loads(EXAMPLE_JOURNAL_STATION)
        parser = MsgParserImpl(session, eddb_session, msg)

        assert parser.date_obj == datetime.datetime(2020, 8, 3, 11, 4, 11, tzinfo=datetime.timezone.utc)

    def test_timestamp(self, session, eddb_session):
        msg = json.loads(EXAMPLE_JOURNAL_STATION)
        parser = MsgParserImpl(session, eddb_session, msg)

        assert parser.timestamp == 1596452651

    def test_select_station(self, session, eddb_session):
        parser = MsgParserImpl(session, eddb_session, EXAMPLE_COMMODITY)
        assert parser.select_station().name == "Mozhaysky Gateway"


class TestEDMCJournal:
    """
    Test all related methods of the EDMCJournal parser
    """
    def test_parse_msg_journal(self):
        msg = json.loads(EXAMPLE_JOURNAL_STATION)
        parser = cogdb.eddn.create_parser(msg)
        result = parser.parse_msg()

        assert result['system']
        assert result['station']
        assert result['factions']
        assert result['influences']
        assert result['conflicts']

    def test_parse_msg_carrier(self):
        msg = json.loads(EXAMPLE_CARRIER_EDMC)
        parser = cogdb.eddn.create_parser(msg)
        result = parser.parse_msg()
        parser.parse_system()
        parser.parse_and_flush_carrier()

        assert result['system']
        assert result['carriers']
        assert result['station']
        assert not result.get('factions')
        assert not result.get('influences')
        assert not result.get('conflicts')

    def test_update_database(self, mapped):
        msg = json.loads(EXAMPLE_JOURNAL_STATION)
        parser = cogdb.eddn.create_parser(msg)
        parser.parse_msg()
        parser.update_database()
        result = parser.parsed

        # Since updating EDDB, these already exist in db so just test valid IDs were set.
        expect_system_id = mapped['systems'][result['system']['name']]
        assert result['system']['id'] == expect_system_id
        station_key = cogdb.spansh.station_key(system=result['system']['name'], station=result['station'])
        # TODO: Update how station ids verified
        #  assert result['station']['id'] == mapped['stations'][station_key]
        faction_name = 'Ahemakino Bridge Organisation'
        assert result['factions'][faction_name]['id'] == mapped['factions'][faction_name]
        assert result['influences'][0]['faction_id'] == mapped['factions']['Ochosag Federal Company']
        assert result['influences'][0]['system_id'] == expect_system_id
        assert result['conflicts'][0]['faction1_id'] == mapped['factions']['Udegobo Silver Power Int']

    def test_parse_system(self, mapped):
        expected = {
            'controlling_minor_faction_id': mapped['factions']['Social Ahemakino Green Party'],
            'id': mapped['systems']['Ahemakino'],
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

        assert result == expected

    def test_flush_system_to_db(self):
        msg = json.loads(EXAMPLE_JOURNAL_STATION)
        parser = cogdb.eddn.create_parser(msg)
        parser.parse_system()

        # TODO: Atm this implicit in parse_system, potentially separate.
        parser.flush_system_to_db()
        assert parser.flushed

    def test_parse_and_flush_carrier_edmc_id(self, session, f_track_testbed):
        msg = json.loads(EXAMPLE_CARRIER_EDMC)
        parser = cogdb.eddn.create_parser(msg)
        parser.parsed['system'] = {
            "name": "Rana",
            "updated_at": "2021-05-20T19:03:20.11111Z",
        }

        result = parser.parse_and_flush_carrier()

        cid = 'OVE-111'
        expected = {
            cid: {
                'id': cid,
                'system': 'Rana',
                'updated_at': parser.date_obj.replace(tzinfo=None),
            }
        }
        assert result == expected

        session.commit()
        tracked = session.query(TrackByID).filter(TrackByID.id == cid).one()
        assert tracked.system == "Rana"

        parser.session.rollback()
        parser.eddb_session.rollback()

    def test_parse_and_flush_carrier_disc_system(self, session, f_track_testbed):
        msg = json.loads(EXAMPLE_CARRIER_DISC)
        parser = cogdb.eddn.create_parser(msg)
        parser.parsed['system'] = {
            "name": "Nanomam",
            "updated_at": "2021-05-20 19:03:20",
        }

        result = parser.parse_and_flush_carrier()

        cid = 'KLG-9TL'
        expected = {
            cid: {
                'id': cid,
                'override': False,
                'system': 'Nanomam',
                'updated_at': parser.date_obj.replace(tzinfo=None),
            }
        }
        assert result == expected

        session.commit()
        tracked = session.query(TrackByID).filter(TrackByID.id == cid).one()
        assert tracked.system == "Nanomam"

        parser.session.rollback()
        parser.eddb_session.rollback()

    def test_parse_station(self, mapped):
        expected = {
            'controlling_minor_faction_id': mapped['factions']['Social Ahemakino Green Party'],
            'distance_to_star': 0,
            'economies': [
                {'economy_id': 4, 'primary': True, 'proportion': 0.8},
                {'economy_id': 6, 'primary': False, 'proportion': 0.2}
            ],
            'features': {
                'apexinterstellar': False,
                'blackmarket': True,
                'carriermanagement': False,
                'carriervendor': False,
                'commodities': True,
                'dock': True,
                'market': False,
                'materialtrader': True,
                'outfitting': True,
                'rearm': True,
                'refuel': True,
                'repair': True,
                'shipyard': True,
                'techBroker': True,
                'universal_cartographics': False,
                'updated_at': 1596452651
            },
            'is_planetary': False,
            'max_landing_pad_size': 'L',
            'name': 'Mattingly Port',
            'system_id': mapped['systems']["Ahemakino"],
            'type_id': 3,
            'updated_at': 1596452651
        }
        msg = json.loads(EXAMPLE_JOURNAL_STATION)
        parser = cogdb.eddn.create_parser(msg)

        parser.parse_system()
        result = parser.parse_station()

        assert result == expected

    def test_flush_station_to_db(self):
        msg = json.loads(EXAMPLE_JOURNAL_STATION)
        parser = cogdb.eddn.create_parser(msg)

        parser.parse_system()
        result = parser.parse_station()
        assert result
        assert len(parser.flushed) == 1

        parser.flush_station_to_db()
        assert parser.flushed[1].name == "Mattingly Port"

    def test_parse_factions(self, mapped):
        expect = {
            'Ahemakino Bridge Organisation': {
                'allegiance_id': 4,
                'government_id': 64,
                'id': mapped['factions']['Ahemakino Bridge Organisation'],
                'name': 'Ahemakino Bridge Organisation',
                'state_id': 80,
                'updated_at': 1596452651
            },
            'Defence Party of Ahemakino': {
                'allegiance_id': 4,
                'government_id': 112,
                'id': mapped['factions']['Defence Party of Ahemakino'],
                'name': 'Defence Party of Ahemakino',
                'state_id': 80,
                'updated_at': 1596452651
            },
            'Natural Ahemakino Defence Party': {
                'allegiance_id': 4,
                'government_id': 112,
                'id': mapped['factions']['Natural Ahemakino Defence Party'],
                'name': 'Natural Ahemakino Defence Party',
                'state_id': 80,
                'updated_at': 1596452651
            },
            'Ochosag Federal Company': {
                'allegiance_id': 3,
                'government_id': 64,
                'id': mapped['factions']['Ochosag Federal Company'],
                'name': 'Ochosag Federal Company',
                'state_id': 80,
                'updated_at': 1596452651
            },
            'Revolutionary Mpalans Confederation': {
                'allegiance_id': 3,
                'government_id': 48,
                'id': mapped['factions']['Revolutionary Mpalans Confederation'],
                'name': 'Revolutionary Mpalans '
                        'Confederation',
                'state_id': 73,
                'updated_at': 1596452651
            },
            'Social Ahemakino Green Party': {
                'allegiance_id': 3,
                'government_id': 96,
                'id': mapped['factions']['Social Ahemakino Green Party'],
                'name': 'Social Ahemakino Green Party',
                'state_id': 16,
                'updated_at': 1596452651
            },
            'Udegobo Silver Power Int': {
                'allegiance_id': 3,
                'government_id': 64,
                'id': mapped['factions']['Udegobo Silver Power Int'],
                'name': 'Udegobo Silver Power Int',
                'state_id': 73,
                'updated_at': 1596452651
            }
        }
        msg = json.loads(EXAMPLE_JOURNAL_STATION)
        parser = cogdb.eddn.create_parser(msg)

        parser.parse_system()
        parser.parse_station()
        result = parser.parse_factions()

        assert result == expect

    def test_parse_influence(self, mapped):
        system_id = mapped['systems']['Ahemakino']
        expect = [
            {
                'faction_id': mapped['factions']['Ochosag Federal Company'],
                'happiness_id': 2,
                'influence': 0.102386,
                'is_controlling_faction': False,
                'system_id': system_id,
                'updated_at': 1596452651
            },
            {
                'faction_id': mapped['factions']['Social Ahemakino Green Party'],
                'happiness_id': 2,
                'influence': 0.643141,
                'is_controlling_faction': True,
                'system_id': system_id,
                'updated_at': 1596452651
            },
            {
                'faction_id': mapped['factions']['Udegobo Silver Power Int'],
                'happiness_id': 2,
                'influence': 0.078529,
                'is_controlling_faction': False,
                'system_id': system_id,
                'updated_at': 1596452651
            },
            {
                'faction_id': mapped['factions']['Defence Party of Ahemakino'],
                'happiness_id': 2,
                'influence': 0.014911,
                'is_controlling_faction': False,
                'system_id': system_id,
                'updated_at': 1596452651
            },
            {
                'faction_id': mapped['factions']['Revolutionary Mpalans Confederation'],
                'happiness_id': 2,
                'influence': 0.078529,
                'is_controlling_faction': False,
                'system_id': system_id,
                'updated_at': 1596452651
            },
            {
                'faction_id': mapped['factions']['Ahemakino Bridge Organisation'],
                'happiness_id': 2,
                'influence': 0.037773,
                'is_controlling_faction': False,
                'system_id': system_id,
                'updated_at': 1596452651
            },
            {
                'faction_id': mapped['factions']['Natural Ahemakino Defence Party'],
                'happiness_id': 2,
                'influence': 0.044732,
                'is_controlling_faction': False,
                'system_id': system_id,
                'updated_at': 1596452651
            }
        ]
        msg = json.loads(EXAMPLE_JOURNAL_STATION)
        parser = cogdb.eddn.create_parser(msg)

        parser.parse_factions()
        parser.parse_system()
        parser.parse_station()
        result = parser.parse_influence()

        assert result == expect

    def test_flush_factions_to_db(self):
        msg = json.loads(EXAMPLE_JOURNAL_STATION)
        parser = cogdb.eddn.create_parser(msg)

        parser.parse_system()
        parser.parse_station()
        result = parser.parse_factions()
        assert result

        parser.flush_factions_to_db()
        assert parser.flushed[1].name == "Ochosag Federal Company"

    def test_flush_influences_to_db(self, mapped):
        msg = json.loads(EXAMPLE_JOURNAL_STATION)
        parser = cogdb.eddn.create_parser(msg)

        parser.parse_system()
        parser.parse_station()
        parser.parse_factions()
        result = parser.parse_influence()
        assert result

        parser.flush_influences_to_db()
        assert parser.flushed[2].faction_id == mapped['factions']['Social Ahemakino Green Party']
        assert parser.flushed[2].is_controlling_faction
        assert parser.flushed[2].happiness_id == 2

    def test_parse_conflicts(self, mapped):
        expect = [{
            'faction1_days': 1,
            'faction1_id': mapped['factions']['Udegobo Silver Power Int'],
            'faction1_stake_id': mapped['stations']['Ahemakino||Haarsma Keep'],
            'faction2_days': 0,
            'faction2_id': mapped['factions']['Revolutionary Mpalans Confederation'],
            'faction2_stake_id': None,
            'status_id': 2,
            'system_id': mapped['systems']['Ahemakino'],
            'type_id': 6,
            'updated_at': 1596452651
        }]
        msg = json.loads(EXAMPLE_JOURNAL_STATION)
        parser = cogdb.eddn.create_parser(msg)

        parser.parse_system()
        parser.parse_station()
        parser.parse_factions()
        result = parser.parse_conflicts()

        assert result == expect

    def test_flush_conflicts_to_db(self, mapped):
        msg = json.loads(EXAMPLE_JOURNAL_STATION)
        parser = cogdb.eddn.create_parser(msg)

        parser.parse_system()
        parser.parse_station()
        parser.parse_factions()
        result = parser.parse_conflicts()
        assert result

        parser.flush_conflicts_to_db()
        assert parser.flushed[1].faction1_id == mapped['factions']['Udegobo Silver Power Int']
        assert parser.flushed[1].faction2_id == mapped['factions']['Revolutionary Mpalans Confederation']


class TestCommodityV3:
    """
    Test all related methods of the comodity parser
    """
    def test_parse_msg(self, session, eddb_session):
        parser = cogdb.eddn.CommodityV3(session, eddb_session, EXAMPLE_COMMODITY)
        parser.parse_msg()
        assert parser.parsed['commodity_pricing'][0]['station_id'] == 16986

    def test_update_database(self, session, eddb_session):
        parser = cogdb.eddn.CommodityV3(session, eddb_session, EXAMPLE_COMMODITY)
        parser.parse_msg()
        station_id = parser.parsed['commodity_pricing'][0]['station_id']
        comm_id = parser.parsed['commodity_pricing'][0]['commodity_id']

        existing = []
        try:
            existing = eddb_session.query(SCommodityPricing).filter(SCommodityPricing.station_id == station_id).all()
            eddb_session.query(SCommodityPricing).filter(SCommodityPricing.station_id == station_id).delete()
            eddb_session.commit()

            parser.update_database()
            found = eddb_session.query(SCommodityPricing).\
                filter(SCommodityPricing.station_id == station_id,
                        SCommodityPricing.commodity_id == comm_id).\
                one()
            assert found
        finally:
            eddb_session.rollback()
            if existing:
                eddb_session.query(SCommodityPricing).filter(SCommodityPricing.station_id == station_id).delete()
                for item in existing:
                    eddb_session.add(SCommodityPricing(
                        id=item.id,
                        station_id=item.station_id,
                        commodity_id=item.commodity_id,
                        demand=item.demand,
                        supply=item.supply,
                        buy_price=item.buy_price,
                        sell_price=item.sell_price,
                    ))
                eddb_session.commit()


class TestOutfittingV2:
    """
    Test all related methods of the comodity parser
    """
    def test_parse_msg(self, session, eddb_session):
        parser = cogdb.eddn.OutfittingV2(session, eddb_session, EXAMPLE_OUTFIT)
        parser.parse_msg()
        assert parser.parsed['modules_sold'][0]['station_id'] == 16986

    def test_update_database(self, session, eddb_session):
        parser = cogdb.eddn.OutfittingV2(session, eddb_session, EXAMPLE_OUTFIT)
        parser.parse_msg()
        station_id = parser.parsed['modules_sold'][0]['station_id']
        mod_id = parser.parsed['modules_sold'][0]['module_id']

        existing = []
        try:
            existing = eddb_session.query(SModuleSold).filter(SModuleSold.station_id == station_id).all()
            eddb_session.query(SModuleSold).filter(SModuleSold.station_id == station_id).delete()
            eddb_session.commit()

            parser.update_database()
            found = eddb_session.query(SModuleSold).\
                filter(SModuleSold.station_id == station_id,
                        SModuleSold.module_id == mod_id).\
                one()
            assert found
        finally:
            eddb_session.rollback()
            if existing:
                eddb_session.query(SModuleSold).filter(SModuleSold.station_id == station_id).delete()
                for item in existing:
                    eddb_session.add(SModuleSold(
                        id=item.id,
                        station_id=item.station_id,
                        module_id=item.module_id,
                    ))
                eddb_session.commit()


class TestShipyardV2:
    """
    Test all related methods of the comodity parser
    """
    def test_parse_msg(self, session, eddb_session):
        parser = cogdb.eddn.ShipyardV2(session, eddb_session, EXAMPLE_SHIPYARD)
        parser.parse_msg()
        assert parser.parsed['ships_sold'][0]['station_id'] == 788

    def test_update_database(self, session, eddb_session):
        parser = cogdb.eddn.ShipyardV2(session, eddb_session, EXAMPLE_SHIPYARD)
        parser.parse_msg()
        station_id = parser.parsed['ships_sold'][0]['station_id']
        ship_id = parser.parsed['ships_sold'][0]['ship_id']

        existing = []
        try:
            existing = eddb_session.query(ShipSold).filter(ShipSold.station_id == station_id).all()
            eddb_session.query(ShipSold).filter(ShipSold.station_id == station_id).delete()
            eddb_session.commit()

            parser.update_database()
            found = eddb_session.query(ShipSold).\
                filter(ShipSold.station_id == station_id,
                        ShipSold.ship_id == ship_id).\
                one()
            assert found
        finally:
            eddb_session.rollback()
            if existing:
                eddb_session.query(ShipSold).filter(ShipSold.station_id == station_id).delete()
                for item in existing:
                    eddb_session.add(ShipSold(
                        id=item.id,
                        station_id=item.station_id,
                        ship_id=item.ship_id,
                    ))
                eddb_session.commit()
