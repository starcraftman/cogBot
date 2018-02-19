"""
All schema logic related to eddb.

Note there may be duplication between here and side.py.
The latter is purely a mapping of sidewinder's remote.
This module is for internal use.
"""
from __future__ import absolute_import, print_function
import inspect
import math
import sys
try:
    import simplejson as json
except ImportError:
    import json

import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm
import sqlalchemy.ext.declarative
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method

# import cog.exc
import cog.tbl
import cog.util
import cogdb

PRELOAD = True
LEN_COM = 30
LEN_FACTION = 64
LEN_STATION = 76
LEN_SYSTEM = 30
TIME_FMT = "%d/%m/%y %H:%M:%S"
POWER_IDS = {
    None: None,
    "Aisling Duval": 1,
    "Archon Delaine": 2,
    "Arissa Lavigny-Duval": 3,
    "Denton Patreus": 4,
    "Edmund Mahon": 5,
    "Felicia Winters": 6,
    "Li Yong-Rui": 7,
    "Pranav Antal": 8,
    "Zachary Hudson": 9,
    "Zemina Torval": 10,
    "Yuri Grom": 11,
}
Base = sqlalchemy.ext.declarative.declarative_base()


class Allegiance(Base):
    """ Represents the allegiance of a faction. """
    __tablename__ = "allegiance"

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(18))

    def __repr__(self):
        keys = ['id', 'text']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Allegiance) and isinstance(other, Allegiance) and
                self.id == other.id)


class Commodity(Base):
    """ A commodity sold at a station. """
    __tablename__ = 'commodities'

    id = sqla.Column(sqla.Integer, primary_key=True)
    category_id = sqla.Column(sqla.Integer,
                              sqla.ForeignKey("commodity_categories.id"), nullable=False)
    name = sqla.Column(sqla.String(LEN_COM))
    average_price = sqla.Column(sqla.Integer, default=0)
    is_rare = sqla.Column(sqla.Boolean, default=False)

    def __repr__(self):
        keys = ['id', 'category_id', "name", "average_price", "is_rare"]
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "Commodity({})".format(', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Commodity) and isinstance(other, Commodity) and
                self.id == other.id)


class CommodityCat(Base):
    """ The category for a commodity """
    __tablename__ = "commodity_categories"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(20))

    def __repr__(self):
        keys = ['id', 'name']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "CommodityCat({})".format(', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, CommodityCat) and isinstance(other, CommodityCat) and
                self.id == other.id)


class Faction(Base):
    """ Information about a faction. """
    __tablename__ = "factions"

    id = sqla.Column(sqla.Integer, primary_key=True)
    updated_at = sqla.Column(sqla.Integer)
    name = sqla.Column(sqla.String(LEN_FACTION))
    home_system = sqla.Column(sqla.Integer)
    is_player_faction = sqla.Column(sqla.Integer)
    state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_state.id'))
    government_id = sqla.Column(sqla.Integer, sqla.ForeignKey('gov_type.id'))
    allegiance_id = sqla.Column(sqla.Integer, sqla.ForeignKey('allegiance.id'))

    @hybrid_property
    def home_system_id(self):
        return self.home_system

    @home_system_id.expression
    def home_system_id(self):
        return self.home_system

    def __repr__(self):
        keys = ['id', 'name', 'state_id', 'government_id', 'allegiance_id', 'home_system_id',
                'is_player_faction', 'updated_at']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(self, Faction) and isinstance(other, Faction) and self.id == other.id


class FactionState(Base):
    """ The state a faction is in. """
    __tablename__ = "faction_state"

    id = sqla.Column(sqla.Integer, primary_key=True, nullable=True, autoincrement=False)
    text = sqla.Column(sqla.String(12), nullable=False)
    eddn = sqla.Column(sqla.String(12), default=None)

    def __repr__(self):
        keys = ['id', 'text', 'eddn']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, FactionState) and isinstance(other, FactionState) and
                self.id == other.id)


class Government(Base):
    """ All faction government types. """
    __tablename__ = "gov_type"

    id = sqla.Column(sqla.Integer, primary_key=True, nullable=True, autoincrement=False)
    text = sqla.Column(sqla.String(13), nullable=False)
    eddn = sqla.Column(sqla.String(20), default=None)

    def __repr__(self):
        keys = ['id', 'text', 'eddn']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Government) and isinstance(other, Government) and
                self.id == other.id)


class Module(Base):
    """ A module for a ship. """
    __tablename__ = "modules"

    id = sqla.Column(sqla.Integer, primary_key=True)
    group_id = sqla.Column(sqla.Integer, sqla.ForeignKey('module_groups.id'))
    size = sqla.Column(sqla.Integer)  # Equal to in game size, 1-8.
    rating = sqla.Column(sqla.String(1))  # Rating is A-E
    price = sqla.Column(sqla.Integer, default=0)
    mass = sqla.Column(sqla.Integer, default=0)
    name = sqla.Column(sqla.String(LEN_COM))  # Pacifier
    ship = sqla.Column(sqla.String(20))  # Module sepfically for this ship
    weapon_mode = sqla.Column(sqla.String(6))  # Fixed, Gimbal or Turret

    def __repr__(self):
        keys = ['id', 'name', 'group_id', 'size', 'rating', 'mass', 'price', 'ship', 'weapon_mode']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "Module({})".format(', '.join(kwargs))

    def __eq__(self, other):
        return self.id == other.id


class ModuleGroup(Base):
    """ A group for a module. """
    __tablename__ = "module_groups"

    id = sqla.Column(sqla.Integer, primary_key=True)
    category = sqla.Column(sqla.String(20))
    name = sqla.Column(sqla.String(31))  # Name of module group, like "Beam Laser"
    category_id = sqla.Column(sqla.Integer)

    def __repr__(self):
        keys = ['id', 'name', 'category', 'category_id']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "ModuleGroup({})".format(', '.join(kwargs))

    def __eq__(self, other):
        return self.id == other.id


class Power(Base):
    """ Represents a powerplay leader. """
    __tablename__ = "powers"

    id = sqla.Column(sqla.Integer, primary_key=True, nullable=True, autoincrement=False)
    text = sqla.Column(sqla.String(21))
    abbrev = sqla.Column(sqla.String(5))

    def __repr__(self):
        keys = ['id', 'text', 'abbrev']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Power) and isinstance(other, Power) and
                self.id == other.id)


class PowerState(Base):
    """
    Represents the power state of a system (i.e. control, exploited).

    |  0 | None      |
    | 16 | Control   |
    | 32 | Exploited |
    | 48 | Contested |
    """
    __tablename__ = "power_state"

    id = sqla.Column(sqla.Integer, primary_key=True, nullable=True, autoincrement=False)
    text = sqla.Column(sqla.String(10))

    def __repr__(self):
        keys = ['id', 'text']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, PowerState) and isinstance(other, PowerState) and
                self.id == other.id)


class Security(Base):
    """ Security states of a system. """
    __tablename__ = "security"

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(8))
    eddn = sqla.Column(sqla.String(20))

    def __repr__(self):
        keys = ['id', 'text', 'eddn']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Security) and isinstance(other, Security) and
                self.id == other.id)


class SettlementSecurity(Base):
    """ The security of a settlement. """
    __tablename__ = "settlement_security"

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(10))

    def __repr__(self):
        keys = ['id', 'text']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, SettlementSecurity) and isinstance(other, SettlementSecurity) and
                self.id == other.id)


class SettlementSize(Base):
    """ The size of a settlement. """
    __tablename__ = "settlement_size"

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(3))

    def __repr__(self):
        keys = ['id', 'text']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, SettlementSize) and isinstance(other, SettlementSize) and
                self.id == other.id)


class StationFeatures(Base):
    """ The features at a station. """
    __tablename__ = "station_features"

    id = sqla.Column(sqla.Integer, primary_key=True)  # Station.id
    blackmarket = sqla.Column(sqla.Boolean)
    market = sqla.Column(sqla.Boolean)
    refuel = sqla.Column(sqla.Boolean)
    repair = sqla.Column(sqla.Boolean)
    rearm = sqla.Column(sqla.Boolean)
    outfitting = sqla.Column(sqla.Boolean)
    shipyard = sqla.Column(sqla.Boolean)
    docking = sqla.Column(sqla.Boolean)
    commodities = sqla.Column(sqla.Boolean)

    def __repr__(self):
        keys = ['id', 'blackmarket', 'market', 'refuel',
                'repair', 'rearm', 'outfitting', 'shipyard',
                'docking', 'commodities']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, StationFeatures) and isinstance(other, StationFeatures) and
                self.id == other.id)


class StationType(Base):
    __tablename__ = "station_types"

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(24))

    def __repr__(self):
        keys = ['id', 'name']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, StationType) and isinstance(other, StationType) and
                self.id == other.id)


class Station(Base):
    """ Repesents a system in the universe. """
    __tablename__ = "stations"

    id = sqla.Column(sqla.Integer, sqla.ForeignKey('station_features.id'), primary_key=True)
    updated_at = sqla.Column(sqla.Integer, default=0)
    name = sqla.Column(sqla.String(LEN_STATION))
    distance_to_star = sqla.Column(sqla.Integer)
    max_landing_pad_size = sqla.Column(sqla.String(4))
    type_id = sqla.Column(sqla.Integer, sqla.ForeignKey('station_types.id'))
    system_id = sqla.Column(sqla.Integer)
    controlling_minor_faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'))

    def __repr__(self):
        keys = ['id', 'name', 'distance_to_star', 'max_landing_pad_size',
                'system_id', 'controlling_minor_faction_id', 'updated_at']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(self, Station) and isinstance(other, Station) and self.id == other.id


class System(Base):
    """ Repesents a system in the universe. """
    __tablename__ = "systems"

    id = sqla.Column(sqla.Integer, primary_key=True)
    updated_at = sqla.Column(sqla.Integer)
    name = sqla.Column(sqla.String(LEN_SYSTEM))
    population = sqla.Column(sqla.BigInteger)
    needs_permit = sqla.Column(sqla.Integer)
    edsm_id = sqla.Column(sqla.Integer)
    power_id = sqla.Column(sqla.Integer, sqla.ForeignKey('powers.id'))
    security_id = sqla.Column(sqla.Integer, sqla.ForeignKey('security.id'))
    power_state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('power_state.id'))
    controlling_minor_faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), nullable=True)
    control_system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), nullable=True)
    x = sqla.Column(sqla.Numeric(10, 5, None, False))
    y = sqla.Column(sqla.Numeric(10, 5, None, False))
    z = sqla.Column(sqla.Numeric(10, 5, None, False))

    @hybrid_property
    def controlling_faction_id(self):
        return self.controlling_minor_faction_id

    @controlling_faction_id.expression
    def controlling_faction_id(self):
        return self.controlling_minor_faction_id

    @hybrid_method
    def dist_to(self, other):
        """
        Compute the distance from this system to other.
        """
        dist = 0
        for let in ['x', 'y', 'z']:
            temp = getattr(other, let) - getattr(self, let)
            dist += temp * temp

        return math.sqrt(dist)

    @dist_to.expression
    def dist_to(self, other):
        """
        Compute the distance from this system to other.
        """
        return sqla.func.sqrt((other.x - self.x) * (other.x - self.x) +
                              (other.y - self.y) * (other.y - self.y) +
                              (other.z - self.z) * (other.z - self.z))

    def __repr__(self):
        keys = ['id', 'name', 'population',
                'needs_permit', 'updated_at', 'power_id', 'edsm_id',
                'security_id', 'power_state_id', 'controlling_faction_id',
                'control_system_id', 'x', 'y', 'z']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(self, System) and isinstance(other, System) and self.id == other.id


Commodity.category = sqla_orm.relationship(
    'CommodityCat', uselist=False, back_populates='commodities', lazy='select')
CommodityCat.commodities = sqla_orm.relationship(
    'Commodity', cascade='all, delete, delete-orphan', back_populates='category', lazy='select')
Module.group = sqla_orm.relationship(
    'ModuleGroup', uselist=False, back_populates='modules', lazy='select')
ModuleGroup.modules = sqla_orm.relationship(
    'Module', cascade='all, delete, delete-orphan', back_populates='group', lazy='select')
Station.features = sqla_orm.relationship(
    'StationFeatures', uselist=False, back_populates='station', lazy='select')
StationFeatures.station = sqla_orm.relationship(
    'Station', uselist=False, back_populates='features', lazy='select')


def preload_allegiance(session):
    session.add_all([
        Allegiance(id=1, text="Alliance"),
        Allegiance(id=2, text="Empire"),
        Allegiance(id=3, text="Federation"),
        Allegiance(id=4, text="Independent"),
        Allegiance(id=5, text="None"),
        Allegiance(id=7, text="Pilots Federation"),
    ])


def preload_faction_state(session):
    session.add_all([
        FactionState(id=0, text="(unknown)", eddn=None),
        FactionState(id=16, text="Boom", eddn="Boom"),
        FactionState(id=32, text="Bust", eddn="Bust"),
        FactionState(id=37, text="Famine", eddn="Famine"),
        FactionState(id=48, text="Civil Unrest", eddn="CivilUnrest"),
        FactionState(id=64, text="Civil War", eddn="CivilWar"),
        FactionState(id=65, text="Election", eddn="Election"),
        FactionState(id=67, text="Expansion", eddn="Expansion"),
        FactionState(id=69, text="Lockdown", eddn="Lockdown"),
        FactionState(id=72, text="Outbreak", eddn="Outbreak"),
        FactionState(id=73, text="War", eddn="War"),
        FactionState(id=80, text="None", eddn="None"),
        FactionState(id=96, text="Retreat", eddn="Retreat"),
        FactionState(id=101, text="Investment", eddn="Investment"),
    ])


def preload_gov_type(session):
    session.add_all([
        Government(id=0, text='(unknown)', eddn=None),
        Government(id=16, text='Anarchy', eddn="Anarchy"),
        Government(id=32, text='Communism', eddn="Comunism"),
        Government(id=48, text='Confederacy', eddn="Confederacy"),
        Government(id=64, text='Corporate', eddn='Corporate'),
        Government(id=80, text='Cooperative', eddn='Cooperative'),
        Government(id=96, text='Democracy', eddn='Democracy'),
        Government(id=112, text='Dictatorship', eddn='Dictatorship'),
        Government(id=128, text='Feudal', eddn='Feudal'),
        Government(id=144, text='Patronage', eddn='Patronage'),
        Government(id=150, text='Prison Colony', eddn='PrisonColony'),
        Government(id=160, text='Theocracy', eddn='Theocracy'),
        Government(id=176, text='None', eddn='None'),
        Government(id=192, text='Engineer', eddn='Engineer'),
    ])


def preload_powers(session):
    """ All possible powers in Powerplay. """
    session.add_all([
        Power(id=0, text="None", abbrev="NON"),
        Power(id=1, text="Aisling Duval", abbrev="AIS"),
        Power(id=2, text="Archon Delaine", abbrev="ARC"),
        Power(id=3, text="A. Lavigny-Duval", abbrev="ALD"),
        Power(id=4, text="Denton Patreus", abbrev="PAT"),
        Power(id=5, text="Edmund Mahon", abbrev="MAH"),
        Power(id=6, text="Felicia Winters", abbrev="WIN"),
        Power(id=7, text="Li Yong-Rui", abbrev="LYR"),
        Power(id=8, text="Pranav Antal", abbrev="ANT"),
        Power(id=9, text="Zachary Hudson", abbrev="HUD"),
        Power(id=10, text="Zemina Torval", abbrev="TOR"),
        Power(id=11, text="Yuri Grom", abbrev="GRM"),
    ])


def preload_power_states(session):
    """ All possible powerplay states. """
    session.add_all([
        PowerState(id=0, text="None"),
        PowerState(id=16, text="Control"),
        PowerState(id=32, text="Exploited"),
        PowerState(id=48, text="Contested"),
    ])


def preload_security(session):
    """ Preload possible System security values. """
    session.add_all([
        Security(id=16, text="Low", eddn="Low"),
        Security(id=32, text="Medium", eddn="Medium"),
        Security(id=48, text="High", eddn="High"),
        Security(id=64, text="Anarchy", eddn="state_anarchy"),
        Security(id=80, text="Lawless", eddn="state_lawless"),
    ])


def preload_settlement_security(session):
    """ Preload possible settlement security values. """
    session.add_all([
        SettlementSecurity(id=1, text="Low"),
        SettlementSecurity(id=2, text="Medium"),
        SettlementSecurity(id=3, text="High"),
        SettlementSecurity(id=4, text="None"),
    ])


def preload_settlement_size(session):
    """ Preload possible settlement sizes values. """
    session.add_all([
        SettlementSize(id=16, text=""),
        SettlementSize(id=32, text="+"),
        SettlementSize(id=48, text="++"),
        SettlementSize(id=64, text="+++"),
    ])


def preload_station_types(session):
    """ Preload station types table. """
    session.add_all([
        StationType(id=1, text='Civilian Outpost'),
        StationType(id=2, text='Commercial Outpost'),
        StationType(id=3, text='Coriolis Starport'),
        StationType(id=4, text='Industrial Outpost'),
        StationType(id=5, text='Military Outpost'),
        StationType(id=6, text='Mining Outpost'),
        StationType(id=7, text='Ocellus Starport'),
        StationType(id=8, text='Orbis Starport'),
        StationType(id=9, text='Scientific Outpost'),
        StationType(id=11, text='Unknown Outpost'),
        StationType(id=12, text='Unknown Starport'),
        StationType(id=13, text='Planetary Outpost'),
        StationType(id=14, text='Planetary Port'),
        StationType(id=15, text='Unknown Planetary'),
        StationType(id=16, text='Planetary Settlement'),
        StationType(id=17, text='Planetary Engineer Base'),
        StationType(id=19, text='Megaship'),
        StationType(id=20, text='Asteroid Base'),
    ])


def preload_tables(session):
    """
    Preload all minor linked tables.
    """
    if not PRELOAD:
        return

    preload_allegiance(session)
    preload_faction_state(session)
    preload_gov_type(session)
    preload_powers(session)
    preload_power_states(session)
    preload_security(session)
    preload_settlement_security(session)
    preload_settlement_size(session)
    preload_station_types(session)
    session.commit()


def parse_allegiance(session):
    objs = []

    def parse_actual(data):
        if data["allegiance_id"] and data["allegiance_id"] not in objs and not PRELOAD:
            if data["allegiance"] is None:
                data["allegiance"] = "None"
            session.add(Allegiance(id=data["allegiance_id"], text=data["allegiance"]))
            session.commit()
            objs.append(data["allegiance_id"])
        del data["allegiance"]

    return parse_actual


def parse_commodity_categories(session):
    objs = []

    def parse_actual(data):
        cat = data["category"]
        if cat['id'] and cat["id"] not in objs:
            objs.append(cat["id"])
            session.add(CommodityCat(**cat))
        del data['category']

        return data['category_id']

    return parse_actual


def parse_faction_state(session):
    objs = []

    def parse_actual(data):
        if data["state_id"] and data["state_id"] not in objs and not PRELOAD:
            if data["state"] is None:
                data["state"] = "None"
            session.add(FactionState(id=data["state_id"], text=data["state"]))
            session.commit()
            objs.append(data["state_id"])
        del data["state"]

    return parse_actual


def parse_government(session):
    objs = []

    def parse_actual(data):
        if data["government_id"] and data["government_id"] not in objs and not PRELOAD:
            if data["government"] is None:
                data["government"] = "None"
            session.add(Government(id=data["government_id"], text=data["government"]))
            session.commit()
            objs.append(data["government_id"])
        del data["government"]

    return parse_actual


def parse_module_groups(session):
    objs = []

    def parse_actual(data):
        grp = data['group']
        if grp['id'] and grp["id"] not in objs:
            objs.append(grp["id"])
            session.add(ModuleGroup(**grp))
        gid = grp['id']
        del data['group']

        return gid

    return parse_actual


def parse_security(session):
    objs = []

    def parse_actual(data):
        did = data["security_id"]
        if did and did not in objs and not PRELOAD:
            # if data["security"] is None:
                # data["allegiance"] = "None"
            session.add(Security(id=did, text=data["security"]))
            session.commit()
            objs.append(did)
            del data["security"]
        del data["allegiance"]

    return parse_actual


def parse_power(data):
    data["power_id"] = POWER_IDS[data["power"]]
    del data["power"]

    return data


def parse_station_features(session):
    def parse_actual(data):
        session.add(StationFeatures(id=data["id"],
                                    blackmarket=data['has_blackmarket'],
                                    commodities=data['has_commodities'],
                                    docking=data['has_docking'],
                                    market=data['has_market'],
                                    outfitting=data['has_outfitting'],
                                    refuel=data['has_refuel'],
                                    repair=data['has_repair'],
                                    rearm=data['has_rearm'],
                                    shipyard=data['has_shipyard']))

    return parse_actual


def parse_station_type(session):
    objs = []

    def parse_actual(data):
        if data["type_id"] and data["type_id"] not in objs and not PRELOAD:
            session.add(StationType(id=data["type_id"], text=data["type"]))
            # session.commit()
            objs.append(data["type_id"])
        del data["type"]

    return parse_actual


def load_commodities(session, fname):
    """ Parse standard eddb dump commodities.json and enter into database. """
    with open(fname) as fin:
        all_data = json.load(fin)

    parse_cat = parse_commodity_categories(session)
    for data in all_data:
        commodity = Commodity(id=data["id"], category_id=parse_cat(data), name=data["name"],
                              average_price=data["average_price"], is_rare=data["is_rare"])
        session.add(commodity)
        # print(commodity)


def load_modules(session, fname):
    """ Parse standard eddb dump modules.json and enter into database. """
    with open(fname) as fin:
        all_data = json.load(fin)

    group_parser = parse_module_groups(session)
    for data in all_data:
        gid = group_parser(data)
        module = Module(id=data["id"], group_id=gid, name=data["name"],
                        size=data.get('class'), rating=data['rating'],
                        mass=data.get('mass'), price=data['price'],
                        ship=data['ship'], weapon_mode=data["weapon_mode"])
        session.add(module)
        # print(module)


def load_factions(session, fname):
    """ Parse standard eddb dump factions.json and enter into database. """
    with open(fname) as fin:
        all_data = json.load(fin)

    parsers = [parse_allegiance(session), parse_government(session), parse_faction_state(session)]
    print("Parsing factions, takes a while ...")
    for data in all_data:
        for parse in parsers:
            parse(data)

        data['home_system'] = data.pop('home_system_id')

        faction = Faction(**data)
        session.add(faction)
        # print(faction)  # A lot of spam


def load_systems(session, fname):
    """ Parse standard eddb dump populated_systems.json and enter into database. """
    with open(fname) as fin:
        all_data = json.load(fin)

    print("Parsing systems, takes a while ...")
    for data in all_data:
        # Until I start parsing, delete. Some can be inferred and won't store.
        for key in ["allegiance", "allegiance_id",
                    "controlling_minor_faction",
                    "government", "government_id", "is_populated",
                    "minor_faction_presences",  # Inluence numbers
                    "power_state",
                    "primary_economy", "primary_economy_id",
                    "reserve_type", "reserve_type_id",
                    "security", "simbad_ref",
                    "state", "state_id"]:
            del data[key]

        data = parse_power(data)

        system = System(**data)
        session.add(system)
        # print(system)  # A lot of spam


def load_stations(session, fname):
    """ Parse standard eddb dump stations.json and enter into database. """
    with open(fname) as fin:
        all_data = json.load(fin)

    print("Parsing stations, takes a while ...")
    parse_type = parse_station_type(session)
    parse_features = parse_station_features(session)
    count = 0
    for data in all_data:
        parse_type(data)
        parse_features(data)

        station = Station(id=data['id'], name=data['name'], type_id=data['type_id'],
                          distance_to_star=data['distance_to_star'],
                          max_landing_pad_size=data['max_landing_pad_size'],
                          controlling_minor_faction_id=data['controlling_minor_faction_id'],
                          system_id=data['system_id'], updated_at=data['updated_at'])

        session.add(station)

        if count:
            print(data)
            print(station)
            print(4 * ' ', station.features)
            print(4 * ' ', station.type)
            count -= 1


def dump_db(session, classes):
    """
    Dump db to a file.
    """
    with open("/tmp/eddb_dump", "w") as fout:
        for cls in classes:
            for obj in session.query(cls):
                fout.write(repr(obj) + ',')


def recreate_tables():
    """
    Recreate all tables in the database, mainly for schema changes and testing.
    """
    Base.metadata.drop_all(cogdb.eddb_engine)
    Base.metadata.create_all(cogdb.eddb_engine)


def get_shipyard_stations(session, centre_name, sys_dist=15, arrival=1000):
    """
    Given a reference centre system, find nearby orbitals within:
        < sys_dist ly from original system
        < arrival ls from the entry start

    Returns:
        List of matches:
            [system_name, system_dist, station_name, station_arrival_distance]
    """
    centre = session.query(System).filter(System.name == centre_name).subquery()
    centre = sqla_orm.aliased(System, centre)
    exclude = session.query(StationType.text).filter(StationType.text.like("%Planet%")).subquery()

    stations = session.query(Station.name, Station.distance_to_star,
                             System.name, System.dist_to(centre)).\
        filter(System.dist_to(centre) < sys_dist,
               Station.system_id == System.id,
               Station.distance_to_star < arrival,
               Station.max_landing_pad_size == 'L',
               StationType.text.notin_(exclude),
               StationFeatures.shipyard).\
        join(StationType, StationFeatures).\
        order_by(System.dist_to(centre), Station.distance_to_star).\
        all()

    # Slight cleanup for presentation in table
    return [[c, round(d, 2), a, b] for a, b, c, d in stations]


def select_classes(obj):
    """ Simple predicate, select sublasses of Base. """
    return inspect.isclass(obj) and obj.__name__ != "Base"


def main():  # pragma: no cover
    """ Main entry. """
    confirm = input("Reimport EDDB Database? (y/n) ").strip().lower()
    if confirm == "dump":
        print("Dumping to: /tmp/eddb_dump")
        classes = [x[1] for x in inspect.getmembers(sys.modules[__name__], select_classes)]
        dump_db(cogdb.EDDBSession(), classes)
    elif not confirm.startswith('y'):
        print("Aborting.")
        return

    recreate_tables()
    session = cogdb.EDDBSession()
    preload_tables(session)

    load_commodities(session, cog.util.rel_to_abs("data", "eddb", "commodities.json"))
    load_modules(session, cog.util.rel_to_abs("data", "eddb", "modules.json"))
    load_factions(session, cog.util.rel_to_abs("data", "eddb", "factions.json"))
    session.commit()
    load_systems(session, cog.util.rel_to_abs("data", "eddb", "systems_populated.json"))
    load_stations(session, cog.util.rel_to_abs("data", "eddb", "stations.json"))
    session.commit()

    print("Faction count:", session.query(Faction).count())
    print("System count (populated):", session.query(System).count())
    print("Station count:", session.query(Station).count())

    stations = get_shipyard_stations(session, input("Please enter a system name ... "))
    if stations:
        print(cog.tbl.format_table(stations))


if __name__ == "__main__":  # pragma: no cover
    main()
