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
import ijson.backends.yajl2_cffi as ijson

import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm
import sqlalchemy.exc as sqla_exc
import sqlalchemy.ext.declarative
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method

import cog.exc
import cog.tbl
import cog.util
import cogdb

PRELOAD = True
LEN = {  # Lengths for strings stored in the db
    "allegiance": 18,
    "commodity": 34,
    "commodity_category": 20,
    "eddn": 20,
    "faction": 90,
    "faction_happiness": 12,
    "faction_state": 12,
    "government": 13,
    "module": 30,
    "module_category": 20,  # Name of group of similar groups like limpets, weapons
    "module_group": 36,  # Name of module group, i.e. "Beam Laser"
    "power": 21,
    "power_abv": 5,
    "power_state": 10,
    "security": 8,
    "settlement_security": 10,
    "settlement_size": 3,
    "ship": 20,
    "station": 76,
    "station_pad": 4,
    "station_type": 24,
    "system": 30,
    "weapon_mode": 6,
}
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
    text = sqla.Column(sqla.String(LEN["allegiance"]))

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
    name = sqla.Column(sqla.String(LEN["commodity"]))
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
    name = sqla.Column(sqla.String(LEN["commodity_category"]))

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
    name = sqla.Column(sqla.String(LEN["faction"]))
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


class FactionHappiness(Base):
    """ The happiness of a faction. """
    __tablename__ = "faction_happiness"

    id = sqla.Column(sqla.Integer, primary_key=True, nullable=True, autoincrement=False)
    text = sqla.Column(sqla.String(LEN["faction_happiness"]), nullable=False)
    eddn = sqla.Column(sqla.String(LEN["eddn"]), default=None)

    def __repr__(self):
        keys = ['id', 'text', 'eddn']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, FactionHappiness) and isinstance(other, FactionHappiness) and
                self.id == other.id)


class FactionState(Base):
    """ The state a faction is in. """
    __tablename__ = "faction_state"

    id = sqla.Column(sqla.Integer, primary_key=True, nullable=True, autoincrement=False)
    text = sqla.Column(sqla.String(LEN["faction_state"]), nullable=False)
    eddn = sqla.Column(sqla.String(LEN["eddn"]), default=None)

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
    text = sqla.Column(sqla.String(LEN["government"]), nullable=False)
    eddn = sqla.Column(sqla.String(LEN["eddn"]), default=None)

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
    name = sqla.Column(sqla.String(LEN["module"]))  # Pacifier
    ship = sqla.Column(sqla.String(LEN["ship"]))  # Module sepfically for this ship
    weapon_mode = sqla.Column(sqla.String(LEN["weapon_mode"]))  # Fixed, Gimbal or Turret

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
    category = sqla.Column(sqla.String(LEN["module_category"]))
    name = sqla.Column(sqla.String(LEN["module_group"]))  # Name of module group, i.e. "Beam Laser"
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
    text = sqla.Column(sqla.String(LEN["power"]))
    abbrev = sqla.Column(sqla.String(LEN["power_abv"]))

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
    text = sqla.Column(sqla.String(LEN["power_state"]))

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
    text = sqla.Column(sqla.String(LEN["security"]))
    eddn = sqla.Column(sqla.String(LEN["eddn"]))

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
    text = sqla.Column(sqla.String(LEN["settlement_security"]))

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
    text = sqla.Column(sqla.String(LEN["settlement_size"]))

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
    text = sqla.Column(sqla.String(LEN["station_type"]))

    def __repr__(self):
        keys = ['id', 'text']
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
    name = sqla.Column(sqla.String(LEN["station"]))
    distance_to_star = sqla.Column(sqla.Integer)
    max_landing_pad_size = sqla.Column(sqla.String(LEN["station_pad"]))
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
    name = sqla.Column(sqla.String(LEN["system"]))
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


def preload_faction_happiness(session):
    session.add_all([
        FactionHappiness(id=1, text='Elated', eddn='Elated'),
        FactionHappiness(id=2, text='Happy', eddn='Happy'),
        FactionHappiness(id=3, text='Discontented', eddn='Discontented'),
        FactionHappiness(id=4, text='Unhappy', eddn='Unhappy'),
        FactionHappiness(id=5, text='Despondent', eddn='Despondent'),
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
        Government(id=208, text='Prison', eddn='Prison'),
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
        StationType(id=10, text='Unsanctioned Outpost'),
        StationType(id=11, text='Unknown Outpost'),
        StationType(id=12, text='Unknown Starport'),
        StationType(id=13, text='Planetary Outpost'),
        StationType(id=14, text='Planetary Port'),
        StationType(id=15, text='Unknown Planetary'),
        StationType(id=16, text='Planetary Settlement'),
        StationType(id=17, text='Planetary Engineer Base'),
        StationType(id=19, text='Megaship'),
        StationType(id=20, text='Asteroid Base'),
        StationType(id=22, text='Unknown Dockable'),
        StationType(id=23, text='Non-Dockable Orbital'),
    ])


def preload_tables(session):
    """
    Preload all minor linked tables.
    """
    if not PRELOAD:
        return

    preload_allegiance(session)
    preload_faction_happiness(session)
    preload_faction_state(session)
    preload_gov_type(session)
    preload_powers(session)
    preload_power_states(session)
    preload_security(session)
    preload_settlement_security(session)
    preload_settlement_size(session)
    preload_station_types(session)
    session.commit()


# TODO: Test these load functions
def load_commodities(session, fname):
    """ Parse standard eddb dump commodities.json and enter into database. """
    item = {}
    item_cat = {}

    # High level mapppings direct data flow by path in json
    # Mappings should be mutually exclusive
    # Format prefix, [(target_dictionary, key_in_dict), (target_dictionary, key_in_dict), ...]
    mappings = {
        'item.id': [('item', 'id')],
        'item.name': [('item', 'name')],
        'item.average_price': [('item', 'average_price')],
        'item.is_rare': [('item', 'is_rare')],
        'item.category.id': [('item', 'category_id'), ('item_cat', 'id')],
        'item.category.name': [('item_cat', 'name')],
    }

    print("Parsing commodities ...")
    with open(fname, 'rb') as fin:
        for prefix, the_type, value in ijson.parse(fin):
            #  print(prefix, the_type, value)
            if (prefix, the_type, value) == ('item', 'end_map', None):
                # JSON Item terminated
                commodity = Commodity(**item)
                commodity_cat = CommodityCat(**item_cat)

                #  Debug
                #  print('Item', commodity)
                #  print('Item Cat', commodity_cat)

                try:
                    session.add(commodity_cat)
                    session.commit()
                except (sqla_exc.IntegrityError, sqla_orm.exc.FlushError):
                    session.rollback()
                session.add(commodity)
                session.commit()

                item.clear()
                item_cat.clear()
                continue

            try:
                for dic, key in mappings[prefix]:
                    locals()[dic][key] = value
            except KeyError:
                pass


def load_modules(session, fname):
    """ Parse standard eddb dump modules.json and enter into database. """
    item = {'size': None, 'mass': None}
    item_group = {}

    # High level mapppings direct data flow by path in json
    # Mappings should be mutually exclusive
    # Format prefix, [(target_dictionary, key_in_dict), (target_dictionary, key_in_dict), ...]
    mappings = {
        'item.id': [('item', 'id')],
        'item.name': [('item', 'name')],
        'item.rating': [('item', 'rating')],
        'item.price': [('item', 'price')],
        'item.ship': [('item', 'ship')],
        'item.weapon_mode': [('item', 'weapon_mode')],
        'item.class': [('item', 'size')],
        'item.mass': [('item', 'mass')],
        'item.group.id': [('item', 'group_id'), ('item_group', 'id')],
        'item.group.name': [('item_group', 'name')],
        'item.group.category': [('item_group', 'category')],
        'item.group.category_id': [('item_group', 'category_id')],
    }

    print("Parsing modules ...")
    with open(fname, 'rb') as fin:
        for prefix, the_type, value in ijson.parse(fin):
            #  print(prefix, the_type, value)
            if (prefix, the_type, value) == ('item', 'end_map', None):
                # JSON Item terminated
                module = Module(**item)
                module_group = ModuleGroup(**item_group)

                # Debug
                #  print('Item', module)
                #  print('Item mod', module_group)

                try:
                    session.add(module_group)
                    session.commit()
                except (sqla_exc.IntegrityError, sqla_orm.exc.FlushError):
                    session.rollback()
                session.add(module)
                session.commit()

                item.clear()
                item['size'] = None
                item['mass'] = None
                item_group.clear()
                continue

            try:
                for dic, key in mappings[prefix]:
                    locals()[dic][key] = value
            except KeyError:
                pass


def load_factions(session, fname):
    """ Parse standard eddb dump modules.json and enter into database. """
    faction = {}
    allegiance = {}
    government = {}

    # High level mapppings direct data flow by path in json
    # Mappings should be mutually exclusive
    # Format prefix, [(target_dictionary, key_in_dict), (target_dictionary, key_in_dict), ...]
    mappings = {
        'item.id': [('faction', 'id')],
        'item.name': [('faction', 'name')],
        'item.home_system_id': [('faction', 'home_system')],
        'item.is_player_faction': [('faction', 'is_player_faction')],
        'item.updated_at': [('faction', 'updated_at')],
        'item.government_id': [('faction', 'government_id'), ('government', 'id')],
        'item.government': [('government', 'text')],
        'item.allegiance_id': [('faction', 'allegiance_id'), ('allegiance', 'id')],
        'item.allegiance': [('allegiance', 'text')],
    }

    print("Parsing factions, takes a while ...")
    with open(fname, 'rb') as fin:
        for prefix, the_type, value in ijson.parse(fin):
            #  print(prefix, the_type, value)
            if (prefix, the_type, value) == ('item', 'end_map', None):
                # JSON Item terminated
                faction_db = Faction(**faction)
                allegiance_db = Allegiance(**allegiance)
                government_db = Government(**government)

                # Debug
                #  print('Faction', faction_db)
                #  print('Allegiance', allegiance_db)
                #  print('Government', government_db)

                if not PRELOAD:
                    try:
                        session.add(allegiance_db)
                        session.commit()
                    except (sqla_exc.IntegrityError, sqla_orm.exc.FlushError):
                        session.rollback()
                    try:
                        session.add(government_db)
                        session.commit()
                    except (sqla_exc.IntegrityError, sqla_orm.exc.FlushError):
                        session.rollback()
                session.add(faction_db)
                session.commit()

                faction.clear()
                allegiance.clear()
                government.clear()
                continue

            try:
                for dic, key in mappings[prefix]:
                    locals()[dic][key] = value
            except KeyError:
                pass


def load_systems(session, fname):
    """ Parse standard eddb dump populated_systems.json and enter into database. """
    system = {}

    # High level mapppings direct data flow by path in json
    # Mappings should be mutually exclusive
    # Format prefix, [(target_dictionary, key_in_dict), (target_dictionary, key_in_dict), ...]
    mappings = {
        'item.id': [('system', 'id')],
        'item.updated_at': [('system', 'updated_at')],
        'item.name': [('system', 'name')],
        'item.population': [('system', 'population')],
        'item.needs_permit': [('system', 'needs_permit')],
        'item.edsm_id': [('system', 'edsm_id')],
        'item.security_id': [('system', 'security_id')],
        'item.power_state_id': [('system', 'power_state_id')],
        'item.controlling_minor_faction_id': [('system', 'controlling_minor_faction_id')],
        'item.control_system_id': [('system', 'control_system_id')],
        'item.x': [('system', 'x')],
        'item.y': [('system', 'y')],
        'item.z': [('system', 'z')],
    }

    print("Parsing systems, takes a while ...")
    with open(fname, 'rb') as fin:
        for prefix, the_type, value in ijson.parse(fin):
            #  print(prefix, the_type, value)
            if (prefix, the_type, value) == ('item', 'end_map', None):
                # JSON Item terminated
                system_db = System(**system)

                # Debug
                #  print('System', system_db)

                session.add(system_db)
                session.commit()

                system.clear()
                continue

            try:
                for dic, key in mappings[prefix]:
                    locals()[dic][key] = value
            except KeyError:
                pass


def load_stations(session, fname):
    """ Parse standard eddb dump stations.json and enter into database. """
    station = {}
    features = {}
    type = {}

    # High level mapppings direct data flow by path in json
    # Mappings should be mutually exclusive
    # Format prefix, [(target_dictionary, key_in_dict), (target_dictionary, key_in_dict), ...]
    mappings = {
        'item.id': [('station', 'id'), ('features', 'id')],
        'item.name': [('station', 'name')],
        'item.type_id': [('station', 'type_id'), ('type', 'id')],
        'item.distance_to_star': [('station', 'distance_to_star')],
        'item.max_landing_pad_size': [('station', 'max_landing_pad_size')],
        'item.controlling_minor_faction_id': [('station', 'controlling_minor_faction_id')],
        'item.system_id': [('station', 'system_id')],
        'item.updated_at': [('station', 'updated_at')],
        'item.has_blackmarket': [('features', 'blackmarket')],
        'item.has_commodities': [('features', 'commodities')],
        'item.has_docking': [('features', 'docking')],
        'item.has_market': [('features', 'market')],
        'item.has_outfitting': [('features', 'outfitting')],
        'item.has_refuel': [('features', 'refuel')],
        'item.has_repair': [('features', 'repair')],
        'item.has_rearm': [('features', 'rearm')],
        'item.has_shipyard': [('features', 'shipyard')],
        'item.type': [('type', 'text')],
    }

    print("Parsing stations, takes a while ...")
    with open(fname, 'rb') as fin:
        for prefix, the_type, value in ijson.parse(fin):
            #  print(prefix, the_type, value)
            if (prefix, the_type, value) == ('item', 'end_map', None):
                # JSON Item terminated
                station_db = Station(**station)
                features_db = StationFeatures(**features)
                type_db = StationType(**type)

                # Debug
                #  print('Station', station_db)
                #  print('Station Features', features_db)
                #  print('Station Type', type_db)

                try:
                    session.add(type_db)
                    session.commit()
                except (sqla_exc.IntegrityError, sqla_orm.exc.FlushError):
                    session.rollback()
                session.add(features_db)
                session.add(station_db)
                session.commit()

                station.clear()
                features.clear()
                type.clear()
                continue

            try:
                for dic, key in mappings[prefix]:
                    locals()[dic][key] = value
            except KeyError:
                pass


def get_systems(session, system_names):
    """
    Given a list of names, find all exact matching systems.

    Returns:
        [System, System, ...]

    Raises:
        InvalidCommandArgs - One or more systems didn't match.
    """
    systems = session.query(System).\
        filter(System.name.in_(system_names)).\
        all()

    if len(systems) != len(system_names):
        for system in systems:
            system_names = [s_name for s_name in system_names
                            if s_name.lower() != system.name.lower()]
        msg = "Could not find the following system(s):"
        msg += "\n\n" + "\n".join(system_names)
        raise cog.exc.InvalidCommandArgs(msg)

    return systems


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


def nearest_system(centre, systems):
    """
    Given a centre system, choose next nearest in systems.

    Returns:
        [dist_to_centre, System]
    """
    best = [centre.dist_to(systems[0]), systems[0]]
    for system in systems[1:]:
        dist = centre.dist_to(system)
        if dist < best[0]:
            best = [dist, system]

    return best


def find_route(session, start, systems):
    """
    Given a starting system, construct the best route by always selecting the next nearest system.

    Returns:
        [total_distance, [Systems]]
    """
    if not isinstance(start, System):
        start = get_systems(session, [start])[0]
    if not isinstance(systems[0], System):
        systems = get_systems(session, systems)

    course = [start]
    total_dist = 0

    while systems:
        choice = nearest_system(start, systems)
        course += [choice[1]]
        total_dist += choice[0]
        systems.remove(choice[1])
        start = choice[1]

    return [total_dist, course]


def find_best_route(session, systems):
    """
    Find the best route through systems provided by name or System.
    Apply the N nearest algorithm across all possible starting candidates.
    Then select the one with least total distance.

    Returns:
        [total_distance, [System, System, ...]]
    """
    if isinstance(systems[0], System):
        if not isinstance(systems[0], type('')):
            systems = [sys.name for sys in systems]
        systems = get_systems(session, systems)

    best = []
    for start in systems:
        systems_copy = systems[:]
        systems_copy.remove(start)
        result = find_route(session, start, systems_copy)
        if not best or result[0] < best[0]:
            best = result

    return best


def dump_db(session, classes):
    """
    Dump db to a file.
    """
    with open("/tmp/eddb_dump", "w") as fout:
        for cls in classes:
            for obj in session.query(cls):
                fout.write(repr(obj) + '\n')


def select_classes(obj):
    """ Simple predicate, select sublasses of Base. """
    return inspect.isclass(obj) and obj.__name__ not in ["Base", "hybrid_method", "hybrid_property"]


def recreate_tables():
    """
    Recreate all tables in the database, mainly for schema changes and testing.
    """
    Base.metadata.drop_all(cogdb.eddb_engine)
    Base.metadata.create_all(cogdb.eddb_engine)


def import_eddb():
    """ Allows the seeding of db from eddb dumps. """
    try:
        confirm = sys.argv[1].strip().lower()
    except IndexError:
        confirm = input("Reimport EDDB Database? (y/n) ")
    confirm = confirm.strip().lower()

    if confirm == "dump":
        print("Dumping to: /tmp/eddb_dump")
        classes = [x[1] for x in inspect.getmembers(sys.modules[__name__], select_classes)]
        dump_db(cogdb.EDDBSession(), classes)
        return
    elif not confirm.startswith('y'):
        print("Aborting.")
        return

    recreate_tables()
    session = cogdb.EDDBSession()
    preload_tables(session)

    load_commodities(session, cog.util.rel_to_abs("data", "eddb", "commodities.json"))
    load_modules(session, cog.util.rel_to_abs("data", "eddb", "modules.json"))
    load_factions(session, cog.util.rel_to_abs("data", "eddb", "factions.json"))
    load_systems(session, cog.util.rel_to_abs("data", "eddb", "systems_populated.json"))
    load_stations(session, cog.util.rel_to_abs("data", "eddb", "stations.json"))

    print("Faction count:", session.query(Faction).count())
    print("System count (populated):", session.query(System).count())
    print("Station count:", session.query(Station).count())


def main():  # pragma: no cover
    """ Main entry. """
    import_eddb()
    # session = cogdb.EDDBSession()
    # stations = get_shipyard_stations(session, input("Please enter a system name ... "))
    # if stations:
        # print(cog.tbl.format_table(stations))


if __name__ == "__main__":  # pragma: no cover
    main()
