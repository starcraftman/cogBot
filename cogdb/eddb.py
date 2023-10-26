"""
All schema logic related to eddb.

Note there may be duplication between here and side.py.
The latter is purely a mapping of sidewinder's remote.
This module is for internal use.

N.B. Don't put subqueries in FROM of views for now, doesn't work on test docker.
"""
import asyncio
import datetime
import enum
import math
import string
import sys
import time

# Selected backend set in ijson.backend as string.
import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm
import sqlalchemy.orm.session
import sqlalchemy.ext.declarative
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.sql.expression import or_
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method

import cogdb
import cogdb.common
import cog.exc
import cog.tbl
import cog.util
from cog.util import ReprMixin, TimestampMixin, UpdatableMixin


LEN = {  # Lengths for strings stored in the db
    "allegiance": 18,
    "cmdr_name": 25,
    "ship_name": 30,
    "commodity": 34,
    "commodity_category": 20,
    "commodity_group": 30,
    "economy": 18,
    "eddn": 25,
    "faction": 90,
    "faction_happiness": 12,
    "faction_state": 23,
    "government": 18,
    "module": 40,
    "module_category": 20,  # Name of group of similar groups like limpets, weapons
    "module_group": 36,  # Name of module group, i.e. "Beam Laser"
    "module_symbol": 50,  # Information about module
    "power": 21,
    "power_abv": 6,
    "power_state": 18,
    "pvp_name": 50,
    "pvp_fname": 80,
    "pvp_hash": 150,
    "security": 8,
    "settlement_security": 10,
    "settlement_size": 3,
    "ship": 25,
    "station": 45,
    "station_pad": 4,
    "station_type": 24,
    "system": 50,
    "weapon_mode": 6,
}
LEN["spy_location"] = 5 + LEN["system"] + LEN["station"]
TIME_FMT = "%d/%m/%y %H:%M:%S"
# These are the faction types strong/weak verse.
HUDSON_BGS = [['Feudal', 'Patronage'], ["Dictatorship"]]
WINTERS_BGS = [["Corporate"], ["Communism", "Cooperative", "Feudal", "Patronage"]]
# State ids: 16 control, 32 exploited, 48 contested
VIEW_CONTESTEDS = """
CREATE or REPLACE VIEW eddb.v_systems_contested
AS
    SELECT s.system_id as system_id, exp.name as system,
           s.control_id as control_id, con.name as control,
           s.power_id as power_id, p.text as power
    FROM systems_controlled as s
    INNER JOIN systems as exp ON s.system_id = exp.id
    INNER JOIN systems as con ON s.control_id = con.id
    INNER JOIN powers as p ON s.power_id = p.id
    INNER JOIN power_state as ps ON s.power_state_id = ps.id
    WHERE s.power_state_id = 48
    ORDER BY exp.name;
"""
VIEW_SYSTEM_CONTROLS = """
CREATE or REPLACE VIEW eddb.v_systems_controlled
AS
    SELECT s.system_id as system_id, exp.name as system,
           s.power_state_id as power_state_id, ps.text as power_state,
           s.control_id as control_id, con.name as control,
           s.power_id as power_id, p.text as power
    FROM systems_controlled as s
    INNER JOIN systems as exp ON s.system_id = exp.id
    INNER JOIN systems as con ON s.control_id = con.id
    INNER JOIN powers as p ON s.power_id = p.id
    INNER JOIN power_state as ps ON s.power_state_id = ps.id
    ORDER BY con.name, exp.name;
"""
EVENT_CONFLICTS = """
CREATE EVENT IF NOT EXISTS clean_conflicts
ON SCHEDULE
    EVERY 1 DAY
COMMENT "Conflicts expire after 4 days + 1 day grace or 3 days no activity"
DO
    DELETE FROM eddb.conflicts
    WHERE
        (
            (conflicts.faction1_days + conflicts.faction2_days) >= 4 AND
             conflicts.updated_at < (unix_timestamp() - (24 * 60 * 60))
        ) OR (
            conflicts.updated_at < (unix_timestamp() - (3 * 24 * 60 * 60))
        );
"""
EVENT_HISTORY_INFLUENCE = """
CREATE EVENT IF NOT EXISTS clean_history_influence
ON SCHEDULE
    EVERY 1 DAY
COMMENT "Check daily for HistoryInfluence entries older than 30 days."
DO
    DELETE FROM eddb.history_influence
    WHERE updated_at < (unix_timestamp() - (30 * 24 * 60 * 60));
"""
CONTROL_DISTANCE = 15  # A control exploits all systems in this distance
HISTORY_INF_LIMIT = 40
HOUR_SECONDS = 60 * 60
HISTORY_INF_TIME_GAP = HOUR_SECONDS * 4  # min seconds between data points
DEFAULT_DIST = 75
DEFAULT_ARRIVAL = 5000
# To select planetary stations
Base = sqlalchemy.ext.declarative.declarative_base()


class TraderType(enum.Enum):
    """
    Types of traders to filter for get_nearest_station_economies.
    """
    BROKERS_GUARDIAN = 1
    BROKERS_HUMAN = 2
    MATS_DATA = 3
    MATS_RAW = 4
    MATS_MANUFACTURED = 5


class Allegiance(ReprMixin, Base):
    """ Represents the allegiance of a faction. """
    __tablename__ = "allegiance"
    _repr_keys = ['id', 'text', 'eddn']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["allegiance"]))
    eddn = sqla.Column(sqla.String(LEN["allegiance"]))

    def __eq__(self, other):
        return (isinstance(self, Allegiance) and isinstance(other, Allegiance)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class Commodity(ReprMixin, UpdatableMixin, Base):
    """ A commodity sold at a station. """
    __tablename__ = 'commodities'
    _repr_keys = [
        'id', 'category_id', "name", "average_price", "is_rare", "is_non_marketable",
        "max_buy_price", "max_sell_price", "min_buy_price", "min_sell_price", "updated_at"
    ]

    id = sqla.Column(sqla.Integer, primary_key=True)
    category_id = sqla.Column(sqla.Integer,
                              sqla.ForeignKey("commodity_categories.id"), nullable=False)

    name = sqla.Column(sqla.String(LEN["commodity"]))
    average_price = sqla.Column(sqla.Integer, default=0)
    min_buy_price = sqla.Column(sqla.Integer, default=0)
    min_sell_price = sqla.Column(sqla.Integer, default=0)
    max_buy_price = sqla.Column(sqla.Integer, default=0)
    max_sell_price = sqla.Column(sqla.Integer, default=0)
    is_non_marketable = sqla.Column(sqla.Boolean, default=False)
    is_rare = sqla.Column(sqla.Boolean, default=False)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    def __eq__(self, other):
        return (isinstance(self, Commodity) and isinstance(other, Commodity)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class CommodityCat(ReprMixin, Base):
    """ The category for a commodity """
    __tablename__ = "commodity_categories"
    _repr_keys = ['id', 'name']

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN["commodity_category"]))

    def __eq__(self, other):
        return (isinstance(self, CommodityCat) and isinstance(other, CommodityCat)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class Economy(ReprMixin, Base):
    """ The type of economy """
    __tablename__ = "economies"
    _repr_keys = ['id', 'text', 'eddn']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["economy"]))
    eddn = sqla.Column(sqla.String(LEN["economy"]))

    def __eq__(self, other):
        return (isinstance(self, Economy) and isinstance(other, Economy)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class Faction(ReprMixin, TimestampMixin, UpdatableMixin, Base):
    """ Information about a faction. """
    __tablename__ = "factions"
    _repr_keys = [
        'id', 'name', 'state_id', 'government_id', 'allegiance_id', 'home_system_id',
        'is_player_faction', 'updated_at'
    ]

    id = sqla.Column(sqla.Integer, primary_key=True)
    allegiance_id = sqla.Column(sqla.Integer, sqla.ForeignKey('allegiance.id'), default=5)
    government_id = sqla.Column(sqla.Integer, sqla.ForeignKey('gov_type.id'), default=176)
    home_system_id = sqla.Column(sqla.Integer, index=True)  # Makes circular foreigns.
    state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_state.id'), default=80)

    name = sqla.Column(sqla.String(LEN["faction"]), index=True)
    is_player_faction = sqla.Column(sqla.Boolean, default=False)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    allegiance = sqla.orm.relationship('Allegiance', viewonly=True)
    government = sqla.orm.relationship('Government', viewonly=True)
    state = sqla.orm.relationship('FactionState', viewonly=True)

    def __eq__(self, other):
        return isinstance(self, Faction) and isinstance(other, Faction) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class FactionHappiness(ReprMixin, Base):
    """ The happiness of a faction. """
    __tablename__ = "faction_happiness"
    _repr_keys = ['id', 'text', 'eddn']

    id = sqla.Column(sqla.Integer, primary_key=True, nullable=True, autoincrement=False)
    text = sqla.Column(sqla.String(LEN["faction_happiness"]), nullable=False)
    eddn = sqla.Column(sqla.String(LEN["eddn"]), default=None)

    def __eq__(self, other):
        return (isinstance(self, FactionHappiness) and isinstance(other, FactionHappiness)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class FactionState(ReprMixin, Base):
    """ The state a faction is in. """
    __tablename__ = "faction_state"
    _repr_keys = ['id', 'text', 'eddn']

    id = sqla.Column(sqla.Integer, primary_key=True, nullable=True, autoincrement=False)
    text = sqla.Column(sqla.String(LEN["faction_state"]), nullable=False)
    eddn = sqla.Column(sqla.String(LEN["eddn"]), default=None)

    def __eq__(self, other):
        return (isinstance(self, FactionState) and isinstance(other, FactionState)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class FactionActiveState(ReprMixin, TimestampMixin, Base):
    """ Represents the actual or pending states of a faction/system pair."""
    __tablename__ = "faction_active_states"
    _repr_keys = ['system_id', 'faction_id', 'state_id', 'updated_at']

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), primary_key=True)
    state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_state.id'), primary_key=True)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    state = sqla.orm.relationship('FactionState', viewonly=True, lazy='joined')

    def __eq__(self, other):
        return (isinstance(self, FactionActiveState)
                and isinstance(other, FactionActiveState)
                and self.__hash__() == other.__hash__())

    def __hash__(self):
        return hash(f"{self.faction_id}_{self.system_id}_{self.state_id}")


class FactionPendingState(ReprMixin, TimestampMixin, Base):
    """ Represents the actual or pending states of a faction/system pair."""
    __tablename__ = "faction_pending_states"
    _repr_keys = ['system_id', 'faction_id', 'state_id', 'updated_at']

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), primary_key=True)
    state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_state.id'), primary_key=True)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    state = sqla.orm.relationship('FactionState', viewonly=True, lazy='joined')

    def __eq__(self, other):
        return (isinstance(self, FactionPendingState)
                and isinstance(other, FactionPendingState)
                and self.__hash__() == other.__hash__())

    def __hash__(self):
        return hash(f"{self.faction_id}_{self.system_id}_{self.state_id}")


class FactionRecoveringState(ReprMixin, TimestampMixin, Base):
    """ Represents the actual or pending states of a faction/system pair."""
    __tablename__ = "faction_recovering_states"
    _repr_keys = ['system_id', 'faction_id', 'state_id', 'updated_at']

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), primary_key=True)
    state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_state.id'), primary_key=True)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    state = sqla.orm.relationship('FactionState', viewonly=True, lazy='joined')

    def __eq__(self, other):
        return (isinstance(self, FactionRecoveringState)
                and isinstance(other, FactionRecoveringState)
                and self.__hash__() == other.__hash__())

    def __hash__(self):
        return hash(f"{self.faction_id}_{self.system_id}_{self.state_id}")


class Government(ReprMixin, Base):
    """ All faction government types. """
    __tablename__ = "gov_type"
    _repr_keys = ['id', 'text', 'eddn']

    id = sqla.Column(sqla.Integer, primary_key=True, nullable=True, autoincrement=False)
    text = sqla.Column(sqla.String(LEN["government"]), nullable=False)
    eddn = sqla.Column(sqla.String(LEN["eddn"]), default=None)

    def __eq__(self, other):
        return (isinstance(self, Government) and isinstance(other, Government)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class Influence(ReprMixin, TimestampMixin, Base):
    """ Represents influence of a faction in a system. """
    __tablename__ = "influence"
    _repr_keys = ['system_id', 'faction_id', 'happiness_id', 'influence', 'is_controlling_faction', 'updated_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), nullable=False)
    happiness_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_happiness.id'), nullable=True)
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), nullable=False)

    influence = sqla.Column(sqla.Numeric(7, 4, None, False), default=0.0)
    is_controlling_faction = sqla.Column(sqla.Boolean, default=False)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    happiness = sqla.orm.relationship('FactionHappiness', lazy='joined', viewonly=True)
    system = sqla.orm.relationship('System', viewonly=True)
    faction = sqla.orm.relationship('Faction', viewonly=True)

    def __eq__(self, other):
        return (isinstance(self, Influence) and isinstance(other, Influence)
                and self.system_id == other.system_id
                and self.faction_id == other.faction_id)

    def update(self, **kwargs):
        """
        Simple kwargs update to this object.

        If update_at not present will use current timestamp.
        """
        for key, val in kwargs.items():
            if key not in ('active_states', 'pending_states', 'recovering_states'):
                setattr(self, key, val)


class Module(ReprMixin, UpdatableMixin, Base):
    """ A module for a ship. """
    __tablename__ = "modules"
    _repr_keys = ['id', 'name', 'group_id', 'size', 'rating', 'mass', 'price', 'ship', 'weapon_mode']

    id = sqla.Column(sqla.Integer, primary_key=True)
    group_id = sqla.Column(sqla.Integer, sqla.ForeignKey('module_groups.id'))

    size = sqla.Column(sqla.Integer)  # Equal to in game size, 1-8.
    rating = sqla.Column(sqla.String(1))  # Rating is A-E
    price = sqla.Column(sqla.Integer, default=0)
    mass = sqla.Column(sqla.Integer, default=0)
    name = sqla.Column(sqla.String(LEN["module"]))  # Pacifier
    ship = sqla.Column(sqla.String(LEN["ship"]))  # Module sepfically for this ship
    weapon_mode = sqla.Column(sqla.String(LEN["weapon_mode"]))  # Fixed, Gimbal or Turret

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


class ModuleGroup(ReprMixin, Base):
    """ A group for a module. """
    __tablename__ = "module_groups"
    _repr_keys = ['id', 'name', 'category', 'category_id']

    id = sqla.Column(sqla.Integer, primary_key=True)
    category_id = sqla.Column(sqla.Integer)

    category = sqla.Column(sqla.String(LEN["module_category"]))
    name = sqla.Column(sqla.String(LEN["module_group"]))  # Name of module group, i.e. "Beam Laser"

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


class Power(ReprMixin, Base):
    """ Represents a powerplay leader. """
    __tablename__ = "powers"
    _repr_keys = ['id', 'text', 'eddn', 'abbrev', 'home_system_name']

    id = sqla.Column(sqla.Integer, primary_key=True, nullable=True, autoincrement=False)
    text = sqla.Column(sqla.String(LEN["power"]))
    eddn = sqla.Column(sqla.String(LEN["power"]))
    abbrev = sqla.Column(sqla.String(LEN["power_abv"]))
    home_system_name = sqla.Column(sqla.String(LEN["system"]))

    # Relationships
    home_system = sqla.orm.relationship(
        'System', uselist=False, lazy='select', viewonly=True,
        primaryjoin='foreign(System.name) == Power.home_system_name',
    )

    def __eq__(self, other):
        return (isinstance(self, Power) and isinstance(other, Power)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class PowerState(ReprMixin, Base):
    """
    Represents the power state of a system (i.e. control, exploited).
    """
    __tablename__ = "power_state"
    _repr_keys = ['id', 'text', 'eddn']

    id = sqla.Column(sqla.Integer, primary_key=True, nullable=True, autoincrement=False)
    text = sqla.Column(sqla.String(LEN["power_state"]))
    eddn = sqla.Column(sqla.String(LEN["power_state"]))

    def __eq__(self, other):
        return (isinstance(self, PowerState) and isinstance(other, PowerState)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class Security(ReprMixin, Base):
    """ Security states of a system. """
    __tablename__ = "security"
    _repr_keys = ['id', 'text', 'eddn']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["security"]))
    eddn = sqla.Column(sqla.String(LEN["eddn"]))

    def __eq__(self, other):
        return (isinstance(self, Security) and isinstance(other, Security)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class SettlementSecurity(ReprMixin, Base):
    """ The security of a settlement. """
    __tablename__ = "settlement_security"
    _repr_keys = ['id', 'text']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["settlement_security"]))

    def __eq__(self, other):
        return (isinstance(self, SettlementSecurity) and isinstance(other, SettlementSecurity)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class SettlementSize(ReprMixin, Base):
    """ The size of a settlement. """
    __tablename__ = "settlement_size"
    _repr_keys = ['id', 'text']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["settlement_size"]))

    def __eq__(self, other):
        return (isinstance(self, SettlementSize) and isinstance(other, SettlementSize)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class Ship(ReprMixin, Base):
    """
    Constants for ship types in the game.
    """
    __tablename__ = 'ships'
    _repr_keys = ['id', 'text', 'traffic_text']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["ship"]))
    traffic_text = sqla.Column(sqla.String(LEN["ship"]))
    eddn = sqla.Column(sqla.String(LEN["ship"]))

    def __str__(self):
        """ A pretty one line to give all information. """
        return f"Ship: {self.text}"

    def __eq__(self, other):
        return isinstance(other, Ship) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.id}")


class ShipSold(ReprMixin, UpdatableMixin, Base):
    """
    Table to store the ships sold at particular stations.
    """
    __tablename__ = 'station_ships_sold'
    __table_args__ = (
        UniqueConstraint('station_id', 'ship_id', name='station_ship_sold_unique'),
    )
    _repr_keys = [
        'id', 'station_id', 'ship_id',
    ]

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    station_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey("stations.id"), nullable=False)
    ship_id = sqla.Column(sqla.Integer, sqla.ForeignKey("ships.id"), nullable=False)

    ship = sqla_orm.relationship('Ship', uselist=False, viewonly=True, lazy='joined')

    def __eq__(self, other):
        return (isinstance(self, ShipSold) and isinstance(other, ShipSold)
                and hash(self) == hash(other))

    def __hash__(self):
        return hash(f'{self.station_id}_{self.ship_id}')


class StationFeatures(ReprMixin, TimestampMixin, UpdatableMixin, Base):
    """ The features at a station. """
    __tablename__ = "station_features"
    _repr_keys = [
        'id', 'blackmarket', 'carrier_administration', 'carrier_vendor', 'commodities',
        'dock', 'interstellar_factors', 'market', 'outfitting', 'rearm', 'refuel',
        'repair', 'shipyard', 'technology_broker', 'universal_cartographics', 'updated_at'
    ]

    id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('stations.id'), primary_key=True)

    blackmarket = sqla.Column(sqla.Boolean)
    carrier_administration = sqla.Column(sqla.Boolean)
    carrier_vendor = sqla.Column(sqla.Boolean)
    commodities = sqla.Column(sqla.Boolean)
    dock = sqla.Column(sqla.Boolean)
    interstellar_factors = sqla.Column(sqla.Boolean)
    market = sqla.Column(sqla.Boolean)
    material_trader = sqla.Column(sqla.Boolean)
    outfitting = sqla.Column(sqla.Boolean)
    rearm = sqla.Column(sqla.Boolean)
    refuel = sqla.Column(sqla.Boolean)
    repair = sqla.Column(sqla.Boolean)
    shipyard = sqla.Column(sqla.Boolean)
    technology_broker = sqla.Column(sqla.Boolean)
    universal_cartographics = sqla.Column(sqla.Boolean)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Realtionships
    station = sqla.orm.relationship('Station', uselist=False, viewonly=True)

    def __eq__(self, other):
        return (isinstance(self, StationFeatures) and isinstance(other, StationFeatures)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class StationType(ReprMixin, Base):
    """ The type of a station, like Outpost and so on. """
    __tablename__ = "station_types"
    _repr_keys = ['id', 'text', 'eddn', 'is_planetary', 'max_landing_pad_size']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["station_type"]))
    eddn = sqla.Column(sqla.String(LEN["station_type"]))
    is_planetary = sqla.Column(sqla.Boolean, default=False)
    max_landing_pad_size = sqla.Column(sqla.String(LEN["station_pad"]))

    def __eq__(self, other):
        return (isinstance(self, StationType) and isinstance(other, StationType)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class StationEconomy(ReprMixin, Base):
    """ The economy of a station, can have up to 2 usually. """
    __tablename__ = "station_economies"
    _repr_keys = ['id', 'economy_id', 'primary', 'proportion']

    id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('stations.id'), primary_key=True)
    economy_id = sqla.Column(sqla.Integer, sqla.ForeignKey('economies.id'), primary_key=True)
    primary = sqla.Column(sqla.Boolean, primary_key=True, default=False)
    proportion = sqla.Column(sqla.Float)

    def __eq__(self, other):
        return (isinstance(self, StationEconomy) and isinstance(other, StationEconomy)
                and self.__hash__() == other.__hash__())

    def __hash__(self):
        return hash(f"{self.id}_{self.economy_id}")


class Station(ReprMixin, TimestampMixin, UpdatableMixin, Base):
    """ Repesents a station in a system in the universe. """
    __tablename__ = "stations"
    __table_args__ = (
        UniqueConstraint('name', 'system_id', name='station_name_system_id_unique'),
    )
    _repr_keys = [
        'id', 'name', 'distance_to_star', 'max_landing_pad_size',
        'type_id', 'system_id', 'controlling_minor_faction_id', 'updated_at'
    ]

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    controlling_minor_faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'))
    system_id = sqla.Column(sqla.Integer)
    type_id = sqla.Column(sqla.Integer, nullable=False)

    name = sqla.Column(sqla.String(LEN["station"]), index=True)
    distance_to_star = sqla.Column(sqla.Integer, default=0)
    is_planetary = sqla.Column(sqla.Boolean, default=False)
    max_landing_pad_size = sqla.Column(sqla.String(LEN["station_pad"]))
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    features = sqla.orm.relationship('StationFeatures', uselist=False, viewonly=True)
    type = sqla.orm.relationship(
        'StationType', uselist=False, viewonly=True,
        primaryjoin='foreign(Station.id) == remote(StationType.id)',
    )
    station_economies = sqla.orm.relationship(
        'StationEconomy', uselist=True, viewonly=True, lazy='select',
        primaryjoin='foreign(Station.id) == remote(StationEconomy.id)',
    )
    economies = sqla.orm.relationship(
        'Economy', uselist=True, viewonly=True, lazy='select',
        primaryjoin='and_(foreign(Station.id) == remote(StationEconomy.id), foreign(StationEconomy.economy_id) == Economy.id)',
    )
    faction = sqla.orm.relationship('Faction', uselist=False, viewonly=True)
    allegiance = sqla_orm.relationship(
        'Allegiance', uselist=False, viewonly=True, lazy='select',
        primaryjoin='and_(Station.controlling_minor_faction_id == remote(Faction.id), foreign(Faction.allegiance_id) == foreign(Allegiance.id))',
    )
    government = sqla_orm.relationship(
        'Government', uselist=False, viewonly=True, lazy='select',
        primaryjoin='and_(Station.controlling_minor_faction_id == remote(Faction.id), foreign(Faction.government_id) == foreign(Government.id))',
    )

    def __eq__(self, other):
        return isinstance(self, Station) and isinstance(other, Station) and self.id == other.id

    def __hash__(self):
        return hash(self.id)

    @classmethod
    def carrier(cls, *, name, station_id, system_id, distance_to_star=0):
        """
        Factory method to create player carrier stations.
        All carriers are stations but have the following specific or constant fields:
            name is unique globally and of form WWW-WWW where W is alphanumeric
            controlling_minor_faction_id = 77170
            type_id = 24
            is_planetary = False
            max_landing_pad_size = "L" (carriers have 8 L, 4 M and 4 S)

        Args:
            cls: This class.
            name: The name of the station.
            station_id: The id of the the carrier.
            system_id: The id of the system the carrier is currently seen in.
            distance_to_star: The distance from the star the carrier was observed at.

        Returns: A Station object preconfigured with constants.
        """
        return cls(**{
            'name': name,
            'id': station_id,
            'system_id': system_id,
            'distance_to_star': distance_to_star,
            'controlling_minor_faction_id': 77170,
            'type_id': 24,
            'is_planetary': False,
            'max_landing_pad_size': "L",
        })


class CarrierSighting(ReprMixin, TimestampMixin, Base):
    """ Repesents a carrier sighting in the universe. """
    __tablename__ = "carrier_sightings"
    __table_args__ = (
        UniqueConstraint('carrier_id', 'system_id', 'created_at', name='carrier_id_system_id_created_at_unique'),
    )
    _repr_keys = [
        'id', 'system_id', 'distance_to_star', 'created_at'
    ]

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    carrier_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('stations.id'))
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'))
    distance_to_star = sqla.Column(sqla.Integer)
    created_at = sqla.Column(sqla.Integer, default=time.time)

    # Relationships
    carrier = sqla.orm.relationship('Station', uselist=False, viewonly=True)
    system = sqla.orm.relationship('System', uselist=False, viewonly=True)

    def __eq__(self, other):
        return isinstance(self, CarrierSighting) and isinstance(other, CarrierSighting) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class System(ReprMixin, TimestampMixin, UpdatableMixin, Base):
    """
    Repesents a system in the universe.

    See SystemControlV for complete control information, especially for contesteds.
    """
    __tablename__ = "systems"
    _repr_keys = [
        'id', 'name', 'population', 'needs_permit', 'updated_at', 'power_id', 'edsm_id',
        'primary_economy_id', 'secondary_economy_id', 'security_id', 'power_state_id',
        'controlling_minor_faction_id', 'x', 'y', 'z'
    ]

    id = sqla.Column(sqla.Integer, primary_key=True)
    ed_system_id = sqla.Column(sqla.BigInteger, index=True)
    controlling_minor_faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), nullable=True)
    power_id = sqla.Column(sqla.Integer, sqla.ForeignKey('powers.id'), default=0)
    power_state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('power_state.id'), default=0)
    primary_economy_id = sqla.Column(sqla.Integer, sqla.ForeignKey('economies.id'), default=10)
    secondary_economy_id = sqla.Column(sqla.Integer, sqla.ForeignKey('economies.id'), default=10)
    security_id = sqla.Column(sqla.Integer, sqla.ForeignKey('security.id'))

    name = sqla.Column(sqla.String(LEN["system"]), index=True)
    population = sqla.Column(sqla.BigInteger)
    needs_permit = sqla.Column(sqla.Integer)
    edsm_id = sqla.Column(sqla.Integer)
    x = sqla.Column(sqla.Numeric(10, 5, None, False))
    y = sqla.Column(sqla.Numeric(10, 5, None, False))
    z = sqla.Column(sqla.Numeric(10, 5, None, False))
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    primary_economy = sqla.orm.relationship(
        'Economy', uselist=False, viewonly=True, lazy='select',
        primaryjoin='foreign(System.primary_economy_id) == Economy.id'
    )
    secondary_economy = sqla.orm.relationship(
        'Economy', uselist=False, viewonly=True, lazy='select',
        primaryjoin='foreign(System.primary_economy_id) == Economy.id'
    )
    power = sqla.orm.relationship('Power', viewonly=True)
    power_state = sqla.orm.relationship('PowerState', viewonly=True)
    security = sqla.orm.relationship('Security', viewonly=True)
    active_states = sqla_orm.relationship(
        'FactionActiveState', viewonly=True, lazy='select',
        primaryjoin='and_(remote(FactionActiveState.system_id) == foreign(System.id), remote(FactionActiveState.faction_id) == foreign(System.controlling_minor_faction_id))',
    )
    allegiance = sqla_orm.relationship(
        'Allegiance', uselist=False, viewonly=True, lazy='select',
        primaryjoin='and_(System.controlling_minor_faction_id == remote(Faction.id), foreign(Faction.allegiance_id) == foreign(Allegiance.id))',
    )
    government = sqla_orm.relationship(
        'Government', uselist=False, viewonly=True, lazy='select',
        primaryjoin='and_(System.controlling_minor_faction_id == remote(Faction.id), foreign(Faction.government_id) == foreign(Government.id))',
    )
    controls = sqla_orm.relationship(
        'System', uselist=True, viewonly=True, lazy='select', order_by='System.name',
        primaryjoin='and_(foreign(System.id) == remote(SystemControl.system_id), foreign(SystemControl.control_id) == remote(System.id))'
    )
    exploiteds = sqla_orm.relationship(
        'System', uselist=True, viewonly=True, lazy='select', order_by='System.name',
        primaryjoin='and_(foreign(System.id) == remote(SystemControl.control_id), foreign(SystemControl.system_id) == remote(System.id))'
    )
    contesteds = sqla_orm.relationship(
        'System', uselist=True, viewonly=True, lazy='select', order_by='System.name',
        primaryjoin='and_(foreign(System.id) == remote(SystemContestedV.control_id), foreign(SystemContestedV.system_id) == remote(System.id))',
    )

    @hybrid_property
    def is_populated(self):
        """
        Is the system populated?
        """
        return self.population and self.population > 0

    @is_populated.expression
    def is_populated(self):
        """
        Compute the distance from this system to other.
        """
        return self.population is not None and self.population > 0

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
        return sqla.func.sqrt((other.x - self.x) * (other.x - self.x)
                              + (other.y - self.y) * (other.y - self.y)
                              + (other.z - self.z) * (other.z - self.z))

    def calc_upkeep(self, system):
        """ Approximates the default upkeep. """
        dist = self.dist_to(system)
        return round(20 + 0.001 * (dist * dist), 1)

    def calc_fort_trigger(self, system):
        """ Approximates the default fort trigger. """
        dist = self.dist_to(system)
        return round(5000 - 5 * dist + 0.4 * (dist * dist))

    def calc_um_trigger(self, system, reinforced=0):
        """" Aproximates the default undermining trigger. """
        normal_trigger = round(5000 + (2750000 / math.pow(self.dist_to(system), 1.5)))
        return round(normal_trigger * (1 + (reinforced / 100)))

    def __eq__(self, other):
        return isinstance(self, System) and isinstance(other, System) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class SystemContestedV(ReprMixin, Base):
    """
    This table is a __VIEW__. See VIEW_CONTESTEDS.

    This view simply selects down only those contested systems.
    """
    __tablename__ = 'v_systems_contested'
    _repr_keys = ['system_id', 'system', 'control_id', 'control', 'power_id', 'power']

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    power_id = sqla.Column(sqla.Integer)
    control_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True, )

    system = sqla.Column(sqla.String(LEN['system']))
    control = sqla.Column(sqla.String(LEN['system']))
    power = sqla.Column(sqla.String(LEN['power']))

    def __eq__(self, other):
        return (isinstance(self, SystemContestedV) and isinstance(other, SystemContestedV)
                and self.__hash__() == other.__hash__())

    def __hash__(self):
        return hash(f"{self.id}_{self.control_id}")


class SystemControlV(ReprMixin, Base):
    """
    This table is a __VIEW__. See VIEW_SYSTEM_CONTROLS.

    This view augments SystemControl with joined text information.
    """
    __tablename__ = "v_systems_controlled"
    _repr_keys = [
        'system_id', 'system', 'power_state_id', 'power_state',
        'control_id', 'control', 'power_id', 'power'
    ]

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True, )
    power_state_id = sqla.Column(sqla.Integer)
    control_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True, )
    power_id = sqla.Column(sqla.Integer)

    system = sqla.Column(sqla.String(LEN["system"]))
    power_state = sqla.Column(sqla.String(LEN['power_state']))
    control = sqla.Column(sqla.String(LEN["system"]))
    power = sqla.Column(sqla.String(LEN['power']))

    def __str__(self):
        return f"{self.system} controlled by {self.control} ({self.power})"

    def __eq__(self, other):
        return isinstance(self, SystemControlV) and isinstance(other, SystemControlV) and \
            hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.system_id}_{self.control_id}")


class SystemControl(ReprMixin, Base):
    """
    This table stores all pairs of systems and their controls.
    Importantly for this consideration a control system is not paired with itself.
    Use this system mainly for joins of the IDs, for query use the augmented VIEW above.
    """
    __tablename__ = "systems_controlled"
    _repr_keys = ['system_id', 'power_state_id', 'control_id', 'power_id']

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    control_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    power_id = sqla.Column(sqla.Integer, sqla.ForeignKey('powers.id'))
    power_state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('power_state.id'))

    def __eq__(self, other):
        return isinstance(self, SystemControl) and isinstance(other, SystemControl) and \
            hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.system_id}_{self.control_id}")


class ConflictState(ReprMixin, Base):
    """
    Defines the different states possible for conflicts.
    """
    __tablename__ = 'conflict_states'
    _repr_keys = ['id', 'text', 'eddn']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["faction_state"]))
    eddn = sqla.Column(sqla.String(LEN["faction_state"]))

    def __eq__(self, other):
        return (isinstance(self, ConflictState) and isinstance(other, ConflictState)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class Conflict(ReprMixin, TimestampMixin, UpdatableMixin, Base):
    """
    Defines an in system conflict between two factions.
    """
    __tablename__ = 'conflicts'
    _repr_keys = [
        'system_id', 'status_id', 'type_id',
        'faction1_id', 'faction1_stake_id', 'faction1_days',
        'faction2_id', 'faction2_stake_id', 'faction2_days'
    ]

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    status_id = sqla.Column(sqla.Integer, sqla.ForeignKey('conflict_states.id'))
    type_id = sqla.Column(sqla.Integer, sqla.ForeignKey('conflict_states.id'))
    faction1_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), primary_key=True)
    faction1_stake_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('stations.id'))
    faction2_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), primary_key=True)
    faction2_stake_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('stations.id'))

    faction1_days = sqla.Column(sqla.Integer)
    faction2_days = sqla.Column(sqla.Integer)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    system = sqla_orm.relationship('System', viewonly=True)
    status = sqla_orm.relationship(
        'ConflictState', viewonly=True, lazy='select',
        primaryjoin='foreign(Conflict.status_id) == ConflictState.id',
    )
    type = sqla_orm.relationship(
        'ConflictState', viewonly=True, lazy='select',
        primaryjoin='foreign(Conflict.type_id) == ConflictState.id',
    )
    faction1 = sqla_orm.relationship(
        'Faction', viewonly=True, lazy='select',
        primaryjoin='foreign(Conflict.faction1_id) == Faction.id',
    )
    faction2 = sqla_orm.relationship(
        'Faction', viewonly=True, lazy='select',
        primaryjoin='foreign(Conflict.faction2_id) == Faction.id',
    )
    faction1_stake = sqla_orm.relationship(
        'Station', viewonly=True, lazy='select',
        primaryjoin='foreign(Conflict.faction1_stake_id) == Station.id',
    )
    faction2_stake = sqla_orm.relationship(
        'Station', viewonly=True, lazy='select',
        primaryjoin='foreign(Conflict.faction2_stake_id) == Station.id',
    )

    def __eq__(self, other):
        return (isinstance(self, Conflict) and isinstance(other, Conflict)
                and self.__hash__() == other.__hash__())

    def __hash__(self):
        return hash(f"{self.system_id}_{self.faction1_id}_{self.faction2_id}")


class HistoryTrack(ReprMixin, TimestampMixin, Base):
    """
    Set an entry to flag this system should be tracked.
    """
    __tablename__ = 'history_systems'
    _repr_keys = ['system_id', 'updated_at']

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    system = sqla_orm.relationship('System', viewonly=True)

    def __eq__(self, other):
        return (isinstance(self, HistoryTrack) and isinstance(other, HistoryTrack)
                and self.__hash__() == other.__hash__())

    def __hash__(self):
        return hash(f"{self.system_id}")


# N.B. Ever increasing data, the following rules must be enforced:
#   - Prune data older than X days, run nightly.
#       See EVENT_HISTORY_INFLUENCE
#   - With add_history_influence enforce following:
#       LIMIT total number of entries per key pair
#       Enforce only new data when inf is different than last and min gap in time from last
class HistoryInfluence(ReprMixin, TimestampMixin, Base):
    """ Represents a frozen state of influence for a faction in a system at some point in time. """
    __tablename__ = "history_influence"
    _repr_keys = ['id', 'system_id', 'faction_id', 'happiness_id', 'influence', 'is_controlling_faction', 'updated_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), nullable=False)
    faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), nullable=False)
    happiness_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_happiness.id'), nullable=True)

    influence = sqla.Column(sqla.Numeric(7, 4, None, False), default=0.0)
    is_controlling_faction = sqla.Column(sqla.Boolean, default=False)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    system = sqla.orm.relationship('System', viewonly=True)
    faction = sqla.orm.relationship('Faction', viewonly=True)
    happiness = sqla.orm.relationship('FactionHappiness', viewonly=True)

    def __hash__(self):
        return hash(f"{self.id}_{self.system_id}_{self.faction_id}")

    def __eq__(self, other):
        return (isinstance(self, HistoryInfluence) and isinstance(other, HistoryInfluence)
                and hash(self) == hash(other))

    @classmethod
    def from_influence(cls, influence):
        """
        Create a HistoryInfluence object from an existing Influence record.

        Args:
            cls: The class itself, this is a classmethod.
            influence: The Influence object to base upon.
        """
        return cls(
            system_id=influence.system_id,
            faction_id=influence.faction_id,
            happiness_id=influence.happiness_id,
            influence=influence.influence,
            is_controlling_faction=influence.is_controlling_faction,
            updated_at=influence.updated_at,
        )


# Bidirectional relationships
Commodity.category = sqla_orm.relationship(
    'CommodityCat', uselist=False, back_populates='commodities', lazy='select')
CommodityCat.commodities = sqla_orm.relationship(
    'Commodity', cascade='all, delete, delete-orphan', back_populates='category', lazy='select')
Module.group = sqla_orm.relationship(
    'ModuleGroup', uselist=False, back_populates='modules', lazy='select')
ModuleGroup.modules = sqla_orm.relationship(
    'Module', cascade='all, delete, delete-orphan', back_populates='group', lazy='select')

Faction.home_system = sqla_orm.relationship(
    'System', uselist=False, back_populates='controlling_faction', lazy='select',
    primaryjoin='foreign(Faction.home_system_id) == System.id')
System.controlling_faction = sqla_orm.relationship(
    'Faction', uselist=False, lazy='select',
    primaryjoin='foreign(System.controlling_minor_faction_id) == Faction.id')
FactionActiveState.influence = sqla_orm.relationship(
    'Influence', uselist=False, lazy='select',
    primaryjoin='and_(foreign(Influence.faction_id) == FactionActiveState.faction_id, foreign(Influence.system_id) == FactionActiveState.system_id)')
Influence.active_states = sqla_orm.relationship(
    'FactionActiveState', cascade='all, delete, delete-orphan', lazy='joined',
    primaryjoin='and_(foreign(FactionActiveState.faction_id) == Influence.faction_id, foreign(FactionActiveState.system_id) == Influence.system_id)')
FactionPendingState.influence = sqla_orm.relationship(
    'Influence', uselist=False, lazy='select', viewonly=True,
    primaryjoin='and_(foreign(Influence.faction_id) == FactionPendingState.faction_id, foreign(Influence.system_id) == FactionPendingState.system_id)')
Influence.pending_states = sqla_orm.relationship(
    'FactionPendingState', cascade='all, delete, delete-orphan', lazy='joined',
    primaryjoin='and_(foreign(FactionPendingState.faction_id) == Influence.faction_id, foreign(FactionPendingState.system_id) == Influence.system_id)')
FactionRecoveringState.influence = sqla_orm.relationship(
    'Influence', uselist=False, lazy='select', viewonly=True,
    primaryjoin='and_(foreign(Influence.faction_id) == FactionRecoveringState.faction_id, foreign(Influence.system_id) == FactionRecoveringState.system_id)')
Influence.recovering_states = sqla_orm.relationship(
    'FactionRecoveringState', cascade='all, delete, delete-orphan', lazy='joined',
    primaryjoin='and_(foreign(FactionRecoveringState.faction_id) == Influence.faction_id, foreign(FactionRecoveringState.system_id) == Influence.system_id)')

Station.system = sqla_orm.relationship(
    'System', back_populates='stations', lazy='select',
    primaryjoin='System.id == foreign(Station.system_id)',
)
System.stations = sqla_orm.relationship(
    'Station', back_populates='system', uselist=True, lazy='select',
    primaryjoin='System.id == foreign(Station.system_id)',
)


def preload_system_aliases(session):
    """
    Preload several shortcut names for systems either unpopulated or long.
    Bit of a hack, mainly need their x, y, z. IDs are put at the back, likely never hit.

    Args:
        session: A session onto the EDDB db.
    """
    # https://eddb.io/attraction/73536  | Dav's Hope
    # https://eddb.io/attraction/73512  | Jameson Crash Site
    # https://eddb.io/attraction/73522  | Anaconda Shipwreck
    session.query(System).\
        filter(System.id.in_([99999990, 99999991, 99999992])).\
        delete()
    session.add_all([
        System(
            id=99999990,
            name='Davs',
            x=-104.625,
            y=-0.8125,
            z=-151.90625,
            updated_at=1,
        ), System(
            id=99999991,
            name='Cobra',
            x=-101.90625,
            y=-95.46875,
            z=-165.59375,
            updated_at=1,
        ), System(
            id=99999992,
            name='Anaconda',
            x=68.84375,
            y=48.75,
            z=76.75,
            updated_at=1,
        )
    ])


# PowerState notes
#   HomeSystem and Prepared are EDDN only states.
#   HomeSystem is redundant as I map Power.home_system
#   Prepared can be tracked shouldn't overlap
#   InPreparedRadius is from Spansh
# StationType: Alias: 'Bernal' -> 'Ocellus'
def preload_tables(eddb_session):
    """
    Preload all constant data into the core EDDB tables.
    These tables are based on older EDDB.io data.
    Upon return, data for each table is guaranteed to be loaded.

    Deprecation Note:
        Commodity, CommodityCat, Module and ModuleGroup
        are all deprecated in favour of
        cogdb.spansh.{SCommodity, SCommodityGroup, SModule, SModuleGroup}

    Args:
        eddb_session: A session onto the EDDB.
    """
    classes = [
        Allegiance,
        CommodityCat,
        ConflictState,
        Economy,
        FactionHappiness,
        FactionState,
        Government,
        ModuleGroup,
        Power,
        PowerState,
        Security,
        SettlementSecurity,
        SettlementSize,
        Ship,
        StationType,
    ]
    for cls in classes:
        cogdb.common.preload_table_from_file(eddb_session, cls=cls)

    preload_system_aliases(eddb_session)
    eddb_session.commit()


def get_power_by_name(session, partial=''):
    """
    Given part of a name of a power in game, resolve the Power of the database and return it.

    Args:
        session: A session onto the db.
        partial: Part of a name, could be abbreviation or part of the name. So long as it resolves to one power valid.

    Returns: A Power who was matched. Alternatively if no match found, return None.
    """
    try:
        return session.query(Power).\
            filter(Power.abbrev == partial).\
            one()
    except sqla_orm.exc.NoResultFound:
        pass

    try:
        # If not modified to be loose, modify both sides
        if partial[0] != '%' and partial[-1] != '%':
            partial = f'%{partial}%'
        return session.query(Power).\
            filter(Power.text.ilike(partial)).\
            one()
    except sqla_orm.exc.NoResultFound as exc:
        powers = session.query(Power).\
            filter(Power.id != 0).\
            all()
        msg = "To match a power, use any of these abreviations:\n\n"\
            + '\n'.join([f"{pow.abbrev:6} => {pow.text}" for pow in powers])
        raise cog.exc.InvalidCommandArgs(msg) from exc

    return None


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


def get_systems_around(session, centre_name, distance=CONTROL_DISTANCE):
    """
    Given a central system and a distance, return all systems around it.
    Includes the centre system.

    Returns:
        [System, System, ...]

    Raises:
        centre_name was not found then exception.
    """
    centre = session.query(System).filter(System.name == centre_name).one()
    return session.query(System).\
        filter(System.dist_to(centre) <= distance).\
        order_by(System.name).\
        all()


def get_influences_by_id(eddb_session, influence_ids):
    """
    Get a list of Influence objects by their ids.

    Args:
        eddb_session: A session onto the db.
        influence_ids: A list of id numbers.

    Returns: A list of Influence objects found for ids.
    """
    return eddb_session.query(Influence).\
        join(System, Influence.system_id == System.id).\
        filter(Influence.id.in_(influence_ids)).\
        order_by(System.name, Influence.influence.desc()).\
        all()


def base_get_stations(session, centre_name, *, sys_dist=DEFAULT_DIST, arrival=DEFAULT_ARRIVAL):
    """A base query to get all stations around a centre_name System.
    Applies base criteria filters for max system distance from centre and arrival distance in system.

    Given a reference centre system, find nearby orbitals within:
        < sys_dist ly from original system
        < arrival ls from the entry start

    Importantly station_system(System), Station, StationType and StationFeatures are available to further
    filter the query.

    Returns:
        A partially completed query based on above, has no extra filters.
    """
    exclude = session.query(StationType.text).\
        filter(
            or_(
                StationType.text.like("%Planet%"),
                StationType.text.like("%Fleet%"))).\
        scalar_subquery()

    centre = sqla_orm.aliased(System)
    station_system = sqla_orm.aliased(System)
    stations = session.query(station_system.name, station_system.dist_to(centre).label('dist_c'),
                             Station.name, Station.distance_to_star, Station.max_landing_pad_size).\
        select_from(station_system).\
        join(centre, centre.name == centre_name).\
        join(Station, Station.system_id == station_system.id).\
        join(StationType, Station.type_id == StationType.id).\
        join(StationFeatures, Station.id == StationFeatures.id).\
        filter(
            station_system.dist_to(centre) < sys_dist,
            Station.distance_to_star < arrival,
            StationType.text.notin_(exclude))

    return stations


def get_nearest_stations_with_features(session, centre_name, *, features=None, sys_dist=DEFAULT_DIST, arrival=DEFAULT_ARRIVAL, include_medium=False):
    """Find the nearest stations with the required features present.
    Features is a list of strings that are possible features on StationFeatures.
    Returned results will have stations with ALL requested features.

    Returns:
        List of matches:
            [system_name, system_dist, station_name, station_arrival_distance]
    """
    stations = base_get_stations(session, centre_name, sys_dist=sys_dist, arrival=arrival)

    if not features:
        features = []
    for feature in features:
        stations = stations.filter(getattr(StationFeatures, feature))

    pads = ['M', 'L'] if include_medium else ['L']
    stations = stations.filter(Station.max_landing_pad_size.in_(pads))

    stations = stations.order_by('dist_c', Station.distance_to_star).\
        limit(20).\
        all()

    return [[a, round(b, 2), f"[{e}] {cog.util.shorten_text(c, 16)}", d]
            for [a, b, c, d, e] in stations]


def get_nearest_traders(session, centre_name, *,
                        trader_type=TraderType.BROKERS_GUARDIAN, sys_dist=DEFAULT_DIST, arrival=DEFAULT_ARRIVAL):
    """Find the nearest stations that sell broker equipment or trade mats.
    The determination is made based on station economy and features.

    Args:
        session: A session onto the db.
        centre_name: Search for stations close to this system.
        trader_type: A type in TraderType enum, indicating desired trader.
        sys_dist: The max distance away from centre_name to search.
        arrival: The max distance from arrival in system to allow.

    Returns:
        List of matches:
            [system_name, system_dist, station_name, station_arrival_distance]
    """
    exclude = session.query(StationType.text).\
        filter(
            or_(
                StationType.text.like("%Planet%"),
                StationType.text.like("%Fleet%"))).\
        scalar_subquery()
    high_econ_id = session.query(Economy.id).\
        filter(Economy.text == 'High Tech').\
        scalar_subquery()
    mil_econ_id = session.query(Economy.id).\
        filter(Economy.text == 'Military').\
        scalar_subquery()
    ind_econ_id = session.query(Economy.id).\
        filter(Economy.text == 'Industrial').\
        scalar_subquery()
    ref_econ_id = session.query(Economy.id).\
        filter(Economy.text == 'Refinery').\
        scalar_subquery()
    ext_econ_id = session.query(Economy.id).\
        filter(Economy.text == 'Extraction').\
        scalar_subquery()

    centre = sqla_orm.aliased(System)
    station_system = sqla_orm.aliased(System)
    stations = session.query(station_system.name, station_system.dist_to(centre).label('dist_c'),
                             Station.name, Station.distance_to_star, Station.max_landing_pad_size).\
        select_from(Station).\
        join(station_system, Station.system_id == station_system.id).\
        join(StationType, Station.type_id == StationType.id).\
        join(StationFeatures, Station.id == StationFeatures.id).\
        join(centre, centre.name == centre_name).\
        filter(
            station_system.population > 1000000,
            station_system.population < 22000000,
            station_system.dist_to(centre) < sys_dist,
            Station.distance_to_star < arrival,
            StationType.text.notin_(exclude))

    if trader_type == TraderType.BROKERS_GUARDIAN:
        stations = stations.filter(StationFeatures.technology_broker,
                                   station_system.primary_economy_id == high_econ_id)
    elif trader_type == TraderType.BROKERS_HUMAN:
        stations = stations.filter(StationFeatures.technology_broker,
                                   station_system.primary_economy_id != high_econ_id)
    elif trader_type == TraderType.MATS_DATA:
        stations = stations.filter(
            StationFeatures.material_trader,
            or_(
                station_system.primary_economy_id == high_econ_id,
                station_system.primary_economy_id == mil_econ_id,
                station_system.secondary_economy_id == high_econ_id,
                station_system.secondary_economy_id == mil_econ_id,
            )
        )
    elif trader_type == TraderType.MATS_RAW:
        stations = stations.filter(
            StationFeatures.material_trader,
            or_(
                station_system.primary_economy_id == ref_econ_id,
                station_system.primary_economy_id == ext_econ_id,
            )
        )
    elif trader_type == TraderType.MATS_MANUFACTURED:
        stations = stations.filter(
            StationFeatures.material_trader,
            or_(
                station_system.primary_economy_id == ind_econ_id,
                station_system.secondary_economy_id == ind_econ_id,
            )
        )

    stations = stations.order_by('dist_c', Station.distance_to_star).\
        limit(20).\
        all()

    return [[a, round(b, 2), f"[{e}] {cog.util.shorten_text(c, 16)}", d]
            for [a, b, c, d, e] in stations]


def get_shipyard_stations(session, centre_name, *, sys_dist=DEFAULT_DIST, arrival=DEFAULT_ARRIVAL, include_medium=False):
    """Get the nearest shipyard stations.
    Filter stations to find those with shipyards, these stations have all repair features.
    If mediums requested, filter to find all repair required features: repair, rearm, feuel, outfitting

    Returns:
        List of matches:
            [system_name, system_dist, station_name, station_arrival_distance]
    """
    return get_nearest_stations_with_features(
        session, centre_name, features=['outfitting', 'rearm', 'refuel', 'repair'],
        sys_dist=sys_dist, arrival=arrival, include_medium=include_medium
    )


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


def find_route_closest_hq(session, systems):
    """
    Given a set of system names in eddb:
        - Find the system that is closest to the HQ.
        - Route the remaining systems to minimize jump distance starting at closest to HQ.

    Args:
        session: Session onto the db.
        systems: A list of system names to route.

    Returns:
        [total_distance, [Systems]]
    """
    start = get_system_closest_to_HQ(session, systems)
    systems = [x for x in systems if x.lower() != start.name.lower()]

    return find_route(session, start, systems)


def get_nearest_controls(session, *, centre_name='sol', power='%hudson', limit=3):
    """
    Find nearest control systems of a particular power.

    Args:
        session: The EDDBSession variable.

    Kwargs:
        centre_name: The central system to find closest powers to.
        power: The power you are looking for.
        limit: The number of nearest controls to return, default 3.
    """
    centre = session.query(System).filter(System.name == centre_name).one()
    results = session.query(System).\
        join(Power, System.power_id == Power.id).\
        join(PowerState, System.power_state_id == PowerState.id).\
        filter(PowerState.text == 'Control',
               Power.text.ilike(power)).\
        order_by(System.dist_to(centre))

    if limit > 0:
        results = results.limit(limit)

    return results.all()


def get_controls_of_power(session, *, power='%hudson'):
    """
    Find the names of all controls of a given power.

    Args:
        session: The EDDBSession variable.

    Kwargs:
        power: The loose like match of the power, i.e. "%hudson".
    """
    return [
        x[0] for x in session.query(System.name).
        join(Power, System.power_id == Power.id).
        join(PowerState, System.power_state_id == PowerState.id).
        filter(PowerState.text == 'Control',
               Power.text.ilike(power)).
        order_by(System.name)
    ]


def get_systems_of_power(session, *, power='%hudson'):
    """
    Find the names of all exploited and contested systems of a power.

    Args:
        session: The EDDBSession variable.

    Kwargs:
        power: The loose like match of the power, i.e. "%hudson".
    """
    return [
        x[0] for x in session.query(System.name).
        join(Power, System.power_id == Power.id).
        filter(Power.text.ilike(power)).
        order_by(System.name)
    ]


def is_system_of_power(session, system_name, *, power='%hudson'):
    """
    Returns True if a system is under control of a given power.

    Args:
        session: The EDDBSession variable.
        system: The name of the system.

    Kwargs:
        power: The loose like match of the power, i.e. "%hudson".

    Returns: True if system is owned by power.
    """
    return session.query(SystemControlV.control).\
        filter(
            SystemControlV.power.ilike(power),
            sqla.or_(
                SystemControlV.system == system_name,
                SystemControlV.control == system_name
            )).\
        all()


def compute_dists(session, system_names):
    """
    Given a list of systems, compute the distance from the first to all others.

    Returns:
        Dict of {system: distance, ...}

    Raises:
        InvalidCommandArgs - One or more system could not be matched.
    """
    system_names = [name.lower() for name in system_names]
    try:
        centre = session.query(System).filter(System.name.ilike(system_names[0])).one()
    except sqla_orm.exc.NoResultFound as exc:
        raise cog.exc.InvalidCommandArgs(f"The start system {system_names[0]} was not found.") from exc
    systems = session.query(System.name, System.dist_to(centre)).\
        filter(System.name.in_(system_names[1:])).\
        order_by(System.name).\
        all()

    if len(systems) != len(system_names[1:]):
        for system in systems:
            system_names.remove(system[0].lower())

        msg = "Some systems were not found:\n%s" % "\n    " + "\n    ".join(system_names)
        raise cog.exc.InvalidCommandArgs(msg)

    return systems


def bgs_funcs(system_name):
    """
    Generate strong and weak functions to check gov_type text.

    Returns:
        strong(gov_type), weak(gov_type)
    """
    bgs = HUDSON_BGS
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        if is_system_of_power(eddb_session, system_name, power='%winters'):
            bgs = WINTERS_BGS

    def strong(gov_type):
        """ Strong vs these governments. """
        return gov_type in bgs[0]

    def weak(gov_type):
        """ Weak vs these governments. """
        return gov_type in bgs[1]

    return strong, weak


def get_power_hq(substr):
    """
    Loose match substr against keys in powers full names.

    Returns:
        [Full name of power, their HQ system name]

    Raises:
        InvalidCommandArgs - Unable to identify power from substring.
    """
    matches = [key for key in HQS if substr in key]
    if len(matches) != 1:
        msg = "Power must be substring of the following:"
        msg += "\n  " + "\n  ".join(sorted(HQS.keys()))
        raise cog.exc.InvalidCommandArgs(msg)

    return [string.capwords(matches[0]), HQS[matches[0]]]


def get_system_closest_to_HQ(session, systems, *, power='hudson'):
    """
    Return the system closest to the HQ of the given power.

    Args:
        session; A session onto the db.
        systems: A list of names of systems.

    Kwargs:
        power: A substring of the power we want to find closest system to.

    Returns: A System object of the closest system to the power's HQ.

    Raises:
        InvalidCommandArgs - A bad name of power was given.
    """
    _, hq_system = get_power_hq(power)
    subq_hq_system = session.query(System).\
        filter(System.name == hq_system).\
        one()

    return session.query(System).\
        filter(System.name.in_(systems)).\
        order_by(System.dist_to(subq_hq_system)).\
        limit(1).\
        one()


def get_closest_station_by_government(session, system, gov_type, *, limit=10):
    """Find the closest pirson megaship to designated system.

    Results will be a list of tuples of [(Station, System), ...] ordered by distance to system starting at.

    Args:
        session: A session onto the db.
        system: The system to find prison megaships near.
        limit: The number of matching stations to return.

    Raises:
        InvalidCommandArgs: Could not find the system.
    """
    try:
        found = session.query(System).\
            filter(System.name == system).\
            one()
    except sqla_orm.exc.NoResultFound as exc:
        raise cog.exc.InvalidCommandArgs(f"Could not find system: {system}\n\nPlease check for typos.") from exc

    return session.query(Station, System, System.dist_to(found)).\
        join(System, Station.system_id == System.id).\
        join(Faction, Station.controlling_minor_faction_id == Faction.id).\
        join(Government, Faction.government_id == Government.id).\
        filter(Government.text == gov_type).\
        order_by(System.dist_to(found)).\
        limit(limit).\
        all()


def get_all_systems_named(session, system_names, *, include_exploiteds=False):
    """Compute a full list of individual systems from a mixed list of controls and exploiteds.

    Args:
        session: A session onto the db.
        system_names: A list of system names.
        include_exploiteds: For any control system named, include all exploited systems in range.

    Returns: The list of system names resolved and those that were not found.
    """
    found = session.query(System).\
        filter(System.name.in_(system_names)).\
        all()

    exploits = []
    if include_exploiteds:
        for system in found:
            if system.power_state.text == "Control":
                exploits += get_systems_around(session, system.name)

    found_systems = sorted(list(set(found + exploits)), key=lambda x: x.name)
    found_names_lower = [x.name.lower() for x in found_systems]
    not_found = [x for x in system_names if x.lower() not in found_names_lower]

    return found_systems, not_found


def populate_system_controls(session):
    """
    Compute all pairs of control and exploited systems
    based on the current EDDB information.

    Insert the computed information into SystemControl objects.
    """
    session.query(SystemControl).delete()

    subq_pcontrol = session.query(PowerState.id).\
        filter(PowerState.text == 'Control').\
        scalar_subquery()
    control_ids = session.query(System.id).\
        filter(System.power_state_id == subq_pcontrol).\
        scalar_subquery()

    subq_pexploits = session.query(PowerState.id).\
        filter(PowerState.text.in_(['Exploited', 'Contested'])).\
        scalar_subquery()
    exploited = sqla_orm.aliased(System)
    systems = session.query(System.id, System.power_id, exploited.id, exploited.power_state_id).\
        filter(System.id.in_(control_ids)).\
        join(exploited, System.dist_to(exploited) <= 15).\
        filter(exploited.power_state_id.in_(subq_pexploits)).\
        all()

    for c_id, p_id, s_id, sp_id in systems:
        session.add(SystemControl(system_id=s_id, control_id=c_id, power_id=p_id,
                                  power_state_id=sp_id))


def add_history_track(eddb_session, system_names):
    """Add all systems to bgs tracking.

    Args:
        eddb_session: A session onto the db.
        systems: The list of systems to add.
    """
    system_ids = [
        x[0] for x in eddb_session.query(System.id).
        filter(System.name.in_(system_names)).
        all()
    ]
    eddb_session.query(HistoryTrack).\
        filter(HistoryTrack.system_id.in_(system_ids)).\
        delete()
    eddb_session.add_all([HistoryTrack(system_id=x) for x in system_ids])


def remove_history_track(eddb_session, system_names):
    """Add all systems to bgs tracking.

    Args:
        eddb_session: A session onto the db.
        systems: The list of systems to add.
    """
    system_ids = [
        x[0] for x in eddb_session.query(System.id).
        filter(System.name.in_(system_names)).
        all()
    ]
    eddb_session.query(HistoryTrack).\
        filter(HistoryTrack.system_id.in_(system_ids)).\
        delete()


def add_history_influence(eddb_session, latest_inf):
    """Add a history influence to the database.

    For the following, key pair means (system_id, faction_id).
    The following rules apply to adding data points:
        - Only add if latest_inf.system_id is in the HistoryTrack entries.
        - Ensure the number of entries for the key pair is <= HISTORY_INF_LIMIT
        - Add data points when the time since last point is > HISTORY_INF_TIME_GAP
            OR
        - Add data points when influence change > 1% and time since last point is > 1 hour (floor to prevent flood)

    Args:
        eddb_session: A session onto the db.
        latest_inf: An Influence entry that was updated (should be flush to db).
    """
    if not eddb_session.query(HistoryTrack).filter(HistoryTrack.system_id == latest_inf.system_id).all():
        return

    data = eddb_session.query(HistoryInfluence).\
        filter(
            HistoryInfluence.system_id == latest_inf.system_id,
            HistoryInfluence.faction_id == latest_inf.faction_id).\
        order_by(HistoryInfluence.updated_at.desc()).\
        all()

    # Only keep up to limit
    while len(data) > HISTORY_INF_LIMIT:
        last = data[-1]
        data = data[:-1]
        eddb_session.delete(last)

    if data:
        last = data[-1]
        # Influence stored as decimal of [0.0, 1.0]
        inf_diff = math.fabs(last.influence - latest_inf.influence)
        time_diff = latest_inf.updated_at - last.updated_at
        if (inf_diff >= 0.01 and time_diff > HOUR_SECONDS) or time_diff >= HISTORY_INF_TIME_GAP:
            eddb_session.add(HistoryInfluence.from_influence(latest_inf))

    else:
        eddb_session.add(HistoryInfluence.from_influence(latest_inf))


def service_status(eddb_session):
    """
    Poll for the status of the local EDDB database.

    Args:
        eddb_session: A session onto the EDDB db.

    Returns: A list of cells to format into a 2 column wide table.
    """
    now = datetime.datetime.utcnow()
    try:
        oldest = eddb_session.query(System).\
            order_by(System.updated_at.desc()).\
            limit(1).\
            one()
        cells = [
            ['Latest EDDB System Update', f'{oldest.name} ({now - oldest.updated_date} ago)']
        ]
    except sqla_orm.exc.NoResultFound:
        cells = [
            ['Oldest EDDB System', 'EDDB Database Empty'],
        ]

    return cells


def dump_db(session, classes, fname):
    """
    Dump db to a file.
    """
    with open(fname, "w", encoding='utf-8') as fout:
        for cls in classes:
            for obj in session.query(cls):
                fout.write(repr(obj) + '\n')


def drop_tables(*, all_tables=False):  # pragma: no cover | destructive to test
    """Ensure all safe to drop tables are dropped.

    Args:
        all_tables: When True, drop all EDDB tables.
    """
    meta = sqlalchemy.MetaData(bind=cogdb.eddb_engine)
    meta.reflect()
    for tbl in reversed(meta.sorted_tables):
        try:
            if is_safe_to_drop(tbl.name) or all_tables:
                tbl.drop()
        except sqla.exc.OperationalError:
            pass


def empty_tables(*, all_tables=False):
    """Ensure all safe to drop tables are empty.

    Args:
        all_tables: When True, empty all EDDB tables.
    """
    sqla.orm.session.close_all_sessions()

    meta = sqlalchemy.MetaData(bind=cogdb.eddb_engine)
    meta.reflect()
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        for tbl in reversed(meta.sorted_tables):
            try:
                if is_safe_to_drop(tbl.name) or all_tables:
                    eddb_session.query(tbl).delete()
            except sqla.exc.OperationalError:
                pass


def is_safe_to_drop(tbl_name):
    """Check if the table is safe to drop.

    Basically any table that can be reconstructed or imported from external can be dropped.
    For now, tables prefixed with 'spy_' and 'history_' will return False.
    """
    return not tbl_name.startswith('spy_') and not tbl_name.startswith('history_') and not tbl_name.startswith('pvp_')


# TODO: Bit messy but works for now.
#       Core SQLAlchemy lacks proper views, might be in libraries.
def recreate_tables():  # pragma: no cover | destructive to test
    """
    Recreate all tables in the database, mainly for schema changes and testing.
    """
    sqlalchemy.orm.session.close_all_sessions()

    drop_cmds = [
        "DROP VIEW eddb.v_systems_contested"
        "DROP VIEW eddb.v_systems_controlled",
        "DROP EVENT clean_conflicts",
        "DROP EVENT clean_history_influence",
    ]
    try:
        with cogdb.eddb_engine.connect() as con:
            for drop_cmd in drop_cmds:
                con.execute(sqla.sql.text(drop_cmd))
    except (sqla.exc.OperationalError, sqla.exc.ProgrammingError):
        pass

    drop_tables(all_tables=True)
    Base.metadata.create_all(cogdb.eddb_engine)
    try:
        SystemContestedV.__table__.drop(cogdb.eddb_engine)
    except sqla.exc.OperationalError:
        pass
    try:
        SystemControlV.__table__.drop(cogdb.eddb_engine)
    except sqla.exc.OperationalError:
        pass

    # Create views
    create_cmds = [
        VIEW_CONTESTEDS.strip(),
        VIEW_SYSTEM_CONTROLS.strip(),
        EVENT_CONFLICTS.strip(),
        EVENT_HISTORY_INFLUENCE.strip(),
    ]
    with cogdb.eddb_engine.connect() as con:
        for create_cmd in create_cmds:
            con.execute(sqla.sql.text(create_cmd))


async def monitor_eddb_caches(*, delay_hours=4):  # pragma: no cover
    """
    Monitor and recompute cached tables:
        - Repopulates SystemControls from latest data.

    Kwargs:
        delay_hours: The hours between refreshing cached tables. Default: 2
    """
    while True:
        await asyncio.sleep(delay_hours * 3600)

        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            await asyncio.get_event_loop().run_in_executor(
                None, populate_system_controls, eddb_session
            )


def main_test_area(eddb_session):  # pragma: no cover
    """ A test area for testing things with schema. """
    station = eddb_session.query(Station).filter(Station.is_planetary).limit(5).all()[0]
    print(station.name, station.economies)

    #  Check relationships
    system = eddb_session.query(System).filter(System.name == 'Sol').one()
    print(system.allegiance, system.government, system.power, system.power_state, system.security)
    print('------')
    print(system.controls, system.controlling_faction)
    print('------')
    print(system.stations)
    print('------')
    print(system.controlling_faction.home_system)
    print('------')

    station = eddb_session.query(Station).\
        filter(
            Station.system_id == system.id,
            Station.name == "Daedalus").\
        one()
    print(station.system, station.type, station.features, station.faction)

    __import__('pprint').pprint(get_nearest_stations_with_features(
        eddb_session, centre_name='rana'), features=['interstellar_factors']
    )
    stations = get_shipyard_stations(eddb_session, input("Please enter a system name ... "))
    if stations:
        print(cog.tbl.format_table(stations))

    print('31 Aquilae')
    sys_alias = sqla_orm.aliased(System)
    sys_control = sqla_orm.aliased(System)
    systems = eddb_session.query(sys_alias.name, sys_control.name).\
        filter(sys_alias.name == '31 Aquilae').\
        join(SystemControl, sys_alias.id == SystemControl.system_id).\
        join(sys_control, sys_control.id == SystemControl.control_id).\
        all()
    __import__('pprint').pprint(systems)
    print('Togher')
    system = eddb_session.query(sys_alias).\
        filter(sys_alias.name == 'Togher').\
        one()
    __import__('pprint').pprint(system.controls)
    print('Wat Yu')
    system = eddb_session.query(sys).\
        filter(sys_alias.name == 'Wat Yu').\
        one()
    __import__('pprint').pprint(system.exploiteds)
    __import__('pprint').pprint(system.contesteds)


try:
    Base.metadata.create_all(cogdb.eddb_engine)
    with cogdb.session_scope(cogdb.EDDBSession) as init_session:
        preload_tables(init_session)
        PLANETARY_TYPE_IDS = [
            x[0] for x in
            init_session.query(StationType.id).filter(StationType.text.ilike('%planetary%')).all()
        ]
        HQS = {
            p.text.lower(): p.home_system.name for p in
            init_session.query(Power).filter(Power.text != 'None').all()
        }
    del init_session
except (AttributeError, sqla_orm.exc.NoResultFound, sqla.exc.ProgrammingError):  # pragma: no cover
    PLANETARY_TYPE_IDS = None
    HQS = None
FACTION_STATE_PAIRS = [
    ('active_states', FactionActiveState),
    ('pending_states', FactionPendingState),
    ('recovering_states', FactionRecoveringState)
]


if __name__ == "__main__":
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        main_test_area(eddb_session)
