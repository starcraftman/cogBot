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
from sqlalchemy.sql.expression import or_
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method

import cog.exc
import cog.tbl
import cog.util
import cogdb
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
    _repr_keys = ['id', 'text', 'eddn']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["station_type"]))
    eddn = sqla.Column(sqla.String(LEN["station_type"]))

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
    """ Repesents a system in the universe. """
    __tablename__ = "stations"
    _repr_keys = [
        'id', 'name', 'distance_to_star', 'max_landing_pad_size',
        'type_id', 'system_id', 'controlling_minor_faction_id', 'updated_at'
    ]

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    controlling_minor_faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'))
    system_id = sqla.Column(sqla.Integer)
    type_id = sqla.Column(sqla.Integer, nullable=False)

    name = sqla.Column(sqla.String(LEN["station"]), index=True)
    distance_to_star = sqla.Column(sqla.Integer)
    is_planetary = sqla.Column(sqla.Boolean)
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


# NOTE: Map text '' -> 'None'
def preload_allegiance(session):
    """ Preload all Allegiance constants to table. """
    session.add_all([
        Allegiance(id=1, text="Alliance", eddn="Alliance"),
        Allegiance(id=2, text="Empire", eddn="Empire"),
        Allegiance(id=3, text="Federation", eddn="Federation"),
        Allegiance(id=4, text="Independent", eddn="Independent"),
        Allegiance(id=5, text="None", eddn="None"),
        Allegiance(id=7, text="Pilots Federation", eddn="PilotsFederation"),
        Allegiance(id=8, text="Thargoid", eddn="Thargoid"),
        Allegiance(id=9, text="Guardian", eddn="Guardian"),
    ])


def preload_commodity_categories(session):
    """ Preload all CommodityCat constants to table. """
    session.add_all([
        CommodityCat(id=1, name='Chemicals'),
        CommodityCat(id=2, name='Consumer Items'),
        CommodityCat(id=3, name='Legal Drugs'),
        CommodityCat(id=4, name='Foods'),
        CommodityCat(id=5, name='Industrial Materials'),
        CommodityCat(id=6, name='Machinery'),
        CommodityCat(id=7, name='Medicines'),
        CommodityCat(id=8, name='Metals'),
        CommodityCat(id=9, name='Minerals'),
        CommodityCat(id=10, name='Slavery'),
        CommodityCat(id=11, name='Technology'),
        CommodityCat(id=12, name='Textiles'),
        CommodityCat(id=13, name='Waste'),
        CommodityCat(id=14, name='Weapons'),
        CommodityCat(id=15, name='Unknown'),
        CommodityCat(id=16, name='Salvage'),
    ])


def preload_conflict_states(session):
    """ Preload all ConflictState constants to table. """
    session.add_all([
        ConflictState(id=1, text="Unknown", eddn=""),
        ConflictState(id=2, text="Active", eddn="active"),
        ConflictState(id=3, text="Pending", eddn="pending"),
        ConflictState(id=4, text="Civil War", eddn="civilwar"),
        ConflictState(id=5, text="Election", eddn="election"),
        ConflictState(id=6, text="War", eddn="war"),
    ])


def preload_economies(session):
    """ Preload all Economy constants to table. """
    session.add_all([
        Economy(id=1, text="Agriculture", eddn="Agri"),
        Economy(id=2, text="Extraction", eddn="Extraction"),
        Economy(id=3, text="High Tech", eddn="HighTech"),
        Economy(id=4, text="Industrial", eddn="Industrial"),
        Economy(id=5, text="Military", eddn="Military"),
        Economy(id=6, text="Refinery", eddn="Refinery"),
        Economy(id=7, text="Service", eddn="Service"),
        Economy(id=8, text="Terraforming", eddn="Terraforming"),
        Economy(id=9, text="Tourism", eddn="Tourism"),
        Economy(id=10, text="None", eddn="None"),
        Economy(id=11, text="Colony", eddn="Colony"),
        Economy(id=12, text="Private Enterprise", eddn="PrivateEnterprise"),
        Economy(id=13, text="Rescue", eddn="Rescue"),
        Economy(id=14, text="Prison", eddn="Prison"),
        Economy(id=15, text="Carrier", eddn="Carrier"),
        Economy(id=16, text="Damaged", eddn="Damaged"),
        Economy(id=17, text="Repair", eddn="Repair"),
        Economy(id=18, text="Engineering", eddn="Engineering"),
    ])


def preload_faction_happiness(session):
    """ Preload all FactionHappiness constants to table. """
    session.add_all([
        FactionHappiness(id=1, text='Elated', eddn='Elated'),
        FactionHappiness(id=2, text='Happy', eddn='Happy'),
        FactionHappiness(id=3, text='Discontented', eddn='Discontented'),
        FactionHappiness(id=4, text='Unhappy', eddn='Unhappy'),
        FactionHappiness(id=5, text='Despondent', eddn='Despondent'),
    ])


def preload_faction_state(session):
    """ Preload all FactionState constants to table. """
    session.add_all([
        FactionState(id=0, text="(unknown)", eddn=None),
        FactionState(id=16, text="Boom", eddn="Boom"),
        FactionState(id=32, text="Bust", eddn="Bust"),
        FactionState(id=37, text="Famine", eddn="Famine"),
        FactionState(id=48, text="Civil Unrest", eddn="CivilUnrest"),
        FactionState(id=64, text="Civil War", eddn="CivilWar"),
        FactionState(id=65, text="Election", eddn="Election"),
        FactionState(id=66, text="Civil Liberty", eddn="CivilLiberty"),
        FactionState(id=67, text="Expansion", eddn="Expansion"),
        FactionState(id=69, text="Lockdown", eddn="Lockdown"),
        FactionState(id=72, text="Outbreak", eddn="Outbreak"),
        FactionState(id=73, text="War", eddn="War"),
        FactionState(id=80, text="None", eddn="None"),
        FactionState(id=81, text="Pirate Attack", eddn="PirateAttack"),
        FactionState(id=96, text="Retreat", eddn="Retreat"),
        FactionState(id=101, text="Investment", eddn="Investment"),
        FactionState(id=102, text="Blight", eddn="Blight"),
        FactionState(id=103, text="Drought", eddn="Drought"),
        FactionState(id=104, text="Infrastructure Failure", eddn="InfrastructureFailure"),
        FactionState(id=105, text="Natural Disaster", eddn="NaturalDisaster"),
        FactionState(id=106, text="Public Holiday", eddn="PublicHoliday"),
        FactionState(id=107, text="Terrorist Attack", eddn="Terrorism"),
        FactionState(id=127, text="Civil Liberty", eddn="CivilLiberty"),
    ])


def preload_gov_type(session):
    """ Preload all Government constants to table. """
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
        Government(id=133, text='Imperial', eddn='Imperial'),
        Government(id=144, text='Patronage', eddn='Patronage'),
        Government(id=150, text='Prison Colony', eddn='PrisonColony'),
        Government(id=160, text='Theocracy', eddn='Theocracy'),
        Government(id=176, text='None', eddn='None'),
        Government(id=192, text='Engineer', eddn='Engineer'),
        Government(id=208, text='Prison', eddn='Prison'),
        Government(id=209, text='Private Ownership', eddn='PrivateOwnership'),
        Government(id=300, text='Carrier', eddn='Carrier'),
    ])


def preload_module_groups(session):
    """ Preload all ModuleGroup constants to table. """
    session.add_all([
        ModuleGroup(id=50, name='Lightweight Alloy', category='Bulkhead', category_id=40),
        ModuleGroup(id=51, name='Reinforced Alloy', category='Bulkhead', category_id=40),
        ModuleGroup(id=52, name='Military Grade Composite', category='Bulkhead', category_id=40),
        ModuleGroup(id=53, name='Mirrored Surface Composite', category='Bulkhead', category_id=40),
        ModuleGroup(id=54, name='Reactive Surface Composite', category='Bulkhead', category_id=40),
        ModuleGroup(id=55, name='Pulse Laser', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=56, name='Burst Laser', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=57, name='Beam Laser', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=58, name='Cannon', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=59, name='Fragment Cannon', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=60, name='Multi-Cannon', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=61, name='Plasma Accelerator', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=62, name='Rail Gun', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=63, name='Missile Rack', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=64, name='Mine Launcher', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=65, name='Torpedo Pylon', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=66, name='Chaff Launcher', category='Utility Mount', category_id=10),
        ModuleGroup(id=67, name='Electronic Countermeasure', category='Utility Mount', category_id=10),
        ModuleGroup(id=68, name='Heat Sink Launcher', category='Utility Mount', category_id=10),
        ModuleGroup(id=69, name='Point Defence', category='Utility Mount', category_id=10),
        ModuleGroup(id=70, name='Mining Laser', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=71, name='Standard Docking Computer', category='Internal Compartment', category_id=20),
        ModuleGroup(id=72, name='Power Plant', category='Essential Equipment', category_id=30),
        ModuleGroup(id=73, name='Thrusters', category='Essential Equipment', category_id=30),
        ModuleGroup(id=74, name='Frame Shift Drive', category='Essential Equipment', category_id=30),
        ModuleGroup(id=75, name='Life Support', category='Essential Equipment', category_id=30),
        ModuleGroup(id=76, name='Power Distributor', category='Essential Equipment', category_id=30),
        ModuleGroup(id=77, name='Sensors', category='Essential Equipment', category_id=30),
        ModuleGroup(id=78, name='Shield Generator', category='Internal Compartment', category_id=20),
        ModuleGroup(id=79, name='Shield Cell Bank', category='Internal Compartment', category_id=20),
        ModuleGroup(id=80, name='Cargo Rack', category='Internal Compartment', category_id=20),
        ModuleGroup(id=81, name='Fuel Tank', category='Essential Equipment', category_id=30),
        ModuleGroup(id=82, name='Hatch Breaker Limpet Controller', category='Internal Compartment', category_id=20),
        ModuleGroup(id=83, name='Cargo Scanner', category='Utility Mount', category_id=10),
        ModuleGroup(id=84, name='Frame Shift Wake Scanner', category='Utility Mount', category_id=10),
        ModuleGroup(id=85, name='Kill Warrant Scanner', category='Utility Mount', category_id=10),
        ModuleGroup(id=89, name='Detailed Surface Scanner', category='Internal Compartment', category_id=20),
        ModuleGroup(id=90, name='Fuel Scoop', category='Internal Compartment', category_id=20),
        ModuleGroup(id=91, name='Refinery', category='Internal Compartment', category_id=20),
        ModuleGroup(id=92, name='Frame Shift Drive Interdictor', category='Internal Compartment', category_id=20),
        ModuleGroup(id=93, name='Auto Field-Maintenance Unit', category='Internal Compartment', category_id=20),
        ModuleGroup(id=94, name='Shield Booster', category='Utility Mount', category_id=10),
        ModuleGroup(id=95, name='Hull Reinforcement Package', category='Internal Compartment', category_id=20),
        ModuleGroup(id=96, name='Collector Limpet Controller', category='Internal Compartment', category_id=20),
        ModuleGroup(id=97, name='Fuel Transfer Limpet Controller', category='Internal Compartment', category_id=20),
        ModuleGroup(id=98, name='Prospector Limpet Controller', category='Internal Compartment', category_id=20),
        ModuleGroup(id=99, name='Planetary Vehicle Hangar', category='Internal Compartment', category_id=20),
        ModuleGroup(id=100, name='Shock Mine Launcher', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=101, name='Bi-Weave Shield Generator', category='Internal Compartment', category_id=20),
        ModuleGroup(id=102, name='Planetary Approach Suite', category='Essential Equipment', category_id=30),
        ModuleGroup(id=103, name='Enhanced Performance Thrusters', category='Essential Equipment', category_id=30),
        ModuleGroup(id=104, name='Corrosion Resistant Cargo Rack', category='Internal Compartment', category_id=20),
        ModuleGroup(id=105, name='Fighter Hangar', category='Internal Compartment', category_id=20),
        ModuleGroup(id=106, name='Economy Class Passenger Cabin', category='Internal Compartment', category_id=20),
        ModuleGroup(id=107, name='Business Class Passenger Cabin', category='Internal Compartment', category_id=20),
        ModuleGroup(id=108, name='First Class Passenger Cabin', category='Internal Compartment', category_id=20),
        ModuleGroup(id=109, name='Luxury Passenger Cabin', category='Internal Compartment', category_id=20),
        ModuleGroup(id=110, name='Module Reinforcement Package', category='Internal Compartment', category_id=20),
        ModuleGroup(id=111, name='Repair Limpet Controller', category='Internal Compartment', category_id=20),
        ModuleGroup(id=112, name='AX Missile Rack', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=113, name='Xeno Scanner', category='Utility Mount', category_id=10),
        ModuleGroup(id=114, name='Research Limpet Controller', category='Internal Compartment', category_id=20),
        ModuleGroup(id=115, name='AX Multi-Cannon', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=116, name='Remote Release Flak Launcher', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=117, name='Shutdown Field Neutraliser', category='Utility Mount', category_id=10),
        ModuleGroup(id=118, name='Decontamination Limpet Controller', category='Internal Compartment', category_id=20),
        ModuleGroup(id=119, name='Recon Limpet Controller', category='Internal Compartment', category_id=20),
        ModuleGroup(id=120, name='Guardian Gauss Cannon', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=121, name='Guardian Hull Reinforcement', category='Internal Compartment', category_id=20),
        ModuleGroup(id=122, name='Guardian Module Reinforcement', category='Internal Compartment', category_id=20),
        ModuleGroup(id=123, name='Guardian Shield Reinforcement', category='Internal Compartment', category_id=20),
        ModuleGroup(id=124, name='Guardian FSD Booster', category='Internal Compartment', category_id=20),
        ModuleGroup(id=125, name='Guardian Hybrid Power Distributor', category='Internal Compartment', category_id=20),
        ModuleGroup(id=126, name='Guardian Hybrid Power Plant', category='Internal Compartment', category_id=20),
        ModuleGroup(id=127, name='Enzyme Missile Rack', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=128, name='Remote Release Flechette Launcher', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=129, name='Guardian Plasma Charger', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=130, name='Guardian Shard Cannon', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=131, name='Shock Cannon', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=132, name='Sub-Surface Displacement Missile', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=133, name='Abrasion Blaster', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=134, name='Seismic Charge Launcher', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=135, name='Pulse Wave Analyser', category='Utility Mount', category_id=10),
        ModuleGroup(id=136, name='Meta Alloy Hull Reinforcement', category='Internal Compartment', category_id=20),
        ModuleGroup(id=137, name='Limpet Control', category='Internal Compartment', category_id=20),
        ModuleGroup(id=138, name='Supercruise Assist', category='Internal Compartment', category_id=20),
        ModuleGroup(id=139, name='Advanced Docking Computer', category='Internal Compartment', category_id=20),
        ModuleGroup(id=140, name='Advanced Multi Cannon', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=141, name='Advanced Missile Rack', category='Weapon Hardpoint', category_id=50),
        ModuleGroup(id=142, name='Advanced Planetary Approach Suite', category='Essential Equipment', category_id=30),
        ModuleGroup(id=143, name='Mining Multi Limpet Controller', category='Internal Compartment', category_id=20),
        ModuleGroup(id=144, name='Operations Multi Limpet Controller', category='Internal Compartment', category_id=20),
        ModuleGroup(id=145, name='Rescue Multi Limpet Controller\t', category='Internal Compartment', category_id=20),
        ModuleGroup(id=146, name='Xeno Multi Limpet Controller', category='Internal Compartment', category_id=20),
        ModuleGroup(id=147, name='Universal Multi Limpet Controller', category='Internal Compartment', category_id=20),
    ])


def preload_powers(session):
    """ All possible powers in Powerplay. """
    session.add_all([
        Power(id=0, text="None", eddn='None', abbrev="NONE"),
        Power(id=1, text="Aisling Duval", eddn='Aisling Duval', abbrev="AD", home_system_name='Cubeo'),
        Power(id=2, text="Archon Delaine", eddn='Archon Delaine', abbrev="Archon", home_system_name='Harma'),
        Power(id=3, text="Arissa Lavigny-Duval", eddn='A. Lavigny-Duval', abbrev="ALD", home_system_name='Kamadhenu'),
        Power(id=4, text="Denton Patreus", eddn='Denton Patreus', abbrev="DP", home_system_name='Eotienses'),
        Power(id=5, text="Edmund Mahon", eddn='Edmund Mahon', abbrev="EM", home_system_name='Gateway'),
        Power(id=6, text="Felicia Winters", eddn='Felicia Winters', abbrev="FW", home_system_name='Rhea'),
        Power(id=7, text="Li Yong-Rui", eddn='Li Yong-Rui', abbrev="LYR", home_system_name='Lembava'),
        Power(id=8, text="Pranav Antal", eddn='Pranav Antal', abbrev="PA", home_system_name='Polevnic'),
        Power(id=9, text="Zachary Hudson", eddn='Zachary Hudson', abbrev="ZH", home_system_name='Nanomam'),
        Power(id=10, text="Zemina Torval", eddn='Zemina Torval', abbrev="ZT", home_system_name='Synteini'),
        Power(id=11, text="Yuri Grom", eddn='Yuri Grom', abbrev="YG", home_system_name='Clayakarma'),
    ])


def preload_power_states(session):
    """ All possible powerplay states. """
    session.add_all([
        PowerState(id=0, text="None", eddn='None'),
        PowerState(id=16, text="Control", eddn='Controlled'),
        PowerState(id=32, text="Exploited", eddn='Exploited'),
        PowerState(id=48, text="Contested", eddn='Contested'),
        PowerState(id=64, text="Expansion", eddn='Expansion'),
        PowerState(id=80, text="Prepared", eddn='Prepared'),  # EDDN only, system prepared
        PowerState(id=96, text="HomeSystem", eddn='HomeSystem'),  # EDDN only, HQ of a power
        PowerState(id=112, text="InPrepareRadius", eddn='InPrepareRadius'),  # EDDN only, HQ of a power
        PowerState(id=240, text="Turmoil", eddn='Turmoil'),
    ])
    # Note: HomeSystem and Prepared are EDDN only states.
    #       HomeSystem is redundant as I map Power.home_system
    #       Prepared can be tracked shouldn't overlap
    #       InPreparedRadius is from Spansh


def preload_security(session):
    """ Preload possible System security values. """
    session.add_all([
        Security(id=16, text="Low", eddn="low"),
        Security(id=32, text="Medium", eddn="medium"),
        Security(id=48, text="High", eddn="high"),
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


# Alias: 'Bernal' -> 'Ocellus'
def preload_station_types(session):
    """ Preload station types table. """
    session.add_all([
        StationType(id=1, text='Civilian Outpost', eddn='Outpost'),
        StationType(id=2, text='Commercial Outpost'),
        StationType(id=3, text='Coriolis Starport', eddn='Coriolis'),
        StationType(id=4, text='Industrial Outpost'),
        StationType(id=5, text='Military Outpost'),
        StationType(id=6, text='Mining Outpost'),
        StationType(id=7, text='Ocellus Starport', eddn='Ocellus'),
        StationType(id=8, text='Orbis Starport', eddn='Orbis'),
        StationType(id=9, text='Scientific Outpost'),
        StationType(id=10, text='Unsanctioned Outpost'),
        StationType(id=11, text='Unknown Outpost'),
        StationType(id=12, text='Unknown Starport'),
        StationType(id=13, text='Planetary Outpost', eddn='CraterOutpost'),
        StationType(id=14, text='Planetary Port', eddn='CraterPort'),
        StationType(id=15, text='Unknown Planetary'),
        StationType(id=16, text='Planetary Settlement'),
        StationType(id=17, text='Planetary Engineer Base'),
        StationType(id=19, text='Megaship', eddn='MegaShip'),
        StationType(id=20, text='Asteroid Base', eddn='AsteroidBase'),
        StationType(id=22, text='Unknown Dockable'),
        StationType(id=23, text='Non-Dockable Orbital'),
        StationType(id=24, text='Fleet Carrier', eddn='FleetCarrier'),
        StationType(id=25, text='Odyssey Settlement', eddn='OdysseySettlement'),
    ])


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


def preload_tables(session):
    """
    Preload all minor linked tables.
    """
    preload_allegiance(session)
    preload_commodity_categories(session)
    preload_conflict_states(session)
    preload_economies(session)
    preload_faction_happiness(session)
    preload_faction_state(session)
    preload_gov_type(session)
    preload_module_groups(session)
    preload_powers(session)
    preload_power_states(session)
    preload_security(session)
    preload_settlement_security(session)
    preload_settlement_size(session)
    preload_station_types(session)
    preload_system_aliases(session)
    session.commit()


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


def base_get_stations(session, centre_name, *, sys_dist=75, arrival=2000):
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


def get_nearest_stations_with_features(session, centre_name, *, features=None, sys_dist=75, arrival=2000, include_medium=False):
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
                        trader_type=TraderType.BROKERS_GUARDIAN, sys_dist=75, arrival=2000):
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
    ext_econ_ids = session.query(Economy.id).\
        filter(
            sqla.or_(
                Economy.text == 'Extraction',
                Economy.text == 'Refinery')).\
        scalar_subquery()
    ind_econ_id = session.query(Economy.id).\
        filter(Economy.text == 'Industrial').\
        scalar_subquery()

    centre = sqla_orm.aliased(System)
    station_system = sqla_orm.aliased(System)
    primary_economy = sqla_orm.aliased(StationEconomy)
    stations = session.query(station_system.name, station_system.dist_to(centre).label('dist_c'),
                             Station.name, Station.distance_to_star, Station.max_landing_pad_size).\
        select_from(station_system).\
        join(centre, centre.name == centre_name).\
        join(Station, Station.system_id == station_system.id).\
        join(StationType, Station.type_id == StationType.id).\
        join(StationFeatures, Station.id == StationFeatures.id).\
        join(primary_economy, Station.id == primary_economy.id).\
        filter(
            station_system.dist_to(centre) < sys_dist,
            Station.distance_to_star < arrival,
            StationType.text.notin_(exclude),
            primary_economy.primary)

    if trader_type == TraderType.BROKERS_GUARDIAN:
        stations = stations.filter(StationFeatures.technology_broker,
                                   primary_economy.economy_id == high_econ_id)
    elif trader_type == TraderType.BROKERS_HUMAN:
        stations = stations.filter(StationFeatures.technology_broker,
                                   primary_economy.economy_id != high_econ_id)
    elif trader_type == TraderType.MATS_DATA:
        stations = stations.filter(StationFeatures.material_trader,
                                   primary_economy.economy_id == high_econ_id)
    elif trader_type == TraderType.MATS_RAW:
        stations = stations.filter(StationFeatures.material_trader,
                                   primary_economy.economy_id == ind_econ_id)
    elif trader_type == TraderType.MATS_MANUFACTURED:
        stations = stations.filter(StationFeatures.material_trader,
                                   primary_economy.economy_id.in_(ext_econ_ids))

    stations = stations.order_by('dist_c', Station.distance_to_star).\
        limit(20).\
        all()

    return [[a, round(b, 2), f"[{e}] {cog.util.shorten_text(c, 16)}", d]
            for [a, b, c, d, e] in stations]


def get_shipyard_stations(session, centre_name, *, sys_dist=75, arrival=2000, include_medium=False):
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
    with cogdb.session_scope(cogdb.EDDBSession) as init_session:
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
