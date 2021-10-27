"""
All schema logic related to eddb.

Note there may be duplication between here and side.py.
The latter is purely a mapping of sidewinder's remote.
This module is for internal use.

N.B. Don't put subqueries in FROM of views for now, doesn't work on test docker.
"""
import asyncio
import concurrent.futures as cfut
import copy
import datetime
import glob
import inspect
import math
import os
import string
import sys

import argparse
# Backends tried IN ORDER on import: https://pypi.org/project/ijson/#id3
# Selected backend set in ijson.backend as string.
import ijson
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

LEN = {  # Lengths for strings stored in the db
    "allegiance": 18,
    "commodity": 34,
    "commodity_category": 20,
    "economy": 18,
    "eddn": 25,
    "faction": 90,
    "faction_happiness": 12,
    "faction_state": 23,
    "government": 18,
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
    "station": 45,
    "station_pad": 4,
    "station_type": 24,
    "system": 30,
    "weapon_mode": 6,
}
JOB_LIMIT = os.cpu_count()
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
        )
"""
# To select planetary stations
Base = sqlalchemy.ext.declarative.declarative_base()


class Allegiance(Base):
    """ Represents the allegiance of a faction. """
    __tablename__ = "allegiance"

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["allegiance"]))
    eddn = sqla.Column(sqla.String(LEN["allegiance"]))

    def __repr__(self):
        keys = ['id', 'text', 'eddn']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Allegiance) and isinstance(other, Allegiance)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


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

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Commodity) and isinstance(other, Commodity)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class CommodityCat(Base):
    """ The category for a commodity """
    __tablename__ = "commodity_categories"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN["commodity_category"]))

    def __repr__(self):
        keys = ['id', 'name']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, CommodityCat) and isinstance(other, CommodityCat)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class Economy(Base):
    """ The type of economy """
    __tablename__ = "economies"

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["economy"]))
    eddn = sqla.Column(sqla.String(LEN["economy"]))

    def __repr__(self):
        keys = ['id', 'text', 'eddn']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Economy) and isinstance(other, Economy)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class Faction(Base):
    """ Information about a faction. """
    __tablename__ = "factions"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN["faction"]), index=True)
    is_player_faction = sqla.Column(sqla.Boolean)
    home_system_id = sqla.Column(sqla.Integer)  # Makes circular foreigns.
    allegiance_id = sqla.Column(sqla.Integer, sqla.ForeignKey('allegiance.id'))
    government_id = sqla.Column(sqla.Integer, sqla.ForeignKey('gov_type.id'))
    state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_state.id'))
    updated_at = sqla.Column(sqla.Integer, onupdate=sqla.func.unix_timestamp())

    # Relationships
    allegiance = sqla.orm.relationship('Allegiance')
    government = sqla.orm.relationship('Government')
    state = sqla.orm.relationship('FactionState')

    def __repr__(self):
        keys = ['id', 'name', 'state_id', 'government_id', 'allegiance_id', 'home_system_id',
                'is_player_faction', 'updated_at']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(self, Faction) and isinstance(other, Faction) and self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def update(self, kwargs):
        """
        Update based on existing kwargs, all are optional except updated_at time.
        """
        try:
            self.updated_at = kwargs['updated_at']
            self.allegiance_id = kwargs.get('allegiance_id', self.allegiance_id)
            self.government_id = kwargs.get('government_id', self.government_id)
            self.state_id = kwargs.get('state_id', self.state_id)
        except KeyError:
            pass


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
        return (isinstance(self, FactionHappiness) and isinstance(other, FactionHappiness)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


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
        return (isinstance(self, FactionState) and isinstance(other, FactionState)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class FactionActiveState(Base):
    """ Represents the actual or pending states of a faction/system pair."""
    __tablename__ = "faction_active_states"

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), primary_key=True)
    state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_state.id'), primary_key=True)

    def __repr__(self):
        keys = ['system_id', 'faction_id', 'state_id']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, FactionActiveState)
                and isinstance(other, FactionActiveState)
                and self.__hash__() == other.__hash__())

    def __hash__(self):
        return hash("{}_{}_{}".format(self.faction_id, self.system_id, self.state_id))


class FactionPendingState(Base):
    """ Represents the actual or pending states of a faction/system pair."""
    __tablename__ = "faction_pending_states"

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), primary_key=True)
    state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_state.id'), primary_key=True)

    def __repr__(self):
        keys = ['system_id', 'faction_id', 'state_id']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, FactionPendingState)
                and isinstance(other, FactionPendingState)
                and self.__hash__() == other.__hash__())

    def __hash__(self):
        return hash("{}_{}_{}".format(self.faction_id, self.system_id, self.state_id))


class FactionRecoveringState(Base):
    """ Represents the actual or pending states of a faction/system pair."""
    __tablename__ = "faction_recovering_states"

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), primary_key=True)
    state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_state.id'), primary_key=True)

    def __repr__(self):
        keys = ['system_id', 'faction_id', 'state_id']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, FactionRecoveringState)
                and isinstance(other, FactionRecoveringState)
                and self.__hash__() == other.__hash__())

    def __hash__(self):
        return hash("{}_{}_{}".format(self.faction_id, self.system_id, self.state_id))


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
        return (isinstance(self, Government) and isinstance(other, Government)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class Influence(Base):
    """ Represents influence of a faction in a system. """
    __tablename__ = "influence"

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), nullable=False)
    faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), nullable=False)
    happiness_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_happiness.id'), nullable=True)
    influence = sqla.Column(sqla.Numeric(7, 4, None, False))
    is_controlling_faction = sqla.Column(sqla.Boolean)
    updated_at = sqla.Column(sqla.Integer, onupdate=sqla.func.unix_timestamp())

    # Relationships
    happiness = sqla.orm.relationship('FactionHappiness')
    system = sqla.orm.relationship('System', viewonly=True)
    faction = sqla.orm.relationship('Faction', viewonly=True)

    def __repr__(self):
        keys = ['system_id', 'faction_id', 'happiness_id', 'influence', 'is_controlling_faction', 'updated_at']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Influence) and isinstance(other, Influence)
                and self.system_id == other.system_id
                and self.faction_id == other.faction_id)

    def update(self, kwargs):
        """
        Update the object from kwargs.
        """
        try:
            self.updated_at = kwargs['updated_at']
            self.happiness_id = kwargs.get('happiness_id', self.happiness_id)
            self.influence = kwargs.get('influence', self.influence)
            self.is_controlling_faction = kwargs.get('is_controlling_faction', self.is_controlling_faction)
        except KeyError:
            pass


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

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


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

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


class Power(Base):
    """ Represents a powerplay leader. """
    __tablename__ = "powers"

    id = sqla.Column(sqla.Integer, primary_key=True, nullable=True, autoincrement=False)
    text = sqla.Column(sqla.String(LEN["power"]))
    eddn = sqla.Column(sqla.String(LEN["power"]))
    abbrev = sqla.Column(sqla.String(LEN["power_abv"]))
    home_system_name = sqla.Column(sqla.String(LEN["system"]))

    # Relationships
    home_system = sqla.orm.relationship(
        'System', uselist=False, lazy='select',
        primaryjoin='foreign(System.name) == Power.home_system_name',
    )

    def __repr__(self):
        keys = ['id', 'text', 'eddn', 'abbrev', 'home_system_name']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Power) and isinstance(other, Power)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class PowerState(Base):
    """
    Represents the power state of a system (i.e. control, exploited).
    """
    __tablename__ = "power_state"

    id = sqla.Column(sqla.Integer, primary_key=True, nullable=True, autoincrement=False)
    text = sqla.Column(sqla.String(LEN["power_state"]))
    eddn = sqla.Column(sqla.String(LEN["power_state"]))

    def __repr__(self):
        keys = ['id', 'text', 'eddn']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, PowerState) and isinstance(other, PowerState)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


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
        return (isinstance(self, Security) and isinstance(other, Security)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


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
        return (isinstance(self, SettlementSecurity) and isinstance(other, SettlementSecurity)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


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
        return (isinstance(self, SettlementSize) and isinstance(other, SettlementSize)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class StationFeatures(Base):
    """ The features at a station. """
    __tablename__ = "station_features"

    id = sqla.Column(sqla.Integer, sqla.ForeignKey('stations.id'), primary_key=True)
    blackmarket = sqla.Column(sqla.Boolean)
    commodities = sqla.Column(sqla.Boolean)
    dock = sqla.Column(sqla.Boolean)
    market = sqla.Column(sqla.Boolean)
    outfitting = sqla.Column(sqla.Boolean)
    rearm = sqla.Column(sqla.Boolean)
    refuel = sqla.Column(sqla.Boolean)
    repair = sqla.Column(sqla.Boolean)
    shipyard = sqla.Column(sqla.Boolean)

    # Realtionships
    station = sqla.orm.relationship('Station', uselist=False)

    def __repr__(self):
        keys = ['id', 'blackmarket', 'market', 'refuel',
                'repair', 'rearm', 'outfitting', 'shipyard',
                'dock', 'commodities']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, StationFeatures) and isinstance(other, StationFeatures)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)

    def update(self, kwargs):
        """ Update this object based on a dictionary of kwargs. """
        self.__dict__.update(kwargs)


class StationType(Base):
    __tablename__ = "station_types"

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["station_type"]))
    eddn = sqla.Column(sqla.String(LEN["station_type"]))

    def __repr__(self):
        keys = ['id', 'text', 'eddn']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, StationType) and isinstance(other, StationType)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class StationEconomy(Base):
    __tablename__ = "station_economies"

    id = sqla.Column(sqla.Integer, sqla.ForeignKey('stations.id'), primary_key=True)
    economy_id = sqla.Column(sqla.Integer, sqla.ForeignKey('economies.id'), primary_key=True)
    primary = sqla.Column(sqla.Boolean, primary_key=True, default=False)
    proportion = sqla.Column(sqla.Float)

    def __repr__(self):
        keys = ['id', 'economy_id', 'primary', 'proportion']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, StationEconomy) and isinstance(other, StationEconomy)
                and self.__hash__() == other.__hash__())

    def __hash__(self):
        return hash("{}_{}".format(self.id, self.economy_id))


class Station(Base):
    """ Repesents a system in the universe. """
    __tablename__ = "stations"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN["station"]), index=True)
    distance_to_star = sqla.Column(sqla.Integer)
    is_planetary = sqla.Column(sqla.Boolean)
    max_landing_pad_size = sqla.Column(sqla.String(LEN["station_pad"]))
    type_id = sqla.Column(sqla.Integer, sqla.ForeignKey('station_types.id'))
    system_id = sqla.Column(sqla.Integer)
    controlling_minor_faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'))
    updated_at = sqla.Column(sqla.Integer, onupdate=sqla.func.unix_timestamp())

    # Relationships
    features = sqla.orm.relationship('StationFeatures', uselist=False, viewonly=True)
    type = sqla.orm.relationship('StationType', uselist=False)
    faction = sqla.orm.relationship('Faction')
    economies = sqla.orm.relationship(
        'Economy', uselist=True, lazy='select', viewonly=True,
        primaryjoin='and_(foreign(Station.id) == remote(StationEconomy.id), foreign(StationEconomy.economy_id) == Economy.id)',
    )
    allegiance = sqla_orm.relationship(
        'Allegiance', viewonly=True, uselist=False, lazy='select',
        primaryjoin='and_(Station.controlling_minor_faction_id == remote(Faction.id), foreign(Faction.allegiance_id) == foreign(Allegiance.id))',
    )
    government = sqla_orm.relationship(
        'Government', viewonly=True, uselist=False, lazy='select',
        primaryjoin='and_(Station.controlling_minor_faction_id == remote(Faction.id), foreign(Faction.government_id) == foreign(Government.id))',
    )

    def update(self, kwargs):
        """ Update this object based on a dictionary of kwargs. """
        self.__dict__.update(kwargs)

    def __repr__(self):
        keys = ['id', 'name', 'distance_to_star', 'max_landing_pad_size',
                'type_id', 'system_id', 'controlling_minor_faction_id', 'updated_at']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(self, Station) and isinstance(other, Station) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class System(Base):
    """
    Repesents a system in the universe.

    See SystemControlV for complete control information, especially for contesteds.
    """
    __tablename__ = "systems"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN["system"]), index=True)
    population = sqla.Column(sqla.BigInteger)
    needs_permit = sqla.Column(sqla.Integer)
    edsm_id = sqla.Column(sqla.Integer)
    primary_economy_id = sqla.Column(sqla.Integer, sqla.ForeignKey('economies.id'))
    secondary_economy_id = sqla.Column(sqla.Integer, sqla.ForeignKey('economies.id'))
    power_id = sqla.Column(sqla.Integer, sqla.ForeignKey('powers.id'))
    security_id = sqla.Column(sqla.Integer, sqla.ForeignKey('security.id'))
    power_state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('power_state.id'))
    controlling_minor_faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), nullable=True)
    x = sqla.Column(sqla.Numeric(10, 5, None, False))
    y = sqla.Column(sqla.Numeric(10, 5, None, False))
    z = sqla.Column(sqla.Numeric(10, 5, None, False))
    updated_at = sqla.Column(sqla.Integer, onupdate=sqla.func.unix_timestamp())

    # Relationships
    primary_economy = sqla.orm.relationship(
        'Economy', uselist=False, lazy='select',
        primaryjoin='foreign(System.primary_economy_id) == Economy.id'
    )
    secondary_economy = sqla.orm.relationship(
        'Economy', uselist=False, lazy='select', viewonly=True,
        primaryjoin='foreign(System.primary_economy_id) == Economy.id'
    )
    power = sqla.orm.relationship('Power')
    power_state = sqla.orm.relationship('PowerState')
    security = sqla.orm.relationship('Security')
    active_states = sqla_orm.relationship(
        'FactionActiveState', viewonly=True, lazy='select',
        primaryjoin='and_(remote(FactionActiveState.system_id) == foreign(System.id), remote(FactionActiveState.faction_id) == foreign(System.controlling_minor_faction_id))',
    )
    allegiance = sqla_orm.relationship(
        'Allegiance', viewonly=True, uselist=False, lazy='select',
        primaryjoin='and_(System.controlling_minor_faction_id == remote(Faction.id), foreign(Faction.allegiance_id) == foreign(Allegiance.id))',
    )
    government = sqla_orm.relationship(
        'Government', viewonly=True, uselist=False, lazy='select',
        primaryjoin='and_(System.controlling_minor_faction_id == remote(Faction.id), foreign(Faction.government_id) == foreign(Government.id))',
    )
    controls = sqla_orm.relationship(
        'System', uselist=True, lazy='select', viewonly=True, order_by='System.name',
        primaryjoin='and_(foreign(System.id) == remote(SystemControl.system_id), foreign(SystemControl.control_id) == remote(System.id))'
    )
    exploiteds = sqla_orm.relationship(
        'System', uselist=True, lazy='select', viewonly=True, order_by='System.name',
        primaryjoin='and_(foreign(System.id) == remote(SystemControl.control_id), foreign(SystemControl.system_id) == remote(System.id))'
    )
    contesteds = sqla_orm.relationship(
        'System', uselist=True, lazy='select', viewonly=True, order_by='System.name',
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

    def update(self, kwargs):
        """ Update this object based on a dictionary of kwargs. """
        self.__dict__.update(kwargs)

    def __repr__(self):
        keys = ['id', 'name', 'population',
                'needs_permit', 'updated_at', 'power_id', 'edsm_id',
                'primary_economy_id', 'secondary_economy_id', 'security_id', 'power_state_id',
                'controlling_minor_faction_id', 'x', 'y', 'z']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(self, System) and isinstance(other, System) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class SystemContestedV(Base):
    """
    This table is a __VIEW__. See VIEW_CONTESTEDS.

    This view simply selects down only those contested systems.
    """
    __tablename__ = 'v_systems_contested'

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    system = sqla.Column(sqla.String(LEN['system']))
    control_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True, )
    control = sqla.Column(sqla.String(LEN['system']))
    power_id = sqla.Column(sqla.Integer)
    power = sqla.Column(sqla.String(LEN['power']))

    def __repr__(self):
        keys = ['system_id', 'system', 'control_id', 'control', 'power_id', 'power']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, SystemContestedV) and isinstance(other, SystemContestedV)
                and self.__hash__() == other.__hash__())

    def __hash__(self):
        return hash("{}_{}".format(self.id, self.control_id))


class SystemControlV(Base):
    """
    This table is a __VIEW__. See VIEW_SYSTEM_CONTROLS.

    This view augments SystemControl with joined text information.
    """
    __tablename__ = "v_systems_controlled"

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True, )
    system = sqla.Column(sqla.String(LEN["system"]))
    power_state_id = sqla.Column(sqla.Integer)
    power_state = sqla.Column(sqla.String(LEN['power_state']))
    control_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True, )
    control = sqla.Column(sqla.String(LEN["system"]))
    power_id = sqla.Column(sqla.Integer)
    power = sqla.Column(sqla.String(LEN['power']))

    def __str__(self):
        return f"{self.system} controlled by {self.control} ({self.power})"

    def __repr__(self):
        keys = ['system_id', 'system', 'power_state_id', 'power_state',
                'control_id', 'control', 'power_id', 'power']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(self, SystemControlV) and isinstance(other, SystemControlV) and \
            hash(self) == hash(other)

    def __hash__(self):
        return hash("{}_{}".format(self.system_id, self.control_id))


class SystemControl(Base):
    """
    This table stores all pairs of systems and their controls.
    Importantly for this consideration a control system is not paired with itself.
    Use this system mainly for joins of the IDs, for query use the augmented VIEW above.
    """
    __tablename__ = "systems_controlled"

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    power_state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('power_state.id'))
    control_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    power_id = sqla.Column(sqla.Integer, sqla.ForeignKey('powers.id'))

    def __repr__(self):
        keys = ['system_id', 'power_state_id', 'control_id', 'power_id']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(self, SystemControl) and isinstance(other, SystemControl) and \
            hash(self) == hash(other)

    def __hash__(self):
        return hash("{}_{}".format(self.system_id, self.control_id))


class ConflictState(Base):
    __tablename__ = 'conflict_states'

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["faction_state"]))
    eddn = sqla.Column(sqla.String(LEN["faction_state"]))

    def __repr__(self):
        keys = ['id', 'text', 'eddn']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, ConflictState) and isinstance(other, ConflictState)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class Conflict(Base):
    """
    Defines an in system conflict between two factions.
    """
    __tablename__ = 'conflicts'

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    status_id = sqla.Column(sqla.Integer, sqla.ForeignKey('conflict_states.id'))
    type_id = sqla.Column(sqla.Integer, sqla.ForeignKey('conflict_states.id'))
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'))
    faction1_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), primary_key=True)
    faction1_stake_id = sqla.Column(sqla.Integer, sqla.ForeignKey('stations.id'))
    faction1_days = sqla.Column(sqla.Integer)
    faction2_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), primary_key=True)
    faction2_stake_id = sqla.Column(sqla.Integer, sqla.ForeignKey('stations.id'))
    faction2_days = sqla.Column(sqla.Integer)
    updated_at = sqla.Column(sqla.Integer, onupdate=sqla.func.unix_timestamp())

    # Relationships
    system = sqla_orm.relationship('System')
    status = sqla_orm.relationship(
        'ConflictState', lazy='select',
        primaryjoin='foreign(Conflict.status_id) == ConflictState.id',
    )
    type = sqla_orm.relationship(
        'ConflictState', lazy='select',
        primaryjoin='foreign(Conflict.type_id) == ConflictState.id',
    )
    faction1 = sqla_orm.relationship(
        'Faction', lazy='select',
        primaryjoin='foreign(Conflict.faction1_id) == Faction.id',
    )
    faction2 = sqla_orm.relationship(
        'Faction', lazy='select',
        primaryjoin='foreign(Conflict.faction2_id) == Faction.id',
    )
    faction1_stake = sqla_orm.relationship(
        'Station', lazy='select',
        primaryjoin='foreign(Conflict.faction1_stake_id) == Station.id',
    )
    faction2_stake = sqla_orm.relationship(
        'Station', lazy='select',
        primaryjoin='foreign(Conflict.faction2_stake_id) == Station.id',
    )

    def __repr__(self):
        keys = ['system_id', 'status_id', 'type_id',
                'faction1_id', 'faction1_stake_id', 'faction1_days',
                'faction2_id', 'faction2_stake_id', 'faction2_days']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Conflict) and isinstance(other, Conflict)
                and self.__hash__() == other.__hash__())

    def __hash__(self):
        return hash("{}_{}_{}".format(self.system_id, self.faction1_id, self.faction2_id))

    def update(self, kwargs):
        """ Update this object based on a dictionary of kwargs. """
        self.__dict__.update(kwargs)


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
    'FactionActiveState', cascade='all, delete, delete-orphan', lazy='select',
    primaryjoin='and_(foreign(FactionActiveState.faction_id) == Influence.faction_id, foreign(FactionActiveState.system_id) == Influence.system_id)')
FactionPendingState.influence = sqla_orm.relationship(
    'Influence', uselist=False, lazy='select', viewonly=True,
    primaryjoin='and_(foreign(Influence.faction_id) == FactionPendingState.faction_id, foreign(Influence.system_id) == FactionPendingState.system_id)')
Influence.pending_states = sqla_orm.relationship(
    'FactionPendingState', cascade='all, delete, delete-orphan', lazy='select',
    primaryjoin='and_(foreign(FactionPendingState.faction_id) == Influence.faction_id, foreign(FactionPendingState.system_id) == Influence.system_id)')
FactionRecoveringState.influence = sqla_orm.relationship(
    'Influence', uselist=False, lazy='select', viewonly=True,
    primaryjoin='and_(foreign(Influence.faction_id) == FactionRecoveringState.faction_id, foreign(Influence.system_id) == FactionRecoveringState.system_id)')
Influence.recovering_states = sqla_orm.relationship(
    'FactionRecoveringState', cascade='all, delete, delete-orphan', lazy='select',
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


def preload_conflict_states(session):
    session.add_all([
        ConflictState(id=1, text="Unknown", eddn=""),
        ConflictState(id=2, text="Active", eddn="active"),
        ConflictState(id=3, text="Pending", eddn="pending"),
        ConflictState(id=4, text="Civil War", eddn="civilwar"),
        ConflictState(id=5, text="Election", eddn="election"),
        ConflictState(id=6, text="War", eddn="war"),
    ])


def preload_economies(session):
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


def preload_powers(session):
    """ All possible powers in Powerplay. """
    session.add_all([
        Power(id=0, text="None", eddn='None', abbrev="NON"),
        Power(id=1, text="Aisling Duval", eddn='Aisling Duval', abbrev="AIS", home_system_name='Cubeo'),
        Power(id=2, text="Archon Delaine", eddn='Archon Delaine', abbrev="ARC", home_system_name='Harma'),
        Power(id=3, text="Arissa Lavigny-Duval", eddn='A. Lavigny-Duval', abbrev="ALD", home_system_name='Kamadhenu'),
        Power(id=4, text="Denton Patreus", eddn='Denton Patreus', abbrev="PAT", home_system_name='Eotienses'),
        Power(id=5, text="Edmund Mahon", eddn='Edmund Mahon', abbrev="MAH", home_system_name='Gateway'),
        Power(id=6, text="Felicia Winters", eddn='Felicia Winters', abbrev="WIN", home_system_name='Rhea'),
        Power(id=7, text="Li Yong-Rui", eddn='Li Yong-Rui', abbrev="LYR", home_system_name='Lembava'),
        Power(id=8, text="Pranav Antal", eddn='Pranav Antal', abbrev="ANT", home_system_name='Polevnic'),
        Power(id=9, text="Zachary Hudson", eddn='Zachary Hudson', abbrev="HUD", home_system_name='Nanomam'),
        Power(id=10, text="Zemina Torval", eddn='Zemina Torval', abbrev="TOR", home_system_name='Synteini'),
        Power(id=11, text="Yuri Grom", eddn='Yuri Grom', abbrev="GRM", home_system_name='Clayakarma'),
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
    ])
    # Note: HomeSystem and Prepared are EDDN only states.
    #       HomeSystem is redundant as I map Power.home_system
    #       Prepared can be tracked shouldn't overlap


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


def preload_tables(session):
    """
    Preload all minor linked tables.
    """
    preload_allegiance(session)
    preload_conflict_states(session)
    preload_economies(session)
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


def load_commodities(fname):
    """ Parse standard eddb dump commodities.json and enter into database. """
    # High level mapppings direct data flow by path in json
    # Mappings should be mutually exclusive
    # Format prefix, [(target_dictionary, key_in_dict), (target_dictionary, key_in_dict), ...]
    mappings = {
        'item.id': [('commodity', 'id')],
        'item.name': [('commodity', 'name')],
        'item.average_price': [('commodity', 'average_price')],
        'item.is_rare': [('commodity', 'is_rare')],
        'item.category.id': [('commodity', 'category_id'), ('commodity_cat', 'id')],
        'item.category.name': [('commodity_cat', 'name')],
    }

    print(f"Parsing commodities in {fname}")
    categories, commodities = set(), []
    commodity, commodity_cat = {}, {}
    with open(fname, 'rb') as fin:
        for prefix, the_type, value in ijson.parse(fin):
            #  print(prefix, the_type, value)
            if (prefix, the_type, value) == ('item', 'end_map', None):
                # JSON Item terminated
                commodity_db = Commodity(**commodity)
                commodity_cat_db = CommodityCat(**commodity_cat)

                #  Debug
                #  print('Commodity', commodity_db)
                #  print('Commodity Category', commodity_cat_db)

                categories.add(commodity_cat_db)
                commodities += [commodity_db]
                commodity.clear()
                commodity_cat.clear()
                continue

            try:
                for dic, key in mappings[prefix]:
                    locals()[dic][key] = value
            except KeyError:
                pass

    print(f"FIN: Parsing commodities in {fname}")
    return categories, commodities


def load_modules(fname):
    """ Parse standard eddb dump modules.json and enter into database. """
    # High level mapppings direct data flow by path in json
    # Mappings should be mutually exclusive
    # Format prefix, [(target_dictionary, key_in_dict), (target_dictionary, key_in_dict), ...]
    mappings = {
        'item.id': [('module', 'id')],
        'item.name': [('module', 'name')],
        'item.rating': [('module', 'rating')],
        'item.price': [('module', 'price')],
        'item.ship': [('module', 'ship')],
        'item.weapon_mode': [('module', 'weapon_mode')],
        'item.class': [('module', 'size')],
        'item.mass': [('module', 'mass')],
        'item.group.id': [('module', 'group_id'), ('module_group', 'id')],
        'item.group.name': [('module_group', 'name')],
        'item.group.category': [('module_group', 'category')],
        'item.group.category_id': [('module_group', 'category_id')],
    }

    print(f"Parsing modules in {fname}")
    module_groups, modules = set(), []
    module_base = {'size': None, 'mass': None}
    module_group, module = {}, copy.deepcopy(module_base)
    with open(fname, 'rb') as fin:
        for prefix, the_type, value in ijson.parse(fin):
            #  print(prefix, the_type, value)
            if (prefix, the_type, value) == ('item', 'end_map', None):
                # JSON Item terminated
                module_group_db = ModuleGroup(**module_group)
                module_db = Module(**module)

                # Debug
                #  print('Module', module_db)
                #  print('Module Group', module_group_db)

                module_groups.add(module_group_db)
                modules += [module_db]
                module = copy.deepcopy(module_base)
                module_group.clear()
                continue

            try:
                for dic, key in mappings[prefix]:
                    locals()[dic][key] = value
            except KeyError:
                pass

    print(f"FIN: Parsing modules in {fname}")
    return module_groups, modules


def load_factions(fname, preload=True):
    """ Parse standard eddb dump modules.json and enter into database. """
    # High level mapppings direct data flow by path in json
    # Mappings should be mutually exclusive
    # Format prefix, [(target_dictionary, key_in_dict), (target_dictionary, key_in_dict), ...]
    mappings = {
        'item.id': [('faction', 'id')],
        'item.name': [('faction', 'name')],
        'item.home_system_id': [('faction', 'home_system_id')],
        'item.is_player_faction': [('faction', 'is_player_faction')],
        'item.updated_at': [('faction', 'updated_at')],
        'item.government_id': [('faction', 'government_id'), ('government', 'id')],
        'item.government': [('government', 'text')],
        'item.allegiance_id': [('faction', 'allegiance_id'), ('allegiance', 'id')],
        'item.allegiance': [('allegiance', 'text')],
    }

    print(f"Parsing factions in {fname}")
    allegiances, governments, factions = set(), set(), []
    faction, allegiance, government = {}, {}, {}
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

                allegiances.add(allegiance_db)
                governments.add(government_db)
                factions += [faction_db]

                faction.clear()
                allegiance.clear()
                government.clear()
                continue

            try:
                for dic, key in mappings[prefix]:
                    locals()[dic][key] = value
            except KeyError:
                pass

        if preload:
            allegiances = []
            governments = []

    print(f"FIN: Parsing factions in {fname}")
    return allegiances, governments, factions


def load_systems(fname, power_ids):
    """ Parse standard eddb dump populated_systems.json and enter into database. """
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
        'item.primary_economy_id': [('system', 'primary_economy_id')],
        'item.power': [('system', 'power')],
        'item.power_state_id': [('system', 'power_state_id')],
        'item.controlling_minor_faction_id': [('system', 'controlling_minor_faction_id')],
        'item.x': [('system', 'x')],
        'item.y': [('system', 'y')],
        'item.z': [('system', 'z')],
        'item.minor_faction_presences.item.influence': [('faction', 'influence')],
        'item.minor_faction_presences.item.minor_faction_id': [('faction', 'faction_id')],
        'item.minor_faction_presences.item.happiness_id': [('faction', 'happiness_id')],
    }

    print(f"Parsing systems in {fname}")
    systems, influences, states = [], [], []
    faction_base = {'active_states': [], 'pending_states': [], 'recovering_states': []}
    system, factions, faction = {}, [], copy.deepcopy(faction_base)
    with open(fname, 'rb') as fin:
        for prefix, the_type, value in ijson.parse(fin):
            #  print(prefix, the_type, value)

            if (prefix, the_type, value) == ('item', 'map_key', 'minor_faction_presences'):
                factions = []
            elif (prefix, the_type) == ('item.minor_faction_presences.item', 'start_map'):
                faction = copy.deepcopy(faction_base)
            elif (prefix, the_type) == ('item.minor_faction_presences.item', 'end_map'):
                factions += [faction]

            elif (prefix, the_type) == ('item.minor_faction_presences.item.active_states.item.id', 'number'):
                faction['active_states'] += [value]
            elif (prefix, the_type) == ('item.minor_faction_presences.item.pending_states.item.id', 'number'):
                faction['pending_states'] += [value]
            elif (prefix, the_type) == ('item.minor_faction_presences.item.recovering_states.item.id', 'number'):
                faction['recovering_states'] += [value]

            elif (prefix, the_type, value) == ('item', 'end_map', None):
                # JSON Item terminated
                system['power_id'] = power_ids[system.pop('power')]
                for faction in factions:
                    for val in faction.pop('active_states'):
                        states += [FactionActiveState(system_id=system['id'], faction_id=faction['faction_id'], state_id=val)]
                    for val in faction.pop('pending_states'):
                        states += [FactionActiveState(system_id=system['id'], faction_id=faction['faction_id'], state_id=val)]
                    for val in faction.pop('recovering_states'):
                        states += [FactionRecoveringState(system_id=system['id'], faction_id=faction['faction_id'], state_id=val)]

                    faction['is_controlling_faction'] = system['controlling_minor_faction_id'] == faction['faction_id']
                    faction['system_id'] = system['id']
                    faction['updated_at'] = system['updated_at']
                    influences += [Influence(**faction)]

                system_db = System(**system)

                # Debug
                #  print('System', system_db)

                systems += [system_db]
                system.clear()
                continue

            try:
                for dic, key in mappings[prefix]:
                    locals()[dic][key] = value
            except KeyError:
                pass

    print(f"FIN: Parsing systems in {fname}")
    return systems, influences, states


def load_stations(fname, economy_ids, preload=True):
    """ Parse standard eddb dump stations.json and enter into database. """
    # High level mapppings direct data flow by path in json
    # Mappings should be mutually exclusive
    # Format prefix, [(target_dictionary, key_in_dict), (target_dictionary, key_in_dict), ...]
    mappings = {
        'item.id': [('station', 'id'), ('st_features', 'id')],
        'item.name': [('station', 'name')],
        'item.type_id': [('station', 'type_id'), ('st_type', 'id')],
        'item.distance_to_star': [('station', 'distance_to_star')],
        'item.is_planetary': [('station', 'is_planetary')],
        'item.max_landing_pad_size': [('station', 'max_landing_pad_size')],
        'item.controlling_minor_faction_id': [('station', 'controlling_minor_faction_id')],
        'item.system_id': [('station', 'system_id')],
        'item.updated_at': [('station', 'updated_at')],
        'item.has_blackmarket': [('st_features', 'blackmarket')],
        'item.has_commodities': [('st_features', 'commodities')],
        'item.has_docking': [('st_features', 'dock')],
        'item.has_market': [('st_features', 'market')],
        'item.has_outfitting': [('st_features', 'outfitting')],
        'item.has_refuel': [('st_features', 'refuel')],
        'item.has_repair': [('st_features', 'repair')],
        'item.has_rearm': [('st_features', 'rearm')],
        'item.has_shipyard': [('st_features', 'shipyard')],
        'item.type': [('st_type', 'text')],
    }

    print(f"Parsing stations in {fname}")
    station_features, station_types, stations, economies = [], set(), [], []
    station, st_features, st_type, st_econs = {}, {}, {}, []
    with open(fname, 'rb') as fin:
        for prefix, the_type, value in ijson.parse(fin):
            #  print(prefix, the_type, value)
            if (prefix, the_type) == ('item.economies.item', 'string'):
                st_econs += [value]

            if (prefix, the_type, value) == ('item', 'end_map', None):
                # JSON Item terminated
                station_db = Station(**station)
                st_features_db = StationFeatures(**st_features)
                st_type_db = StationType(**st_type)
                primary = True
                for econ in st_econs:
                    economies += [StationEconomy(id=station['id'], economy_id=economy_ids[econ], primary=primary)]
                    primary = False

                # Debug
                #  print('Station', station_db)
                #  print('Station Features', st_features_db)
                #  print('Station Type', st_type_db)
                station_features += [st_features_db]
                station_types.add(st_type_db)
                stations += [station_db]

                station.clear()
                st_features.clear()
                st_type.clear()
                st_econs.clear()
                continue

            try:
                for dic, key in mappings[prefix]:
                    locals()[dic][key] = value
            except KeyError:
                pass

    if preload:
        station_types = []

    print(f"FIN: Parsing stations in {fname}")
    return station_types, stations, economies, station_features


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


def get_systems_around(session, centre_name, distance):
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


def get_shipyard_stations(session, centre_name, *, sys_dist=75, arrival=2000, include_medium=False):
    """
    Given a reference centre system, find nearby orbitals within:
        < sys_dist ly from original system
        < arrival ls from the entry start

    Returns:
        List of matches:
            [system_name, system_dist, station_name, station_arrival_distance]
    """
    exclude = session.query(StationType.text).\
        filter(or_(StationType.text.like("%Planet%"),
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
        filter(station_system.dist_to(centre) < sys_dist,
               Station.distance_to_star < arrival,
               StationType.text.notin_(exclude))

    if include_medium:
        stations = stations.filter(StationFeatures.repair, StationFeatures.rearm,
                                   StationFeatures.refuel, StationFeatures.outfitting)
    else:
        stations = stations.filter(StationFeatures.shipyard)

    stations = stations.order_by('dist_c', Station.distance_to_star).\
        limit(20).\
        all()

    # Slight cleanup for presentation in table
    return [[a, round(b, 2), "[{}] {}".format(e, cog.util.shorten_text(c, 16)), d]
            for [a, b, c, d, e] in stations]


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
    results = session.query(SystemControlV.control_id, System).\
        distinct(SystemControlV.control_id).\
        filter(SystemControlV.power.ilike(power)).\
        join(System, System.id == SystemControlV.control_id).\
        order_by(System.dist_to(centre))

    if limit > 0:
        results = results.limit(limit)

    return [x[1] for x in results.all()]


def get_controls_of_power(session, *, power='%hudson'):
    """
    Find the names of all controls of a given power.

    Args:
        session: The EDDBSession variable.

    Kwargs:
        power: The loose like match of the power, i.e. "%hudson".
    """
    results = session.query(SystemControlV.control_id, System.name).\
        distinct(SystemControlV.control_id).\
        filter(SystemControlV.power.ilike(power)).\
        join(System, System.id == SystemControlV.control_id).\
        order_by(System.name).\
        all()

    return [x[1] for x in results]


def get_systems_of_power(session, *, power='%hudson'):
    """
    Find the names of all exploited and contested systems of a power.

    Args:
        session: The EDDBSession variable.

    Kwargs:
        power: The loose like match of the power, i.e. "%hudson".
    """
    results = session.query(SystemControlV.system_id, System.name).\
        distinct(SystemControlV.system_id).\
        filter(SystemControlV.power.ilike(power)).\
        join(System, System.id == SystemControlV.system_id).\
        order_by(System.name).\
        all()

    return [x[1] for x in results] + get_controls_of_power(session, power=power)


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
            )
        ).\
        all()


def get_nearest_ifactors(session, *, centre_name, sys_dist=75, arrival=2000, include_medium=False):
    """
    Given a reference centre system, find nearby orbitals within:
        < sys_dist ly from original system
        < arrival ls from the entry start

    Returns:
        List of matches:
            [system_name, system_dist, station_name, station_arrival_distance]
    """
    exclude = session.query(StationType.text).\
        filter(or_(StationType.text.like("%Planet%"),
                   StationType.text.like("%Fleet%"))).\
        scalar_subquery()
    sub_security = session.query(Security.id).filter(Security.text == "Low").scalar_subquery()

    centre = sqla_orm.aliased(System)
    station_system = sqla_orm.aliased(System)
    stations = session.query(station_system.name, station_system.dist_to(centre).label('dist_c'),
                             Station.name, Station.distance_to_star, Station.max_landing_pad_size).\
        select_from(station_system).\
        join(centre, centre.name == centre_name).\
        join(Station, Station.system_id == station_system.id).\
        join(StationType, Station.type_id == StationType.id).\
        join(StationFeatures, Station.id == StationFeatures.id).\
        filter(station_system.dist_to(centre) < sys_dist,
               station_system.security_id == sub_security,
               Station.distance_to_star < arrival,
               StationType.text.notin_(exclude))

    if not include_medium:
        stations = stations.filter(Station.max_landing_pad_size == 'L')

    stations = stations.order_by('dist_c', Station.distance_to_star).\
        limit(20).\
        all()

    return [[a, round(b, 2), "[{}] {}".format(e, cog.util.shorten_text(c, 16)), d]
            for [a, b, c, d, e] in stations]


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
        raise cog.exc.InvalidCommandArgs("The start system %s was not found." % system_names[0]) from exc
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


def dump_db(session, classes, fname):
    """
    Dump db to a file.
    """
    with open(fname, "w") as fout:
        for cls in classes:
            for obj in session.query(cls):
                fout.write(repr(obj) + '\n')


def check_eddb_base_subclass(obj):
    """ Simple predicate, select sublasses of Base. """
    return inspect.isclass(obj) and obj.__name__ not in ["Base", "hybrid_method", "hybrid_property"]


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
    ]
    try:
        with cogdb.eddb_engine.connect() as con:
            for drop_cmd in drop_cmds:
                con.execute(sqla.sql.text(drop_cmd))
    except (sqla.exc.OperationalError, sqla.exc.ProgrammingError):
        pass

    meta = sqlalchemy.MetaData(bind=cogdb.eddb_engine)
    meta.reflect()
    for tbl in reversed(meta.sorted_tables):
        try:
            tbl.drop()
        except sqla.exc.OperationalError:
            pass

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
    ]
    with cogdb.eddb_engine.connect() as con:
        for create_cmd in create_cmds:
            con.execute(sqla.sql.text(create_cmd))


def make_parser():
    parser = argparse.ArgumentParser(description="EDDB Importer")
    parser.add_argument('--preload', '-p', default=True, action="store_true",
                        help='Preload required database entries.')
    parser.add_argument('--no-preload', '-n', dest='preload', action="store_false",
                        help='Skip preloading required database entries.')
    parser.add_argument('--dump', '-d', action="store_true",
                        help='Dump existing database to /tmp/eddb_dump')
    parser.add_argument('--yes', '-y', action="store_true",
                        help='Skip confirmation.')
    parser.add_argument('--jobs', '-j', type=int,
                        help='The max number of jobs to run.')

    return parser


def chunk_jobs(initial_jobs, *, limit=5000):  # pragma: no cover
    """
    Take a list of jobs being prepared and take each fname inside the list
    and chunk that file out in the existing directory with limit lines per file.

    Args:
        initial_jobs: A list that contains a description of functions to be mapped onto args. It is of
        the form:
            [[func, fname, arg1, arg2],
             [func2, fname2, arg3,],
             ]

    Kwargs:
        limit: The limit of lines to chunk into each file.
    """
    jobs = {}
    for tup in initial_jobs:
        fname = tup[1]
        cog.util.chunk_file(fname, limit=limit)
        for globbed in sorted(glob.glob(fname + '_*')):
            jobs[globbed] = [tup[0], globbed] + tup[2:]

    return jobs


def pool_loader(preload=True):  # pragma: no cover
    """
    Use a ProcessPoolExecutor to run jobs that parse parts of the json files
    and return objects to be inserted into the db.

    Assuming extras.fetch_eddb has been run with 'sort' option, then:
        - Chunk the large json files to create units of work.
        - Submit jobs to ProcessPoolExecutor to be completed.
        - Jobs return db objects to be inserted into the database.
        - Multiple rounds allow different parts to be processed once all parts in last
        round completed.

    Args:
        preload: The preload has already been done.
    """
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        # Map eceonomies back onto ids
        economy_ids = {x.text: x.id for x in eddb_session.query(Economy).all()}
        economy_ids[None] = economy_ids['None']
        # Map power names onto their ids
        power_ids = {x.text: x.id for x in eddb_session.query(Power).all()}
        power_ids[None] = power_ids["None"]

    # Top level map of functions, to the files that take them.
    # Things in later rounds have foreign keys to previous ones.
    rounds = {
        1: [
            [load_commodities, cog.util.rel_to_abs("data", "eddb", "commodities.json_per_line")],
            [load_modules, cog.util.rel_to_abs("data", "eddb", "modules.json_per_line")],
            [load_factions, cog.util.rel_to_abs("data", "eddb", "factions.json_per_line"), preload],
        ],
        2: [
            [load_systems, cog.util.rel_to_abs("data", "eddb", "systems_populated.json_per_line"), power_ids],
        ],
        3: [
            [load_stations, cog.util.rel_to_abs("data", "eddb", "stations.json_per_line"), economy_ids, preload],
        ]
    }

    try:
        with cfut.ProcessPoolExecutor(max_workers=JOB_LIMIT) as pool:
            for key, limit in [(1, 4000), (2, 1500), (3, 2500)]:
                jobs = chunk_jobs(rounds.get(key), limit=limit)

                futures = {pool.submit(*job) for job in jobs.values()}
                for fut in cfut.as_completed(futures, 600):
                    for objs in fut.result():
                        if objs:
                            # TODO: Potential for further speedup, move this into load functions.
                            # Current problem: Lock contention on shared table during updates holding conns too long I think.
                            with cogdb.session_scope(cogdb.EDDBSession) as session:
                                try:
                                    session.add_all(objs)
                                    session.commit()
                                except sqla.exc.IntegrityError:
                                    # Just in case, shouldn't trigger
                                    session.rollback()
    finally:
        match = cog.util.rel_to_abs('data', 'eddb', '*_line_0*')
        for fname in glob.glob(match):
            try:
                os.remove(fname)
            except OSError:
                print(f"Could not remove: {fname}")


def import_eddb(eddb_session):  # pragma: no cover
    """ Allows the seeding of db from eddb dumps. """
    args = make_parser().parse_args()
    if args.jobs:
        global JOB_LIMIT
        JOB_LIMIT = args.jobs

    if not args.yes:
        confirm = input("Reimport EDDB Database? (y/n) ").strip().lower()
        if not confirm.startswith('y'):
            print("Aborting.")
            return

    if args.dump:
        fname = '/tmp/eddb_dump'
        print("Dumping to: " + fname)
        classes = [x[1] for x in inspect.getmembers(sys.modules[__name__], check_eddb_base_subclass)]
        dump_db(eddb_session, classes, fname)
        sys.exit(0)

    recreate_tables()
    print('EDDB tables recreated.')
    if args.preload:
        preload_tables(eddb_session)
        print('EDDB tables preloaded.')

    pool_loader(args.preload)


async def monitor_eddb_caches(*, delay_hours=4):  # pragma: no cover
    """
    Monitor and recompute cached tables:
        - Repopulates SystemControls from latest data.

    Kwargs:
        delay_hours: The hours between refreshing cached tables. Default: 2
    """
    await asyncio.sleep(delay_hours * 3600)

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        await asyncio.get_event_loop().run_in_executor(
            None, populate_system_controls, eddb_session
        )

    asyncio.ensure_future(
        monitor_eddb_caches(
            delay_hours=delay_hours
        )
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
        filter(Station.system_id == system.id,
               Station.name == "Daedalus").\
        one()
    print(station.system, station.type, station.features, station.faction)

    __import__('pprint').pprint(get_nearest_ifactors(eddb_session, centre_name='rana'))
    stations = get_shipyard_stations(eddb_session, input("Please enter a system name ... "))
    if stations:
        print(cog.tbl.format_table(stations))

    print('31 Aquilae')
    sys = sqla_orm.aliased(System)
    sys_control = sqla_orm.aliased(System)
    systems = eddb_session.query(sys.name, sys_control.name).\
        filter(sys.name == '31 Aquilae').\
        join(SystemControl, sys.id == SystemControl.system_id).\
        join(sys_control, sys_control.id == SystemControl.control_id).\
        all()
    __import__('pprint').pprint(systems)
    print('Togher')
    system = eddb_session.query(sys).\
        filter(sys.name == 'Togher').\
        one()
    __import__('pprint').pprint(system.controls)
    print('Wat Yu')
    system = eddb_session.query(sys).\
        filter(sys.name == 'Wat Yu').\
        one()
    __import__('pprint').pprint(system.exploiteds)
    __import__('pprint').pprint(system.contesteds)


def main():  # pragma: no cover
    """ Main entry. """
    start = datetime.datetime.utcnow()

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        import_eddb(eddb_session)
        #  Manually compute post import the initial SystemControl table
        populate_system_controls(eddb_session)

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        print("Module count:", eddb_session.query(Module).count())
        print("Commodity count:", eddb_session.query(Commodity).count())
        obj_count = eddb_session.query(Faction).count()
        assert obj_count > 77500
        print("Faction count:", obj_count)
        print("Faction States count:", eddb_session.query(FactionActiveState).count() + eddb_session.query(FactionPendingState).count() + eddb_session.query(FactionRecoveringState).count())
        print("Influence count:", eddb_session.query(Influence).count())
        obj_count = eddb_session.query(System).count()
        assert obj_count > 20500
        print("Populated System count:", obj_count)
        obj_count = eddb_session.query(Station).count()
        assert obj_count > 135000
        print("Station count:", obj_count)
        print("Contested count:", eddb_session.query(SystemContestedV).count())
        print("Time taken:", datetime.datetime.utcnow() - start)
        #  main_test_area(eddb_session)


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


if __name__ == "__main__":  # pragma: no cover
    # Tell user when not using most efficient backend.
    if ijson.backend != 'yajl2_c':
        print("Failed to set backend to yajl2_c. Please check that yajl is installed. Parsing may slow down.")
        print(f"Selected: {ijson.backend}")

    main()
