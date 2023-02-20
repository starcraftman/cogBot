"""
The database backend for pvp bot.
"""
import contextlib
import datetime
import enum
import functools
import time
import random
import math
import shutil
import tempfile

import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm
import sqlalchemy.orm.session
import sqlalchemy.ext.declarative
from sqlalchemy.schema import UniqueConstraint

import cogdb.eddb
from cogdb.eddb import LEN as EDDB_LEN
from cogdb.spy_squirrel import Base
import cog.util
from cog.util import ReprMixin, TimestampMixin, UpdatableMixin

PVP_DEFAULT_HEX = 0x0dd42e
MATCH_STATES = {
    0: "Setup",
    1: "Started",
    2: "Finished",
}


class EventTimeMixin():
    """
    Simple mixing that converts updated_at timestamp to a datetime object.
    Timestamps on object are assumed to be created as UTC timestamps.
    """
    @property
    def event_date(self):
        """The event at date as a naive datetime object."""
        return datetime.datetime.utcfromtimestamp(self.event_at)

    @property
    def event_date_tz(self):
        """The created at date as a timezone aware datetime object."""
        return datetime.datetime.fromtimestamp(self.event_at, tz=datetime.timezone.utc)


class PVPCmdr(ReprMixin, TimestampMixin, Base):
    """
    Table to store PVP users and link against.
    Represent a single commander who reports kills to the bot.
    """
    __tablename__ = 'pvp_cmdrs'
    _repr_keys = ['id', 'name', 'updated_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)  # Discord id
    name = sqla.Column(sqla.String(EDDB_LEN['pvp_name']))
    hex = sqla.Column(sqla.String(6))  # Hex strings, no leading 0x: B20000
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    @property
    def hex_value(self):
        """ Simple conversion of hex to integer value. """
        return int(self.hex, 16)

    def __str__(self):
        """ Convenient string representation of this object. """
        return f'CMDR {self.name} ({self.id})'

    def __eq__(self, other):
        return isinstance(other, PVPCmdr) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


@functools.total_ordering
class PVPLocation(ReprMixin, TimestampMixin, EventTimeMixin, Base):
    """
    Table to store location of a given player.
    """
    __tablename__ = 'pvp_locations'
    __table_args__ = (
        UniqueConstraint('cmdr_id', 'system_id', 'event_at', name='_pvp_location_unique'),
    )
    _repr_keys = ['id', 'cmdr_id', 'system_id', 'created_at', 'event_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cmdr_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_cmdrs.id'))
    system_id = sqla.Column(sqla.Integer)

    created_at = sqla.Column(sqla.Integer, default=time.time)
    event_at = sqla.Column(sqla.Integer, default=time.time)  # Set to time in log

    cmdr = sqla.orm.relationship('PVPCmdr', viewonly=True)
    system = sqla.orm.relationship('System', uselist=False, lazy='joined', viewonly=True,
                                   primaryjoin='foreign(PVPLocation.system_id) == System.id')

    def embed(self):
        """ Short string representation for embed. """
        try:
            system = self.system.name
        except AttributeError:
            system = f"system_id[{self.system_id}]"

        return f'{system} ({self.event_date})'

    def __str__(self):
        """ Show PVPLocation information. """
        try:
            cmdr = self.cmdr.name
        except AttributeError:
            cmdr = self.id
        try:
            system = self.system.name
        except AttributeError:
            system = f"system_id {self.system_id}"

        return f'CMDR {cmdr} located in {system} at {self.event_date}.'

    def __eq__(self, other):
        return isinstance(other, PVPLocation) and hash(self) == hash(other)

    def __lt__(self, other):
        return self.event_at < other.event_at

    def __hash__(self):
        return hash(f'{self.cmdr_id}_{self.system_id}_{self.event_at}')


@functools.total_ordering
class PVPKill(ReprMixin, TimestampMixin, EventTimeMixin, Base):
    """
    Table to store PVP kills reported by a user.
    """
    __tablename__ = 'pvp_kills'
    __table_args__ = (
        UniqueConstraint('cmdr_id', 'system_id', 'event_at', name='_pvp_kill_unique'),
    )
    _repr_keys = ['id', 'cmdr_id', 'system_id', 'victim_name', 'victim_rank', 'created_at', 'event_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cmdr_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_cmdrs.id'))
    system_id = sqla.Column(sqla.Integer)

    victim_name = sqla.Column(sqla.String(EDDB_LEN["pvp_name"]), index=True)
    victim_rank = sqla.Column(sqla.Integer, default=0)
    created_at = sqla.Column(sqla.Integer, default=time.time)
    event_at = sqla.Column(sqla.Integer, default=time.time)  # Set to time in log

    def embed(self):
        """ Short string representation for embed. """
        return f'CMDR {self.victim_name} ({self.event_date})'

    def __str__(self):
        """ Show PVPKill information. """
        try:
            cmdr = self.cmdr.name
        except AttributeError:
            cmdr = self.id

        return f'CMDR {cmdr} killed CMDR {self.victim_name} at {self.event_date}'

    def __eq__(self, other):
        return isinstance(other, PVPKill) and hash(self) == hash(other)

    def __lt__(self, other):
        return self.event_at < other.event_at

    def __hash__(self):
        return hash(f'{self.cmdr_id}_{self.system_id}_{self.event_at}')


@functools.total_ordering
class PVPDeath(ReprMixin, TimestampMixin, EventTimeMixin, Base):
    """
    Table to store any deaths of a cmdr.
    """
    __tablename__ = 'pvp_deaths'
    __table_args__ = (
        UniqueConstraint('cmdr_id', 'system_id', 'event_at', name='_pvp_death_unique'),
    )
    _repr_keys = ['id', 'cmdr_id', 'system_id', 'is_wing_kill', 'created_at', 'event_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cmdr_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_cmdrs.id'))
    system_id = sqla.Column(sqla.Integer)

    is_wing_kill = sqla.Column(sqla.Boolean, default=False)
    created_at = sqla.Column(sqla.Integer, default=time.time)
    event_at = sqla.Column(sqla.Integer, default=time.time)  # Set to time in log

    def killed_by(self, cmdr_name):
        """
        Return True IFF the cmdr_name was part of this CMDR death.
        """
        return cmdr_name in [x.name for x in self.killers]

    def embed(self):
        """ Short string representation for embed. """
        killers = "<unknown>"
        if self.killers:
            killers = ", ".join([str(x) for x in self.killers])
            if self.is_wing_kill:
                killers = f'[{killers}]'

        return f"{killers} ({self.event_date})"

    def __str__(self):
        """ Show the death and killers. """
        try:
            cmdr = self.cmdr.name
        except AttributeError:
            cmdr = self.id

        killers = "<unknown>"
        if self.killers:
            killers = ", ".join([str(x) for x in self.killers])
            if self.is_wing_kill:
                killers = f'[{killers}]'

        return f"CMDR {cmdr} was killed by: {killers} at {self.event_date}"

    def __eq__(self, other):
        return isinstance(other, PVPDeath) and hash(self) == hash(other)

    def __lt__(self, other):
        return self.event_at < other.event_at

    def __hash__(self):
        return hash(f'{self.cmdr_id}_{self.system_id}_{self.event_at}')


class PVPDeathKiller(ReprMixin, TimestampMixin, EventTimeMixin, Base):
    """
    Table to store events where cmdr interdicted other players to initiate combat.
    """
    __tablename__ = 'pvp_deaths_killers'
    _repr_keys = ['pvp_death_id', 'name', 'rank', 'ship_id', 'created_at', 'event_at']

    cmdr_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_cmdrs.id'))
    pvp_death_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_deaths.id'), primary_key=True)
    ship_id = sqla.Column(sqla.Integer)  # Links against 'spy_ships', key not needed

    name = sqla.Column(sqla.String(EDDB_LEN["pvp_name"]), index=True, primary_key=True)
    rank = sqla.Column(sqla.Integer, default=0)
    created_at = sqla.Column(sqla.Integer, default=time.time)
    event_at = sqla.Column(sqla.Integer, default=time.time)  # Set to time in log

    ship = sqla.orm.relationship('SpyShip', uselist=False, lazy='joined', viewonly=True,
                                 primaryjoin='foreign(PVPDeathKiller.ship_id) == SpyShip.id')

    def __str__(self):
        """ Show a single PVP killer of a cmdr. """
        return f"CMDR {self.name} ({self.ship_name})"

    @property
    def ship_name(self):
        """ Return the actual ship name and not the id. If impossible return placeholder. """
        try:
            return self.ship.text
        except AttributeError:
            return "<unknown ship>"

    def __eq__(self, other):
        return isinstance(other, PVPDeathKiller) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f'{self.pvp_death_id}_{self.name}')


@functools.total_ordering
class PVPInterdiction(ReprMixin, TimestampMixin, EventTimeMixin, Base):
    """
    Table to store events where cmdr interdicted other players to initiate combat.
    """
    __tablename__ = 'pvp_interdictions'
    __table_args__ = (
        UniqueConstraint('cmdr_id', 'system_id', 'event_at', name='_pvp_interdiction_unique'),
    )
    _repr_keys = ['id', 'cmdr_id', 'system_id', 'is_player', 'is_success', 'survived',
                  'victim_name', 'victim_rank', 'created_at', 'event_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cmdr_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_cmdrs.id'))
    system_id = sqla.Column(sqla.Integer)

    is_player = sqla.Column(sqla.Boolean, default=False)  # True if interdictor is player
    is_success = sqla.Column(sqla.Boolean, default=False)  # True if forced player out of super cruise
    survived = sqla.Column(sqla.Boolean, default=True)  # True if the victim escaped

    victim_name = sqla.Column(sqla.String(EDDB_LEN["pvp_name"]), index=True)
    victim_rank = sqla.Column(sqla.Integer, default=0)
    created_at = sqla.Column(sqla.Integer, default=time.time)
    event_at = sqla.Column(sqla.Integer, default=time.time)  # Set to time in log

    def embed(self):
        """ Short string representation for embed. """
        return f'CMDR {self.victim_name} ({self.event_date})'

    def __str__(self):
        """ Show a single interdiction by the cmdr. """
        try:
            cmdr = self.cmdr.name
        except AttributeError:
            cmdr = self.id

        return f"CMDR {cmdr} interdicted {'CMDR ' if self.is_player else ''}{self.victim_name} at {self.event_date}. Pulled from SC: {self.is_success} Escaped: {self.survived}"

    def __eq__(self, other):
        return isinstance(other, PVPInterdiction) and hash(self) == hash(other)

    def __lt__(self, other):
        return self.event_at < other.event_at

    def __hash__(self):
        return hash(f'{self.cmdr_id}_{self.system_id}_{self.event_at}')


class PVPInterdictionKill(ReprMixin, TimestampMixin, EventTimeMixin, Base):
    """
    Table to store the event where an interdiction lead to a pvp kill.
    """
    __tablename__ = 'pvp_interdictions_kills'
    __table_args__ = (
        UniqueConstraint('pvp_interdiction_id', 'pvp_kill_id', name='_pvp_interdiction_kill_unique'),
    )
    _repr_keys = ['id', 'cmdr_id', 'pvp_interdiction_id', 'pvp_kill_id', 'created_at', 'event_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cmdr_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_cmdrs.id'))
    pvp_interdiction_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_interdictions.id'))
    pvp_kill_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_kills.id'))
    created_at = sqla.Column(sqla.Integer, default=time.time)
    event_at = sqla.Column(sqla.Integer, default=time.time)  # Set to time in log

    cmdr = sqla.orm.relationship('PVPCmdr', viewonly=True)
    pvp_interdiction = sqla.orm.relationship('PVPInterdiction', viewonly=True)
    pvp_kill = sqla.orm.relationship('PVPKill', viewonly=True)

    def __eq__(self, other):
        return isinstance(other, PVPInterdictionKill) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class PVPInterdictionDeath(ReprMixin, TimestampMixin, EventTimeMixin, Base):
    """
    Table to store the event where an interdiction lead to a cmdr death.
    """
    __tablename__ = 'pvp_interdictions_deaths'
    __table_args__ = (
        UniqueConstraint('pvp_interdiction_id', 'pvp_death_id', name='_pvp_interdiction_death_unique'),
    )
    _repr_keys = ['id', 'cmdr_id', 'pvp_interdiction_id', 'pvp_death_id', 'created_at', 'event_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cmdr_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_cmdrs.id'))
    pvp_interdiction_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_interdictions.id'))
    pvp_death_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_deaths.id'))
    created_at = sqla.Column(sqla.Integer, default=time.time)
    event_at = sqla.Column(sqla.Integer, default=time.time)  # Set to time in log

    cmdr = sqla.orm.relationship('PVPCmdr', viewonly=True)
    pvp_interdiction = sqla.orm.relationship('PVPInterdiction', viewonly=True)
    pvp_death = sqla.orm.relationship('PVPDeath', viewonly=True)

    def __eq__(self, other):
        return isinstance(other, PVPInterdictionDeath) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


@functools.total_ordering
class PVPInterdicted(ReprMixin, TimestampMixin, EventTimeMixin, Base):
    """
    Table to store events when a cmdr was interdicted by other players.
    """
    __tablename__ = 'pvp_interdicteds'
    __table_args__ = (
        UniqueConstraint('cmdr_id', 'system_id', 'event_at', name='_pvp_interdicted_unique'),
    )
    _repr_keys = ['id', 'cmdr_id', 'system_id', 'is_player', 'did_submit', 'survived',
                  'interdictor_name', 'interdictor_rank', 'created_at', 'event_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cmdr_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_cmdrs.id'))
    system_id = sqla.Column(sqla.Integer)

    is_player = sqla.Column(sqla.Boolean, default=False)  # True if interdictor is player
    did_submit = sqla.Column(sqla.Boolean, default=False)  # True if the fictim submitted
    survived = sqla.Column(sqla.Boolean, default=True)  # True if the cmdr managed to escape

    interdictor_name = sqla.Column(sqla.String(EDDB_LEN["pvp_name"]), index=True)
    interdictor_rank = sqla.Column(sqla.Integer, default=0)
    created_at = sqla.Column(sqla.Integer, default=time.time)
    event_at = sqla.Column(sqla.Integer, default=time.time)  # Set to time in log

    def embed(self):
        """ Short string representation for embed. """
        return f'CMDR {self.interdictor_name} ({self.event_date})'

    def __str__(self):
        """ Show a single time cmdr was interdicted by another. """
        try:
            cmdr = self.cmdr.name
        except AttributeError:
            cmdr = self.id

        return f"CMDR {cmdr} was interdicted by {'CMDR ' if self.is_player else ''}{self.interdictor_name} at {self.event_date}. Submitted: {self.did_submit}. Escaped: {self.survived}"

    def __eq__(self, other):
        return isinstance(other, PVPInterdicted) and hash(self) == hash(other)

    def __lt__(self, other):
        return self.event_at < other.event_at

    def __hash__(self):
        return hash(f'{self.cmdr_id}_{self.system_id}_{self.event_at}')


class PVPInterdictedKill(ReprMixin, TimestampMixin, EventTimeMixin, Base):
    """
    Table to store the event where a cmdr was interdicted and pvp killed.
    """
    __tablename__ = 'pvp_interdicteds_kills'
    __table_args__ = (
        UniqueConstraint('pvp_interdicted_id', 'pvp_kill_id', name='_pvp_interdicted_kill_unique'),
    )
    _repr_keys = ['id', 'cmdr_id', 'pvp_interdicted_id', 'pvp_kill_id', 'created_at', 'event_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cmdr_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_cmdrs.id'))
    pvp_interdicted_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_interdicteds.id'))
    pvp_kill_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_kills.id'))
    created_at = sqla.Column(sqla.Integer, default=time.time)
    event_at = sqla.Column(sqla.Integer, default=time.time)  # Set to time in log

    cmdr = sqla.orm.relationship('PVPCmdr', viewonly=True)
    pvp_interdiction = sqla.orm.relationship('PVPInterdicted', viewonly=True)
    pvp_kill = sqla.orm.relationship('PVPKill', viewonly=True)

    def __eq__(self, other):
        return isinstance(other, PVPInterdictedKill) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class PVPInterdictedDeath(ReprMixin, TimestampMixin, EventTimeMixin, Base):
    """
    Table to store the event where a cmdr was interdicted and died.
    """
    __tablename__ = 'pvp_interdicteds_deaths'
    __table_args__ = (
        UniqueConstraint('pvp_interdicted_id', 'pvp_death_id', name='_pvp_interdicted_death_unique'),
    )
    _repr_keys = ['id', 'cmdr_id', 'pvp_interdicted_id', 'pvp_death_id', 'created_at', 'event_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cmdr_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_cmdrs.id'))
    pvp_interdicted_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_interdicteds.id'))
    pvp_death_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_deaths.id'))
    created_at = sqla.Column(sqla.Integer, default=time.time)
    event_at = sqla.Column(sqla.Integer, default=time.time)  # Set to time in log

    cmdr = sqla.orm.relationship('PVPCmdr', viewonly=True)
    pvp_interdicted = sqla.orm.relationship('PVPInterdicted', viewonly=True)
    pvp_death = sqla.orm.relationship('PVPDeath', viewonly=True)

    def __eq__(self, other):
        return isinstance(other, PVPInterdictedDeath) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


@functools.total_ordering
class PVPEscapedInterdicted(ReprMixin, TimestampMixin, EventTimeMixin, Base):
    """
    Table to store events when a cmdr was interdicted by other players.
    """
    __tablename__ = 'pvp_escaped_interdicteds'
    __table_args__ = (
        UniqueConstraint('cmdr_id', 'system_id', 'event_at', name='_pvp_interdicted_escape_unique'),
    )
    _repr_keys = ['id', 'cmdr_id', 'system_id', 'is_player', 'interdictor_name',
                  'created_at', 'event_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cmdr_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_cmdrs.id'))
    system_id = sqla.Column(sqla.Integer)

    is_player = sqla.Column(sqla.Boolean, default=False)  # True if interdictor is player
    interdictor_name = sqla.Column(sqla.String(EDDB_LEN["pvp_name"]), index=True)
    created_at = sqla.Column(sqla.Integer, default=time.time)
    event_at = sqla.Column(sqla.Integer, default=time.time)  # Set to time in log

    cmdr = sqla.orm.relationship('PVPCmdr', viewonly=True)

    def embed(self):
        """ Short string representation for embed. """
        return f'CMDR {self.interdictor_name} ({self.event_date})'

    def __str__(self):
        """ Show a single time cmdr escaped interdiction. """
        try:
            cmdr = self.cmdr.name
        except AttributeError:
            cmdr = self.id

        return f"CMDR {cmdr} escaped interdiction by {'CMDR ' if self.is_player else ''}{self.interdictor_name} at {self.event_date}"

    def __eq__(self, other):
        return isinstance(other, PVPEscapedInterdicted) and hash(self) == hash(other)

    def __lt__(self, other):
        return self.event_at < other.event_at

    def __hash__(self):
        return hash(f'{self.cmdr_id}_{self.system_id}_{self.event_at}')


class PVPStat(ReprMixin, TimestampMixin, UpdatableMixin, Base):
    """
    Table to store derived stats from pvp tracking.
    All stats are relative the single CMDR identified by cmdr_id.
    These stats are cached and fixed until a new log uploaded, then regenerate.
    """
    __tablename__ = 'pvp_stats'
    _repr_keys = [
        'id', 'cmdr_id',
        'last_location_id', 'last_kill_id', 'last_death_id',
        'last_interdicted_id', 'last_interdicted_id', 'last_escaped_interdicted_id',
        'deaths', 'kills', 'interdictions', 'interdicteds', 'escaped_interdicteds',
        'most_kills_system_id', 'most_deaths_system_id',
        'interdicted_kills', 'interdiction_deaths', 'interdicted_kills', 'interdicted_deaths',
        'killed_most', 'most_deaths_by', 'most_interdictions', 'most_interdicted_by', 'most_escaped_interdictions_from'
        'updated_at'
    ]

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cmdr_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_cmdrs.id'))
    last_location_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_locations.id'))
    last_kill_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_kills.id'))
    last_death_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_deaths.id'))
    last_interdiction_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_interdictions.id'))
    last_interdicted_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_interdicteds.id'))
    last_escaped_interdicted_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_escaped_interdicteds.id'))

    deaths = sqla.Column(sqla.Integer, default=0)
    kills = sqla.Column(sqla.Integer, default=0)
    interdictions = sqla.Column(sqla.Integer, default=0)
    interdicteds = sqla.Column(sqla.Integer, default=0)
    escaped_interdicteds = sqla.Column(sqla.Integer, default=0)
    most_kills_system_id = sqla.Column(sqla.BigInteger, default=0)
    most_deaths_system_id = sqla.Column(sqla.BigInteger, default=0)

    interdiction_kills = sqla.Column(sqla.Integer, default=0)
    interdiction_deaths = sqla.Column(sqla.Integer, default=0)
    interdicted_kills = sqla.Column(sqla.Integer, default=0)
    interdicted_deaths = sqla.Column(sqla.Integer, default=0)

    # Names of CMDRS not guaranteed in db, could just be from logs.
    killed_most = sqla.Column(sqla.String(EDDB_LEN['pvp_name']))
    most_deaths_by = sqla.Column(sqla.String(EDDB_LEN['pvp_name']))
    most_interdictions = sqla.Column(sqla.String(EDDB_LEN['pvp_name']))
    most_interdicted_by = sqla.Column(sqla.String(EDDB_LEN['pvp_name']))
    most_escaped_interdictions_from = sqla.Column(sqla.String(EDDB_LEN['pvp_name']))

    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    cmdr = sqla.orm.relationship('PVPCmdr', viewonly=True)
    most_kills_system = sqla.orm.relationship(
        'System', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(PVPStat.most_kills_system_id) == System.id')
    most_deaths_system = sqla.orm.relationship(
        'System', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(PVPStat.most_deaths_system_id) == System.id')
    last_location = sqla.orm.relationship(
        'PVPLocation', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(PVPStat.last_location_id) == PVPLocation.id')
    last_kill = sqla.orm.relationship(
        'PVPKill', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(PVPStat.last_kill_id) == PVPKill.id')
    last_death = sqla.orm.relationship(
        'PVPDeath', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(PVPStat.last_death_id) == PVPDeath.id')
    last_escaped_interdicted = sqla.orm.relationship(
        'PVPEscapedInterdicted', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(PVPStat.last_escaped_interdicted_id) == PVPEscapedInterdicted.id')
    last_interdiction = sqla.orm.relationship(
        'PVPInterdiction', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(PVPStat.last_interdiction_id) == PVPInterdiction.id')
    last_interdicted = sqla.orm.relationship(
        'PVPInterdicted', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(PVPStat.last_interdicted_id) == PVPInterdicted.id')

    @property
    def kill_ratio(self):
        """ Return the k/d of a user. """
        try:
            ratio = round(float(self.kills) / self.deaths, 2)
        except ZeroDivisionError:
            ratio = 0.0

        return ratio

    @property
    def embed_values(self):
        """ Return values for creating an embed. """
        return [
            {'name': name, 'value': value, 'inline': True} for name, value in self.table_cells()
        ]

    def table_cells(self):
        """
        Returns a list of cells that contain all the information for string representations.
        """
        return [
            ['Kills', str(self.kills)],
            ['Deaths', str(self.deaths)],
            ['K/D', str(self.kill_ratio)],
            ['Interdictions', str(self.interdictions)],
            ['Interdiction -> Kill', str(self.interdiction_kills)],
            ['Interdiction -> Death', str(self.interdiction_deaths)],
            ['Interdicteds', str(self.interdicteds)],
            ['Interdicted -> Kill', str(self.interdicted_kills)],
            ['Interdicted -> Death', str(self.interdicted_deaths)],
            ['Escapes From Interdiction', str(self.escaped_interdicteds)],
            ['Most Kills', f'CMDR {self.killed_most}'],
            ['Most Deaths By', f'CMDR {self.most_deaths_by}'],
            ['Most Interdictions', f'CMDR {self.most_interdictions}'],
            ['Most Interdicted By', f'CMDR {self.most_interdicted_by}'],
            ['Most Escaped Interdictions From', f'CMDR {self.most_escaped_interdictions_from}'],
            ['Most Kills In', self.most_kills_system.name if self.most_kills_system else 'N/A'],
            ['Most Deaths In', self.most_deaths_system.name if self.most_deaths_system else 'N/A'],
            ['Last Location', self.last_location.embed() if self.last_location else 'N/A'],
            ['Last Kill', self.last_kill.embed() if self.last_kill else 'N/A'],
            ['Last Death By', self.last_death.embed() if self.last_death else 'N/A'],
            ['Last Interdiction', self.last_interdiction.embed() if self.last_interdiction else 'N/A'],
            ['Last Interdicted By', self.last_interdicted.embed() if self.last_interdicted else 'N/A'],
            ['Last Escaped From', self.last_escaped_interdicted.embed() if self.last_escaped_interdicted else 'N/A'],
        ]

    def __str__(self):
        """ A string representation of PVPStat. """
        return cog.tbl.format_table([['Statistic', 'Value']] + self.table_cells(),
                                    header=True, wrap_msgs=False)[0]

    def __eq__(self, other):
        return isinstance(other, PVPStat) and hash(self) == hash(other)

    def __hash__(self):
        return hash(self.cmdr_id)


class PVPLog(ReprMixin, TimestampMixin, Base):
    """
    Table to store hashes of uploaded logs or zip files.
    """
    __tablename__ = 'pvp_logs'
    _repr_keys = ['id', 'cmdr_id', 'func_used', 'file_hash',
                  'filename', 'msg_id', 'filtered_msg_id', 'updated_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cmdr_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_cmdrs.id'))

    func_used = sqla.Column(sqla.Integer, default=0)  # See PVP_HASH_FUNCS
    file_hash = sqla.Column(sqla.String(EDDB_LEN['pvp_hash']), index=True)
    filename = sqla.Column(sqla.String(EDDB_LEN['pvp_fname']))  # This is the actual name of the file in msg.attachements
    msg_id = sqla.Column(sqla.BigInteger)  # This is the message in archive channel
    filtered_msg_id = sqla.Column(sqla.BigInteger)  # This is the message in archive channel
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    cmdr = sqla.orm.relationship('PVPCmdr', viewonly=True)

    @property
    def filtered_filename(self):
        """ The filtered filename for zips and logs. """
        fname = self.filename

        if fname.endswith('.zip') or fname.endswith('.log'):
            fname = fname[:-4] + '.filter' + fname[-4:]

        return fname

    def __eq__(self, other):
        return isinstance(other, PVPLog) and hash(self) == hash(other)

    def __hash__(self):
        return self.file_hash


class PVPMatchState(enum.IntEnum):
    SETUP = 0
    STARTED = 1
    FINISHED = 2


class PVPMatch(ReprMixin, TimestampMixin, Base):
    """
    Table to store matches.
    """
    __tablename__ = 'pvp_matches'
    _repr_keys = ['id', 'limit', 'state', 'created_at', 'updated_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    discord_channel_id = sqla.Column(sqla.BigInteger, default=0)
    limit = sqla.Column(sqla.Integer, default=20)
    state = sqla.Column(sqla.Integer, default=int(PVPMatchState.SETUP))
    created_at = sqla.Column(sqla.Integer, default=time.time)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    def __eq__(self, other):
        return isinstance(other, PVPMatch) and hash(self) == hash(other)

    def __hash__(self):
        return self.id

    @sqla_orm.validates('state')
    def validate_state(self, _, value):
        """ Validation function for cycle. """
        try:
            return int(value)
        except ValueError as exc:
            raise cog.exc.ValidationFail("State must be set to an int convertible value.") from exc

    @property
    def winners(self):
        """ Return the list of winners from the match. """
        return [x for x in self.players if x.won]

    def get_player(self, *, cmdr_id):
        """
        Get a player in this match with cmdr_id.

        Args:
            cmdr_id: The cmdr id to look for.

        Returns: The PVPMatchPlayer requested or None if not found.
        """
        found = [x for x in self.players if x.cmdr_id == cmdr_id]

        return found[0] if found else None

    def add_player(self, *, cmdr_id):
        """
        Add new player to match. If already in match, return None.
        Assumption: There must be a PVPCmdr for given cmdr_id already existing.

        Args:
            match_id: The match id to add the new player.
            cmdr_id: The player id to add.

        Returns: The PVPMatchPlayer. If already in match, return None.
        """
        found = [x for x in self.players if x.cmdr_id == cmdr_id]

        if found:
            return None

        player = PVPMatchPlayer(match_id=self.id, cmdr_id=cmdr_id)
        eddb_session = sqla.inspect(self).session
        eddb_session.add(player)
        eddb_session.commit()

        return player

    def clone(self):
        """
        Given an existing match and players, create a new match with same players and teams.

        Args:
            eddb_session: A session onto the EDDB db.

        Returns: The new PVPMatch.
        """
        new_match = PVPMatch(state=PVPMatchState.SETUP, limit=self.limit)
        eddb_session = sqla.inspect(self).session
        eddb_session.add(new_match)
        eddb_session.flush()

        new_players = [PVPMatchPlayer(cmdr_id=x.cmdr_id, match_id=new_match.id, team=x.team) for x in self.players]
        eddb_session.add_all(new_players)
        eddb_session.commit()

        return new_match

    def roll_teams(self):
        """
        Roll new teams based on current players.

        Returns: A tuple of Team array (Team1, Team2).
        """
        all_players = set(self.players)
        half_players = len(self.players) / 2
        # When half an odd number, randomly choose which team short 1
        team1_size = random.randint(math.floor(half_players), math.ceil(half_players))
        team1 = set(random.sample(self.players, team1_size))
        team2 = set(all_players) - team1

        for player in team1:
            player.team = 1
        for player in team2:
            player.team = 2

        return list(sorted(team1)), list(sorted(team2))

    def finish(self, *, winning_team):
        """
        Finish a match.

        Args:
            winning_team: The team number than won.

        Returns: The PVPMatch.
        """
        for player in self.players:
            player.won = player.team == winning_team
        self.state = PVPMatchState.FINISHED

        return self

    def teams_dict(self):
        """
        Generate a dictionary that separates players by teams.

        Returns: A dictionary that maps team number to PVPMatchPlayer lists.
        """
        teams = {}

        for player in self.players:
            try:
                teams[player.team] += [player]
            except KeyError:
                teams[player.team] = [player]

        # Sort the players by name within each team
        for key in teams:
            teams[key] = list(sorted(teams[key]))

        return teams

    def embed_dict(self, *, color=None):
        """
        Generate an embed that describes the current state of a match.
        It can be used with discord.Embed.from_dict to create an embed.

        Returns: A dictionary to create an Embed with.
        """
        color = color if color else PVP_DEFAULT_HEX
        embed_values = [{'name': 'State', 'value': MATCH_STATES[self.state], 'inline': True}]
        for team_num, players in self.teams_dict().items():
            player_names = [x.cmdr.name for x in players]
            embed_values += [{'name': f'Team {team_num}', 'value': '\n'.join(player_names), 'inline': True}]
        embed_values = sorted(embed_values, key=lambda x: x['name'])

        return {
            'color': color,
            'author': {
                'name': 'PvP Match',
                'icon_url': cog.util.BOT.user.display_avatar.url if cog.util.BOT else None,
            },
            'provider': {
                'name': cog.util.BOT.user.name if cog.util.BOT else 'N/A',
            },
            'title': f'PVP Match: {len(self.players)}/{self.limit}',
            "fields": embed_values,
        }


@functools.total_ordering
class PVPMatchPlayer(ReprMixin, TimestampMixin, Base):
    """
    Table to store matches participants.
    """
    __tablename__ = 'pvp_match_players'
    _repr_keys = ['id', 'cmdr_id', 'match_id', 'team', 'updated_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cmdr_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_cmdrs.id'))
    match_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_matches.id'))
    team = sqla.Column(sqla.Integer, default=0)
    won = sqla.Column(sqla.Boolean, default=False)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    cmdr = sqla.orm.relationship('PVPCmdr', viewonly=True)

    def __eq__(self, other):
        return isinstance(other, PVPMatchPlayer) and hash(self) == hash(other)

    def __lt__(self, other):
        return self.cmdr.name < other.cmdr.name

    def __hash__(self):
        return hash(f'{self.match_id}_{self.cmdr_id}')


# Relationships that are back_populates
PVPCmdr.kills = sqla_orm.relationship(
    'PVPKill', uselist=True, back_populates='cmdr', lazy='select')
PVPKill.cmdr = sqla_orm.relationship(
    'PVPCmdr', uselist=False, back_populates='kills', lazy='select')
PVPCmdr.deaths = sqla_orm.relationship(
    'PVPDeath', uselist=True, back_populates='cmdr', lazy='select')
PVPDeath.cmdr = sqla_orm.relationship(
    'PVPCmdr', uselist=False, back_populates='deaths', lazy='select')
PVPDeath.killers = sqla_orm.relationship(
    'PVPDeathKiller', uselist=True, back_populates='death', lazy='select')
PVPDeathKiller.death = sqla_orm.relationship(
    'PVPDeath', uselist=False, back_populates='killers', lazy='select')
PVPCmdr.interdictions = sqla_orm.relationship(
    'PVPInterdiction', uselist=True, back_populates='cmdr', lazy='select')
PVPInterdiction.cmdr = sqla_orm.relationship(
    'PVPCmdr', uselist=False, back_populates='interdictions', lazy='select')
PVPCmdr.interdicteds = sqla_orm.relationship(
    'PVPInterdicted', uselist=True, back_populates='cmdr', lazy='select')
PVPInterdicted.cmdr = sqla_orm.relationship(
    'PVPCmdr', uselist=False, back_populates='interdicteds', lazy='select')
PVPMatch.players = sqla_orm.relationship(
    'PVPMatchPlayer', uselist=True, back_populates='match', lazy='joined', cascade='all, delete')
PVPMatchPlayer.match = sqla_orm.relationship(
    'PVPMatch', uselist=False, back_populates='players', lazy='joined')


def get_pvp_cmdr(eddb_session, *, cmdr_id=None, cmdr_name=None):
    """
    Get the PVPCmdr for a given discord user.
    You may lookup either by cmdr_id or cmdr_name.

    Args:
        eddb_session: A session onto the EDDB db.
        cmdr_id: The discord id of the CMDR.
        cmdr_name: The name of the CMDR.

    Returns: The PVPCmdr if present, None otherwise.
    """
    try:
        query = eddb_session.query(PVPCmdr)

        if cmdr_id:
            query = query.filter(PVPCmdr.id == cmdr_id)
        if cmdr_name:
            query = query.filter(PVPCmdr.name == cmdr_name)

        cmdr = query.one()
    except sqla.exc.NoResultFound:
        cmdr = None

    return cmdr


def update_pvp_cmdr(eddb_session, discord_id, *, name, hex_colour):
    """
    Update (or add) the cmdr with the attached information.

    Args:
        eddb_session: A session onto the EDDB db.
        discord_id: The discord id of the CMDR.
        name: The name of the CMDR.

    Raises:
        cog.exc.InvalidCommandArgs - Invalid hex_colour was selected.

    Returns: The added PVPCmdr.
    """
    try:
        if hex_colour is not None:
            int(hex_colour, 16)
        cmdr = eddb_session.query(PVPCmdr).\
            filter(PVPCmdr.id == discord_id).\
            one()
        cmdr.name = name
        cmdr.hex = hex_colour
    except sqla.exc.NoResultFound:
        cmdr = PVPCmdr(id=discord_id, name=name, hex=hex_colour)
        eddb_session.add(cmdr)
        eddb_session.commit()
    except ValueError as exc:
        raise cog.exc.InvalidCommandArgs(f"Bad hex colour value: {hex_colour}") from exc

    return cmdr


def get_pvp_stats(eddb_session, cmdr_id):
    """
    Update the statistics for a commander. To be called after a major change to events tracked.

    Args:
        eddb_session: A session onto the EDDB db.
        cmdr_id: The cmdr's id.

    Returns: The PVPStat for the cmdr. If no stats recorded, None.
    """
    try:
        stat = eddb_session.query(PVPStat).\
            filter(PVPStat.cmdr_id == cmdr_id).\
            one()
    except sqla.exc.NoResultFound:
        stat = None

    return stat


def get_pvp_event_cmdrs(eddb_session, *, cmdr_id):
    """
    Relative the CMDR specified by cmdr_id, get the names of CMDRs who were:
        - Been killed most by cmdr_id
        - Who has caused most deaths of cmdr_id
        - Who has been interdicted most by cmdr_id
        - Who has interdicted most cmdr_id

    Args:
        eddb_session: A session onto the EDDB db.
        cmdr_id: The ID of a cmdr in the database.

    Returns: A dictionary of these results. If entry not found, will be 'N/A'.
    """
    found = {}

    try:
        count = sqla.func.count(PVPKill.victim_name)
        result = eddb_session.query(PVPKill.victim_name, count).\
            filter(PVPKill.cmdr_id == cmdr_id).\
            group_by(PVPKill.victim_name).\
            order_by(count.desc(), PVPKill.id.desc()).\
            limit(1).\
            one()[0]
    except sqla.exc.NoResultFound:
        result = 'N/A'
    found['killed_most'] = result
    try:
        count = sqla.func.count(PVPDeathKiller.name)
        result = eddb_session.query(PVPDeathKiller.name, count).\
            filter(PVPDeath.cmdr_id == cmdr_id).\
            join(PVPDeath, PVPDeath.id == PVPDeathKiller.pvp_death_id).\
            group_by(PVPDeathKiller.name).\
            order_by(count.desc(), PVPDeath.id.desc(), PVPDeathKiller.name).\
            limit(1).\
            one()[0]
    except sqla.exc.NoResultFound:
        result = 'N/A'
    found['most_deaths_by'] = result
    try:
        count = sqla.func.count(PVPInterdiction.victim_name)
        result = eddb_session.query(PVPInterdiction.victim_name, count).\
            filter(PVPInterdiction.cmdr_id == cmdr_id).\
            group_by(PVPInterdiction.victim_name).\
            order_by(count.desc(), PVPInterdiction.id.desc()).\
            limit(1).\
            one()[0]
    except sqla.exc.NoResultFound:
        result = 'N/A'
    found['most_interdictions'] = result
    try:
        count = sqla.func.count(PVPInterdicted.interdictor_name)
        result = eddb_session.query(PVPInterdicted.interdictor_name, count).\
            filter(PVPInterdicted.cmdr_id == cmdr_id).\
            group_by(PVPInterdicted.interdictor_name).\
            order_by(count.desc(), PVPInterdicted.id.desc()).\
            limit(1).\
            one()[0]
    except sqla.exc.NoResultFound:
        result = 'N/A'
    found['most_interdicted_by'] = result
    try:
        count = sqla.func.count(PVPEscapedInterdicted.interdictor_name)
        result = eddb_session.query(PVPEscapedInterdicted.interdictor_name, count).\
            filter(PVPEscapedInterdicted.cmdr_id == cmdr_id).\
            group_by(PVPEscapedInterdicted.interdictor_name).\
            order_by(count.desc(), PVPEscapedInterdicted.id.desc()).\
            limit(1).\
            one()[0]
    except sqla.exc.NoResultFound:
        result = 'N/A'
    found['most_escaped_interdictions_from'] = result

    return found


def get_pvp_last_events(eddb_session, *, cmdr_id):
    """
    Find the IDs of the last events the CMDR uploaded for parsing.
    If no possible ID found for an event, it will return None.

    Args:
        eddb_session: A session onto the EDDB db.
        cmdr_id: The ID of a cmdr in the database.

    Returns: A dictionary of these results. If entry not found, will be None.
    """
    found = {}

    for key, cls in [
        ('last_location_id', PVPLocation),
        ('last_kill_id', PVPKill),
        ('last_death_id', PVPDeath),
        ('last_escaped_interdicted_id', PVPEscapedInterdicted),
        ('last_interdiction_id', PVPInterdiction),
        ('last_interdicted_id', PVPInterdicted),
    ]:
        try:
            result = eddb_session.query(cls.id).\
                filter(cls.cmdr_id == cmdr_id).\
                order_by(cls.event_at.desc()).\
                limit(1).\
                one()[0]
        except sqla.exc.NoResultFound:
            result = None
        found[key] = result

    return found


def update_pvp_stats(eddb_session, *, cmdr_id):
    """
    Update the statistics for a commander. To be called after a major change to events tracked.

    Args:
        eddb_session: A session onto the EDDB db.
        cmdr_id: The cmdr's id.
    """
    kwargs = {
        'deaths': eddb_session.query(PVPDeath).filter(PVPDeath.cmdr_id == cmdr_id).count(),
        'kills': eddb_session.query(PVPKill).filter(PVPKill.cmdr_id == cmdr_id).count(),
        'interdictions':
            eddb_session.query(PVPInterdiction).
            filter(PVPInterdiction.cmdr_id == cmdr_id).
            count(),
        'escaped_interdicteds':
            eddb_session.query(PVPEscapedInterdicted).
            filter(PVPEscapedInterdicted.cmdr_id == cmdr_id).
            count(),
        'interdicteds':
            eddb_session.query(PVPInterdicted).
            filter(PVPInterdicted.cmdr_id == cmdr_id).
            count(),
        'interdiction_deaths':
            eddb_session.query(PVPInterdictionDeath).
            filter(PVPInterdictionDeath.cmdr_id == cmdr_id).
            count(),
        'interdiction_kills':
            eddb_session.query(PVPInterdictionKill).
            filter(PVPInterdictionKill.cmdr_id == cmdr_id).
            count(),
        'interdicted_deaths':
            eddb_session.query(PVPInterdictedDeath).
            filter(PVPInterdictedDeath.cmdr_id == cmdr_id).
            count(),
        'interdicted_kills':
            eddb_session.query(PVPInterdictedKill).
            filter(PVPInterdictedKill.cmdr_id == cmdr_id).
            count(),
    }

    try:
        count_system_id = sqla.func.count(PVPKill.system_id)
        result = eddb_session.query(PVPKill.system_id, count_system_id).\
            filter(PVPKill.cmdr_id == cmdr_id).\
            group_by(PVPKill.system_id).\
            order_by(count_system_id.desc(), PVPKill.id.desc()).\
            limit(1).\
            one()[0]
    except sqla.exc.NoResultFound:
        result = None
    kwargs['most_kills_system_id'] = result
    try:
        count_system_id = sqla.func.count(PVPDeath.system_id)
        result = eddb_session.query(PVPDeath.system_id, count_system_id).\
            filter(PVPDeath.cmdr_id == cmdr_id).\
            group_by(PVPDeath.system_id).\
            order_by(count_system_id.desc(), PVPDeath.id.desc()).\
            limit(1).\
            one()[0]
    except sqla.exc.NoResultFound:
        result = None
    kwargs['most_deaths_system_id'] = result

    kwargs.update(get_pvp_last_events(eddb_session, cmdr_id=cmdr_id))
    kwargs.update(get_pvp_event_cmdrs(eddb_session, cmdr_id=cmdr_id))

    try:
        stat = eddb_session.query(PVPStat).\
            filter(PVPStat.cmdr_id == cmdr_id).\
            one()
        stat.update(**kwargs)
    except sqla.exc.NoResultFound:
        kwargs['cmdr_id'] = cmdr_id
        stat = PVPStat(**kwargs)
        eddb_session.add(stat)
        eddb_session.flush()

    return stat


async def add_pvp_log(eddb_session, fname, *, cmdr_id):
    """
    Add a PVP log to the database and optionally update the client archive with it.
    When a PVPLog is new, the msg_id and filename will be None.

    Args:
        eddb_session: A session onto the EDDB db.
        fname: The filename of the log locally.
        cmdr_id: The cmdr's id.

    Returns: The PVPLog that was created or already existed.
    """
    sha512 = await cog.util.hash_file(fname, alg='sha512')
    try:
        pvp_log = eddb_session.query(PVPLog).\
            filter(PVPLog.file_hash == sha512).\
            one()
    except sqla.exc.NoResultFound:
        pvp_log = PVPLog(
            cmdr_id=cmdr_id,
            file_hash=sha512,
        )
        eddb_session.add(pvp_log)
        eddb_session.flush()

    return pvp_log


def get_filtered_pvp_logs(eddb_session):
    """
    Get all PVPLogs that have been filtered and have useful information to process.

    Args:
        eddb_session: A session onto the EDDB db.

    Returns: A list of PVPLogs with filtered files to retrieve.
    """
    return eddb_session.query(PVPLog).\
        filter(PVPLog.filtered_msg_id).\
        all()


@contextlib.asynccontextmanager
async def create_log_of_events(eddb_session, *, cmdr_id, events=None, last_n=None, after=None):
    """
    Generate a complete log dump of all recorded events for a player.
    This is a context manager, returns the files created with the information.

    Args:
        eddb_session: A session onto the EDDB db.
        cmdr_id: The id of the cmdr to get logs for.
        events: If passed in, filter only these events into log.
        last_n: If passed in, show only last n events found.
        after: If passed in, only show events after this timestamp.

    Returns: A list of files that were written for log upload.
    """
    if not events:
        events = [PVPLocation, PVPKill, PVPDeath, PVPInterdicted, PVPInterdiction, PVPEscapedInterdicted]

    all_logs = []
    for cls in events:
        query = eddb_session.query(cls).\
            filter(cls.cmdr_id == cmdr_id)

        if after:
            query = query.filter(cls.event_at >= after)

        all_logs += query.\
            order_by(cls.event_at).\
            all()
    all_logs = [f'{x}\n' for x in sorted(all_logs)]

    if last_n:
        all_logs = all_logs[-last_n:]

    tdir = tempfile.mkdtemp()
    try:
        yield await cog.util.grouped_text_to_files(
            grouped_lines=cog.util.group_by_filesize(all_logs),
            tdir=tdir, fname_gen=lambda num: f'file_{num:02}.txt'
        )

    finally:
        try:
            shutil.rmtree(tdir)
        except OSError:
            pass


def add_pvp_match(eddb_session, *, discord_channel_id, limit=None):
    """
    Create a new match in DB.

    Args:
        eddb_session: A session onto the EDDB db.
        discord_channel_id: The channel ID where the match was started.
        limit: The player limit if set.

    Returns: The added PVPMatch.
    """
    match = PVPMatch(discord_channel_id=discord_channel_id, limit=limit)
    eddb_session.add(match)
    eddb_session.commit()

    return match


def get_pvp_match(eddb_session, *, discord_channel_id, match_id=None, state=None):
    """
    Get the latest match with the state requested.

    Args:
        eddb_session: A session onto the EDDB db.
        discord_channel_id: The channel ID where the match was started.
        match_id: If present, filter for this exact id.
        state: The state requested, see PVPMatchState.

    Returns: The PVPMatch if present and not started, None otherwise.
    """
    try:
        match = eddb_session.query(PVPMatch).filter(PVPMatch.discord_channel_id == discord_channel_id)

        if match_id is not None:
            match = match.filter(PVPMatch.id == match_id)
        elif state is not None:
            match = match.filter(PVPMatch.state == int(state))

        match = match.order_by(PVPMatch.id.desc()).limit(1).one()
    except sqla.exc.NoResultFound:
        match = None

    return match


def remove_players_from_match(eddb_session, *, match_id, cmdr_ids):
    """
    Remove players from a given match.

    Args:
        eddb_session: A session onto the EDDB db.
        match_id: The match id to remoave the player.
        cmdr_ids: A list of cmdr_ids to remove.

    Returns: The PVPMatchPlayer.
    """
    eddb_session.query(PVPMatchPlayer).\
        filter(PVPMatchPlayer.cmdr_id.in_(cmdr_ids),
               PVPMatchPlayer.match_id == match_id).\
        delete()
    eddb_session.commit()


def purge_cmdr(eddb_session, *, cmdr_id):
    """
    Purge all information relating to a given CMDR.
    This action is final and will be committed.

    Args:
        eddb_session: A session onto the EDDB db.
        cmdr_id: The cmdr id to match.
    """
    for cls in PVP_TABLES:
        if cls in [PVPMatch, PVPCmdr]:
            continue

        eddb_session.query(cls).\
            filter(cls.cmdr_id == cmdr_id).\
            delete()
    eddb_session.flush()

    eddb_session.query(PVPCmdr).filter(PVPCmdr.id == cmdr_id).delete()
    eddb_session.commit()


def drop_tables(keep_cmdrs=False):  # pragma: no cover | destructive to test
    """
    Drop all tables related to this module.
    See is_safe_to_drop for validation on if a table should be dropped.
    """
    sqla.orm.session.close_all_sessions()

    tables = PVP_TABLES
    if keep_cmdrs:
        for tbl in PVP_TABLES_KEEP:
            tables.remove(tbl)

    for table in tables:
        try:
            table.__table__.drop(cogdb.eddb_engine)
        except sqla.exc.OperationalError:
            pass


def empty_tables(keep_cmdrs=False):
    """
    Empty all pvp related tables.
    See is_safe_to_drop for validation on if a table should be dropped.
    """
    sqla.orm.session.close_all_sessions()

    tables = PVP_TABLES
    if keep_cmdrs:
        for tbl in PVP_TABLES_KEEP:
            tables.remove(tbl)

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        for table in tables:
            eddb_session.query(table).delete()


def is_safe_to_drop(tbl_name):
    """
    Check if the table is safe to drop.
    Anything in this module has pvp prefix on table.
    """
    return tbl_name.startswith('pvp_')


def recreate_tables(keep_cmdrs=False):  # pragma: no cover | destructive to test
    """
    Recreate all tables in the database, mainly for schema changes and testing.
    """
    sqlalchemy.orm.session.close_all_sessions()
    drop_tables(keep_cmdrs)
    Base.metadata.create_all(cogdb.eddb_engine)


def main():  # pragma: no cover
    """ Simple main to test pvp db. """
    recreate_tables()

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        eddb_session.add_all([
            PVPCmdr(id=1, name='coolGuy'),
            PVPCmdr(id=2, name='shyGuy'),
            PVPCmdr(id=3, name='shootsALot'),
        ])
        eddb_session.flush()
        eddb_session.add_all([
            PVPKill(id=1, cmdr_id=1, victim_name='LeSuck', victim_rank=3),
            PVPKill(id=2, cmdr_id=1, victim_name='BadGuy', victim_rank=7),
            PVPKill(id=3, cmdr_id=1, victim_name='LeSuck', victim_rank=3),
            PVPKill(id=4, cmdr_id=2, victim_name='CanNotShoot', victim_rank=8),

            PVPDeath(id=1, cmdr_id=1, is_wing_kill=True),
            PVPDeath(id=2, cmdr_id=1, is_wing_kill=False),
            PVPDeath(id=3, cmdr_id=3, is_wing_kill=False),
            PVPDeathKiller(cmdr_id=1, pvp_death_id=1, name='BadGuyWon', rank=7, ship_id=30),
            PVPDeathKiller(cmdr_id=1, pvp_death_id=1, name='BadGuyHelper', rank=5, ship_id=38),
            PVPDeathKiller(cmdr_id=2, pvp_death_id=2, name='BadGuyWon', rank=7, ship_id=30),
            PVPDeathKiller(cmdr_id=3, pvp_death_id=3, name='BadGuyWon', rank=7, ship_id=30),

            PVPInterdiction(id=1, cmdr_id=1, is_player=True, is_success=True, survived=False,
                            victim_name="LeSuck", victim_rank=3),
            PVPInterdiction(id=2, cmdr_id=1, is_player=True, is_success=True, survived=True,
                            victim_name="LeSuck", victim_rank=3),

            PVPInterdicted(id=1, cmdr_id=1, is_player=True, did_submit=False, survived=False,
                           interdictor_name="BadGuyWon", interdictor_rank=7),
            PVPInterdicted(id=2, cmdr_id=2, is_player=True, did_submit=True, survived=True,
                           interdictor_name="BadGuyWon", interdictor_rank=7),
            PVPMatch(id=1, discord_channel_id=99, limit=10, state=PVPMatchState.SETUP),
            PVPMatch(id=2, discord_channel_id=100, limit=20, state=PVPMatchState.FINISHED),
            PVPMatchPlayer(id=1, cmdr_id=1, match_id=1, team=1, won=False),
            PVPMatchPlayer(id=2, cmdr_id=2, match_id=1, team=2, won=False),
            PVPMatchPlayer(id=3, cmdr_id=3, match_id=1, team=2, won=False),
        ])
        eddb_session.flush()
        eddb_session.add_all([
            PVPInterdictionKill(cmdr_id=1, pvp_interdiction_id=1, pvp_kill_id=1),
            PVPInterdictionDeath(cmdr_id=2, pvp_interdiction_id=2, pvp_death_id=2),
            PVPInterdictedKill(cmdr_id=3, pvp_interdicted_id=2, pvp_kill_id=3),
            PVPInterdictedDeath(cmdr_id=1, pvp_interdicted_id=1, pvp_death_id=1),
        ])
        eddb_session.commit()

        kill = eddb_session.query(PVPKill).first()
        print(repr(kill))
        print(kill.cmdr)
        print(kill.event_date)
        death = eddb_session.query(PVPDeath).filter(PVPDeath.id == 1).one()
        print(death.killers)
        empty_tables()


PVP_TABLES = [
    PVPMatchPlayer, PVPMatch,
    PVPLog, PVPStat, PVPInterdictedKill, PVPInterdictedDeath, PVPInterdictionKill, PVPInterdictionDeath,
    PVPEscapedInterdicted, PVPInterdicted, PVPInterdiction, PVPDeathKiller, PVPDeath, PVPKill, PVPLocation, PVPCmdr
]
PVP_TABLES_KEEP = [PVPLog, PVPCmdr, PVPMatchPlayer, PVPMatch]
# Mainly archival, in case need to move to other hashes.
PVP_HASH_FUNCS = {
    0: 'sha512',
}
if __name__ == "__main__":
    main()
