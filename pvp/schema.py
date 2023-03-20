"""
The database backend for pvp bot.
"""
import asyncio
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
EMPTY = 'N/A'


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
    hex = sqla.Column(sqla.String(6), default='')  # Hex strings, no leading 0x: B20000
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)
    created_at = sqla.Column(sqla.Integer, default=time.time)

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


class PVPInara(ReprMixin, TimestampMixin, Base):
    """
    Table to store inara specific information for the cmdr.
    """
    __tablename__ = 'pvp_inaras'
    _repr_keys = ['id', 'squad_id', 'discord_id', 'name', 'updated_at']

    id = sqla.Column(sqla.Integer, primary_key=True)  # Inara CMDR id
    squad_id = sqla.Column(sqla.Integer, sqla.ForeignKey('pvp_inara_squads.id'))
    discord_id = sqla.Column(sqla.BigInteger, unique=True)

    name = sqla.Column(sqla.String(EDDB_LEN['pvp_name']))
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    squad = sqla.orm.relationship('PVPInaraSquad', viewonly=True, lazy='joined')

    @property
    def cmdr_page(self):
        return f'https://inara.cz/elite/cmdr/{self.id}'

    @property
    def squad_page(self):
        return f'https://inara.cz/elite/squadron/{self.squad_id}'

    def __str__(self):
        """ Convenient string representation of this object. """
        squad = self.squad.name if self.squad else EMPTY
        return f'CMDR {self.name} ({squad})'

    def __eq__(self, other):
        return isinstance(other, PVPInara) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class PVPInaraSquad(ReprMixin, TimestampMixin, Base):
    """
    Table to store inara specific information for the cmdr.
    """
    __tablename__ = 'pvp_inara_squads'
    _repr_keys = ['id', 'name', 'updated_at']

    id = sqla.Column(sqla.Integer, primary_key=True)  # Actual inara squad id
    name = sqla.Column(sqla.String(EDDB_LEN['pvp_name']))
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    def __str__(self):
        """ Convenient string representation of this object. """
        return f'{self.name}'

    def __eq__(self, other):
        return isinstance(other, PVPInaraSquad) and self.id == other.id

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
    kos = sqla.Column(sqla.Boolean)
    created_at = sqla.Column(sqla.Integer, default=time.time)
    event_at = sqla.Column(sqla.Integer, default=time.time)  # Set to time in log

    def embed(self):
        """ Short string representation for embed. """
        kos = ' (KOS)' if self.kos else ''
        return f'CMDR {self.victim_name} ({self.event_date}){kos}'

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

    # TODO: This should be obsoleted given filtering changes
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
                'name': cog.util.BOT.user.name if cog.util.BOT else EMPTY,
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
PVPCmdr.inara = sqla_orm.relationship(
    'PVPInara', uselist=False, back_populates='cmdr', lazy='joined',
    primaryjoin='foreign(PVPInara.discord_id) == PVPCmdr.id')
PVPInara.cmdr = sqla_orm.relationship(
    'PVPCmdr', uselist=False, back_populates='inara', lazy='joined',
    primaryjoin='foreign(PVPInara.discord_id) == PVPCmdr.id')
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


def get_squad_cmdrs(eddb_session, *, squad_name=None, cmdr_id=None):
    """
    Get all PVPCmdrs in a given squad, the sqaud can be found by:
        - providing the squad name
        - providing a cmdr_id of a given cmdr inside the squad

    Args:
        eddb_session: A session onto the EDDB db.
        squad_name: The name of the CMDR.

    Raises:
        cog.exc.NoMatch: The squadron is not present in the database at present.

    Returns: The list of all PVPCmdrs in that squad. Empty list if not matched.
    """
    try:
        if cmdr_id:
            squad = eddb_session.query(PVPInaraSquad).\
                join(PVPInara, PVPInara.squad_id == PVPInaraSquad.id).\
                filter(PVPInara.discord_id == cmdr_id).\
                one()
        else:
            squad = eddb_session.query(PVPInaraSquad).\
                filter(PVPInaraSquad.name == squad_name).\
                one()
    except sqla.exc.NoResultFound as exc:
        raise cog.exc.NoMatch(squad_name, PVPInaraSquad) from exc

    return eddb_session.query(PVPCmdr).\
        join(PVPInara, PVPCmdr.id == PVPInara.discord_id).\
        join(PVPInaraSquad, PVPInara.squad_id == PVPInaraSquad.id).\
        filter(PVPInaraSquad.id == squad.id).\
        all()


def update_pvp_cmdr(eddb_session, discord_id, *, name, hex_colour=None):
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


def remove_pvp_inara(eddb_session, *, cmdr_id):
    """
    Remove the inara information for a given CMDR id.
    Squads removed only when last CMDR in it removed.

    Args:
        eddb_session: A session onto the EDDB db.
        cmdr_id: The CMDR id.
    """
    try:
        inara = eddb_session.query(PVPInara).\
            filter(PVPInara.discord_id == cmdr_id).\
            one()
        squad_id = inara.squad_id
        eddb_session.delete(inara)
        eddb_session.commit()

        if squad_id:
            inaras_with_squad = eddb_session.query(PVPInara).\
                filter(PVPInara.squad_id == squad_id).\
                all()
            if not inaras_with_squad:  # No remaining other PVPInara with squad
                eddb_session.query(PVPInaraSquad).\
                    filter(PVPInaraSquad.id == squad_id).\
                    delete()
                eddb_session.commit()
    except sqla.exc.NoResultFound:
        pass


def update_pvp_inara(eddb_session, info):
    """
    Update the inara information cached for the given CMDR.

    Args:
        eddb_session: A session onto the EDDB db.
        info: A dictionary object with info retrieved from CMDR's inara page.
            See: cog.inara.fetch_cmdr_info

    Returns: cmdr, squad: The cmdr and squad objects updated into the db.
    """
    try:
        squad = eddb_session.query(PVPInaraSquad).\
            filter(PVPInaraSquad.id == info['squad_id']).\
            one()
        squad.name = info['inara_squad']
    except sqla.exc.NoResultFound:
        squad = PVPInaraSquad(id=info['squad_id'], name=info['squad'])
        eddb_session.add(squad)
        eddb_session.flush()
    except KeyError:  # CMDR has no squad
        pass

    try:
        cmdr = eddb_session.query(PVPInara).\
            filter(PVPInara.discord_id == info['discord_id']).\
            one()
        cmdr.name = info['name']
        cmdr.id = info['id']
        cmdr.squad_id = info['squad_id']
        cmdr.discord_id = info['discord_id']
    except sqla.exc.NoResultFound:
        cmdr = PVPInara(
            id=info['id'],
            squad_id=info['squad_id'],
            discord_id=info['discord_id'],
            name=info['name'],
        )
        eddb_session.add(cmdr)

    return cmdr, squad


def get_pvp_event_cmdrs(eddb_session, *, cmdr_ids):
    """
    Relative the CMDR specified by cmdr_id, get the names of CMDRs who were:
        - Been killed most by cmdr_id
        - Who has caused most deaths of cmdr_id
        - Who has been interdicted most by cmdr_id
        - Who has interdicted most cmdr_id

    Args:
        eddb_session: A session onto the EDDB db.
        cmdr_ids: The list of CMDR ids to group for information.

    Returns: A dictionary of these results. If entry not found, will be 'N/A'.
    """
    found = {}

    try:
        count = sqla.func.count(PVPKill.victim_name)
        result = eddb_session.query(PVPKill.victim_name, count).\
            filter(PVPKill.cmdr_id.in_(cmdr_ids)).\
            group_by(PVPKill.victim_name).\
            order_by(count.desc(), PVPKill.id.desc()).\
            limit(1).\
            one()[0]
    except sqla.exc.NoResultFound:
        result = EMPTY
    found['killed_most'] = result
    try:
        count = sqla.func.count(PVPDeathKiller.name)
        result = eddb_session.query(PVPDeathKiller.name, count).\
            filter(PVPDeath.cmdr_id.in_(cmdr_ids)).\
            join(PVPDeath, PVPDeath.id == PVPDeathKiller.pvp_death_id).\
            group_by(PVPDeathKiller.name).\
            order_by(count.desc(), PVPDeath.id.desc(), PVPDeathKiller.name).\
            limit(1).\
            one()[0]
    except sqla.exc.NoResultFound:
        result = EMPTY
    found['most_deaths_by'] = result
    try:
        count = sqla.func.count(PVPInterdiction.victim_name)
        result = eddb_session.query(PVPInterdiction.victim_name, count).\
            filter(PVPInterdiction.cmdr_id.in_(cmdr_ids)).\
            group_by(PVPInterdiction.victim_name).\
            order_by(count.desc(), PVPInterdiction.id.desc()).\
            limit(1).\
            one()[0]
    except sqla.exc.NoResultFound:
        result = EMPTY
    found['most_interdictions'] = result
    try:
        count = sqla.func.count(PVPInterdicted.interdictor_name)
        result = eddb_session.query(PVPInterdicted.interdictor_name, count).\
            filter(PVPInterdicted.cmdr_id.in_(cmdr_ids)).\
            group_by(PVPInterdicted.interdictor_name).\
            order_by(count.desc(), PVPInterdicted.id.desc()).\
            limit(1).\
            one()[0]
    except sqla.exc.NoResultFound:
        result = EMPTY
    found['most_interdicted_by'] = result
    try:
        count = sqla.func.count(PVPEscapedInterdicted.interdictor_name)
        result = eddb_session.query(PVPEscapedInterdicted.interdictor_name, count).\
            filter(PVPEscapedInterdicted.cmdr_id.in_(cmdr_ids)).\
            group_by(PVPEscapedInterdicted.interdictor_name).\
            order_by(count.desc(), PVPEscapedInterdicted.id.desc()).\
            limit(1).\
            one()[0]
    except sqla.exc.NoResultFound:
        result = EMPTY
    found['most_escaped_interdictions_from'] = result

    return found


def get_pvp_last_events(eddb_session, *, cmdr_ids):
    """
    Find the IDs of the last events the CMDR uploaded for parsing.
    If no possible ID found for an event, it will return None.

    Args:
        eddb_session: A session onto the EDDB db.
        cmdr_ids: The list of CMDR ids to group and get last events for.

    Returns: A dictionary of these results. If entry not found, will be None.
    """
    found = {}

    for key, cls in [
        ('last_location', PVPLocation),
        ('last_kill', PVPKill),
        ('last_death', PVPDeath),
        ('last_escaped_interdicted', PVPEscapedInterdicted),
        ('last_interdiction', PVPInterdiction),
        ('last_interdicted', PVPInterdicted),
    ]:
        try:
            result = eddb_session.query(cls).\
                filter(cls.cmdr_id.in_(cmdr_ids)).\
                order_by(cls.event_at.desc()).\
                limit(1).\
                one()
        except sqla.exc.NoResultFound:
            result = None
        found[key] = result

    return found


def compute_pvp_stats(eddb_session, *, cmdr_ids):
    """
    Given a list of cmdr_ids, compute statistics by selecting all events of these cmdrs.
    When selecting multiple CMDRs, it represents a group statistic.

    Args:
        eddb_session: A session onto the EDDB db.
        cmdr_ids: The list of CMDR ids.
    """
    kwargs = {
        label: eddb_session.query(cls).filter(cls.cmdr_id.in_(cmdr_ids)).count()
        for label, cls in [
            ['deaths', PVPDeath],
            ['kills', PVPKill],
            ['interdictions', PVPInterdiction],
            ['escaped_interdicteds', PVPEscapedInterdicted],
            ['interdicteds', PVPInterdicted],
            ['interdiction_deaths', PVPInterdictionDeath],
            ['interdiction_kills', PVPInterdictionKill],
            ['interdicted_deaths', PVPInterdictedDeath],
            ['interdicted_kills', PVPInterdictedKill],
        ]
    }
    kwargs['kos_kills'] = eddb_session.query(PVPKill).\
        filter(PVPKill.cmdr_id.in_(cmdr_ids),
               PVPKill.kos).\
        count()

    try:
        count_system_id = sqla.func.count(PVPKill.system_id)
        system_id = eddb_session.query(PVPKill.system_id, count_system_id).\
            filter(PVPKill.cmdr_id.in_(cmdr_ids)).\
            group_by(PVPKill.system_id).\
            order_by(count_system_id.desc(), PVPKill.id.desc()).\
            limit(1).\
            one()[0]
        result = eddb_session.query(cogdb.eddb.System.name).filter(cogdb.eddb.System.id == system_id).scalar()
    except sqla.exc.NoResultFound:
        result = EMPTY
    kwargs['most_kills_system'] = result
    try:
        count_system_id = sqla.func.count(PVPDeath.system_id)
        system_id = eddb_session.query(PVPDeath.system_id, count_system_id).\
            filter(PVPDeath.cmdr_id.in_(cmdr_ids)).\
            group_by(PVPDeath.system_id).\
            order_by(count_system_id.desc(), PVPDeath.id.desc()).\
            limit(1).\
            one()[0]
        result = eddb_session.query(cogdb.eddb.System.name).filter(cogdb.eddb.System.id == system_id).scalar()
    except sqla.exc.NoResultFound:
        result = EMPTY
    kwargs['most_deaths_system'] = result

    # Present embed if found else , N/A
    kwargs.update({key: val.embed() if val else EMPTY for key, val in get_pvp_last_events(eddb_session, cmdr_ids=cmdr_ids).items()})
    kwargs.update(get_pvp_event_cmdrs(eddb_session, cmdr_ids=cmdr_ids))

    return kwargs


def presentable_stats(info, *, discord_embed=False):
    """
    Converts the stats computed from dictionary form to a list suitable for table creation.

    Args:
        info: A dictionary of computed stats returned from compute_pvp_stats.
        for_embed: When set to True, will create a list of dictionary objects to create an Embed from.
                   When set to False, information will be in a 2D list of strings.

    Returns: A list of 2D strings or a dictionary of information to create a discord Embed.
    """
    try:
        ratio = round(float(info['kills']) / info['deaths'], 2)
    except ZeroDivisionError:
        ratio = 0.0

    cells = [
        ['Kills', str(info['kills'])],
        ['Deaths', str(info['deaths'])],
        ['K/D', str(ratio)],
        ['KOS Kills', str(info['kos_kills'])],
        ['Interdictions', str(info['interdictions'])],
        ['Interdiction -> Kill', str(info['interdiction_kills'])],
        ['Interdiction -> Death', str(info['interdiction_deaths'])],
        ['Interdicteds', str(info['interdicteds'])],
        ['Interdicted -> Kill', str(info['interdicted_kills'])],
        ['Interdicted -> Death', str(info['interdicted_deaths'])],
        ['Escapes From Interdiction', str(info['escaped_interdicteds'])],
        ['Most Kills', f"CMDR {info['killed_most']}"],
        ['Most Deaths By', f"CMDR {info['most_deaths_by']}"],
        ['Most Interdictions', f"CMDR {info['most_interdictions']}"],
        ['Most Interdicted By', f"CMDR {info['most_interdicted_by']}"],
        ['Most Escaped Interdictions From', f"CMDR {info['most_escaped_interdictions_from']}"],
        ['Most Kills In', info['most_kills_system']],
        ['Most Deaths In', info['most_deaths_system']],
        ['Last Location', info['last_location']],
        ['Last Kill', info['last_kill']],
        ['Last Death By', info['last_death']],
        ['Last Interdiction', info['last_interdiction']],
        ['Last Interdicted By', info['last_interdicted']],
        ['Last Escaped From', info['last_escaped_interdicted']],
    ]
    if discord_embed:
        cells = [
            {'name': name, 'value': value, 'inline': True} for name, value in cells
        ]

    return cells


def get_pvp_stats(eddb_session, *, cmdr_ids):
    """
    Update the statistics for a commander. To be called after a major change to events tracked.

    Args:
        eddb_session: A session onto the EDDB db.
        cmdr_id: The cmdr's id.

    Returns: The list of objects ready to be converted to an Discord Embed.
    """
    return presentable_stats(compute_pvp_stats(eddb_session, cmdr_ids=cmdr_ids), discord_embed=True)


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


def get_filtered_msg_ids(eddb_session):
    """
    Get all unique filtered msg IDs.

    Args:
        eddb_session: A session onto the EDDB db.

    Returns: A set of unique discord message IDs.
    """
    return {
        x[0] for x in eddb_session.query(PVPLog.filtered_msg_id).
        filter(PVPLog.filtered_msg_id).
        group_by(PVPLog.filtered_msg_id)
    }


def get_filtered_archives_by_cmdr(eddb_session):
    """
    Get a list of all messages with filtered archives.
    Create a map of cmdr_ids -> list of archives with logs for that cmdr.

    Args:
        eddb_session: A session onto the EDDB db.

    Returns: A list of PVPLogs with filtered files to retrieve.
    """
    pvp_logs = eddb_session.query(PVPLog).\
        filter(PVPLog.filtered_msg_id).\
        group_by(PVPLog.filtered_msg_id).\
        all()

    mapped_logs = {}
    for pvp_log in pvp_logs:
        try:
            mapped_logs[pvp_log.cmdr_id] += [pvp_log.filtered_msg_id]
        except KeyError:
            mapped_logs[pvp_log.cmdr_id] = [pvp_log.filtered_msg_id]

    return mapped_logs


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


def query_target_cmdr(query, *, cls, target_cmdr):
    """
    If possible narrow down a query to just events involving a target_cmdr.

    Args:
        query: The already partially builty query.
        cls: The base class of the query, i.e. PVPKill
        target_cmdr: The name of the CMDR to look for.

    Returns: The query passed in, modified to narrow down if possible.
    """
    if cls.__name__ == 'PVPDeath':
        query = query.join(PVPDeathKiller).filter(PVPDeathKiller.name == target_cmdr)
    elif cls.__name__ in ['PVPKill', 'PVPInterdiction']:
        query = query.filter(cls.victim_name == target_cmdr)
    elif cls.__name__ in ['PVPInterdicted', 'PVPEscapedInterdicted']:
        query = query.filter(cls.interdictor_name == target_cmdr)

    return query


def list_of_events(eddb_session, *, cmdr_id, events=None, limit=0,
                   after=None, target_cmdr=None, earliest_first=False):
    """
    Query all PVP events (or a subset) depending on a series of optional qualifiers.

    Args:
        eddb_session: A session onto the EDDB db.
        cmdr_id: The id of the cmdr to get logs for.
        events: If passed in, filter only these events into log.
        last_n: If passed in, show only last n events found.
        after: If passed in, only show events after this UNIX timestamp.
        target_cmdr: If passed, events will be filtered such that they include this CMDR.

    Returns: A list of strings of the matching db objects.
    """
    if not events:
        events = [PVPLocation, PVPKill, PVPDeath, PVPInterdicted, PVPInterdiction, PVPEscapedInterdicted]

    all_logs = []
    for cls in events:
        query = eddb_session.query(cls).\
            filter(cls.cmdr_id == cmdr_id)

        if target_cmdr:
            query = query_target_cmdr(query, cls=cls, target_cmdr=target_cmdr)
        if after:
            query = query.filter(cls.event_at >= after)

        query = query.order_by(cls.event_at if earliest_first else cls.event_at.desc())

        if limit and isinstance(limit, type(0)):
            query = query.limit(limit)

        all_logs += query.all()

    sorted_events = sorted(all_logs) if earliest_first else reversed(sorted(all_logs))
    all_logs = [f'{x}\n' for x in sorted_events]
    if limit and isinstance(limit, type(0)):
        all_logs = all_logs[-limit:]

    return all_logs


@contextlib.asynccontextmanager
async def create_log_of_events(events):
    """
    Generate a complete log dump of all events requested.
    This is a context manager, returns the files created with the information.

    Args:
        events: The list of string events to be writtent to files.

    Returns: A list of files that were written for log upload.
    """
    tdir = tempfile.mkdtemp()
    try:
        yield await cog.util.grouped_text_to_files(
            grouped_lines=cog.util.group_by_filesize(events),
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
        if cls in [PVPMatch, PVPCmdr, PVPInara, PVPInaraSquad]:
            continue

        eddb_session.query(cls).\
            filter(cls.cmdr_id == cmdr_id).\
            delete()
    eddb_session.flush()

    eddb_session.query(PVPInara).filter(PVPInara.discord_id == cmdr_id).delete()
    eddb_session.query(PVPCmdr).filter(PVPCmdr.id == cmdr_id).delete()
    eddb_session.commit()


def update_kos_kills(eddb_session, *, kos_list):
    """
    Bulk update to mark all KOS kills in the PVPKills table.

    Args:
        eddb_session: A session onto EDDB.
        kos_list: A list of CMDR names on the kill list.
    """
    eddb_session.query(PVPKill).\
        filter(PVPKill.victim_name.in_(kos_list)).\
        update({'kos': True})


def drop_tables(keep_cmdrs=False):  # pragma: no cover | destructive to test
    """
    Drop all tables related to this module.
    See is_safe_to_drop for validation on if a table should be dropped.
    """
    sqla.orm.session.close_all_sessions()

    tables = PVP_TABLES[:]
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
    PVPLog, PVPInterdictedKill, PVPInterdictedDeath, PVPInterdictionKill, PVPInterdictionDeath,
    PVPEscapedInterdicted, PVPInterdicted, PVPInterdiction, PVPDeathKiller, PVPDeath, PVPKill, PVPLocation,
    PVPInara, PVPInaraSquad, PVPCmdr
]
PVP_TABLES_KEEP = [PVPLog, PVPCmdr, PVPMatchPlayer, PVPMatch]
# Mainly archival, in case need to move to other hashes.
PVP_HASH_FUNCS = {
    0: 'sha512',
}
if __name__ == "__main__":
    main()
