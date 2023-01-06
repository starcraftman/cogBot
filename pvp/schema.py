"""
The database backend for pvp bot.
"""
import datetime
import time

import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm
import sqlalchemy.orm.session
import sqlalchemy.ext.declarative
from sqlalchemy.schema import UniqueConstraint

import cogdb.eddb
from cogdb.eddb import LEN as EDDB_LEN
from cogdb.spy_squirrel import Base
from cog.util import ReprMixin, TimestampMixin, UpdatableMixin


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

    def __hash__(self):
        return hash(f'{self.cmdr_id}_{self.system_id}_{self.event_at}')


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

    def __str__(self):
        """ Show PVPKill information. """
        try:
            cmdr = self.cmdr.name
        except AttributeError:
            cmdr = self.id

        return f'CMDR {cmdr} killed CMDR {self.victim_name} at {self.event_date}'

    def __eq__(self, other):
        return isinstance(other, PVPKill) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f'{self.cmdr_id}_{self.system_id}_{self.event_at}')


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


class PVPInterdiction(ReprMixin, TimestampMixin, EventTimeMixin, Base):
    """
    Table to store events where cmdr interdicted other players to initiate combat.
    """
    __tablename__ = 'pvp_interdictions'
    __table_args__ = (
        UniqueConstraint('cmdr_id', 'system_id', 'event_at', name='_pvp_interdiction_unique'),
    )
    _repr_keys = ['id', 'cmdr_id', 'system_id', 'is_player', 'is_success', 'did_escape',
                  'victim_name', 'victim_rank', 'created_at', 'event_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cmdr_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_cmdrs.id'))
    system_id = sqla.Column(sqla.Integer)

    is_player = sqla.Column(sqla.Boolean, default=False)  # True if interdictor is player
    is_success = sqla.Column(sqla.Boolean, default=False)  # True if forced player out of super cruise
    did_escape = sqla.Column(sqla.Boolean, default=False)  # True if the victim escaped

    victim_name = sqla.Column(sqla.String(EDDB_LEN["pvp_name"]), index=True)
    victim_rank = sqla.Column(sqla.Integer, default=0)
    created_at = sqla.Column(sqla.Integer, default=time.time)
    event_at = sqla.Column(sqla.Integer, default=time.time)  # Set to time in log

    def __str__(self):
        """ Show a single interdiction by the cmdr. """
        try:
            cmdr = self.cmdr.name
        except AttributeError:
            cmdr = self.id

        return f"CMDR {cmdr} interdicted {'CMDR ' if self.is_player else ''}{self.victim_name} at {self.event_date}. Pulled from SC: {self.is_success} Escaped: {self.did_escape}"

    def __eq__(self, other):
        return isinstance(other, PVPInterdiction) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f'{self.cmdr_id}_{self.system_id}_{self.event_at}')


class PVPInterdictionKill(ReprMixin, TimestampMixin, EventTimeMixin, Base):
    """
    Table to store the event where an interdiction lead to a pvp kill.
    """
    __tablename__ = 'pvp_interdictions_kills'
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


class PVPInterdicted(ReprMixin, TimestampMixin, EventTimeMixin, Base):
    """
    Table to store events when a cmdr was interdicted by other players.
    """
    __tablename__ = 'pvp_interdicteds'
    __table_args__ = (
        UniqueConstraint('cmdr_id', 'system_id', 'event_at', name='_pvp_interdicted_unique'),
    )
    _repr_keys = ['id', 'cmdr_id', 'system_id', 'is_player', 'did_submit', 'did_escape',
                  'interdictor_name', 'interdictor_rank', 'created_at', 'event_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cmdr_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_cmdrs.id'))
    system_id = sqla.Column(sqla.Integer)

    is_player = sqla.Column(sqla.Boolean, default=False)  # True if interdictor is player
    did_submit = sqla.Column(sqla.Boolean, default=False)  # True if the fictim submitted
    did_escape = sqla.Column(sqla.Boolean, default=False)  # True if the cmdr managed to escape

    interdictor_name = sqla.Column(sqla.String(EDDB_LEN["pvp_name"]), index=True)
    interdictor_rank = sqla.Column(sqla.Integer, default=0)
    created_at = sqla.Column(sqla.Integer, default=time.time)
    event_at = sqla.Column(sqla.Integer, default=time.time)  # Set to time in log

    def __str__(self):
        """ Show a single time cmdr was interdicted by another. """
        try:
            cmdr = self.cmdr.name
        except AttributeError:
            cmdr = self.id

        return f"CMDR {cmdr} was interdicted by {'CMDR ' if self.is_player else ''}{self.interdictor_name} at {self.event_date}. Submitted: {self.did_submit}. Escaped: {self.did_escape}"

    def __eq__(self, other):
        return isinstance(other, PVPInterdicted) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f'{self.cmdr_id}_{self.system_id}_{self.event_at}')


class PVPInterdictedKill(ReprMixin, TimestampMixin, EventTimeMixin, Base):
    """
    Table to store the event where a cmdr was interdicted and pvp killed.
    """
    __tablename__ = 'pvp_interdicteds_kills'
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


class PVPStat(ReprMixin, TimestampMixin, UpdatableMixin, Base):
    """
    Table to store derived stats from pvp tracking.
    Represent a single commander who reports kills to the bot.
    """
    __tablename__ = 'pvp_stats'
    _repr_keys = ['id', 'cmdr_id', 'deaths', 'kills', 'interdictions', 'interdicteds',
                  'most_visited_system_id', 'least_visited_system_id',
                  'interdicted_kills', 'interdiction_deaths', 'interdicted_kills', 'interdicted_deaths',
                  'updated_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cmdr_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('pvp_cmdrs.id'))

    deaths = sqla.Column(sqla.Integer, default=0)
    kills = sqla.Column(sqla.Integer, default=0)
    interdictions = sqla.Column(sqla.Integer, default=0)
    interdicteds = sqla.Column(sqla.Integer, default=0)
    most_visited_system_id = sqla.Column(sqla.Integer, default=0)
    least_visited_system_id = sqla.Column(sqla.Integer, default=0)

    interdiction_kills = sqla.Column(sqla.Integer, default=0)
    interdiction_deaths = sqla.Column(sqla.Integer, default=0)
    interdicted_kills = sqla.Column(sqla.Integer, default=0)
    interdicted_deaths = sqla.Column(sqla.Integer, default=0)

    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    cmdr = sqla.orm.relationship('PVPCmdr', viewonly=True)
    most_visited_system = sqla.orm.relationship(
        'System', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(PVPStat.most_visited_system_id) == System.id')
    least_visited_system = sqla.orm.relationship(
        'System', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(PVPStat.least_visited_system_id) == System.id')

    @property
    def kill_ratio(self):
        """ Return the k/d of a user. """
        return float(self.kills) / self.deaths

    @property
    def embed_values(self):
        """ Return values for creating an embed. """
        return [
            {'name': "Kills", 'value': str(self.kills), 'inline': True},
            {'name': "Deaths", 'value': str(self.deaths), 'inline': True},
            {'name': "K/D", 'value': str(self.kill_ratio), 'inline': True},
            {'name': "Interdictions", 'value': str(self.interdictions), 'inline': True},
            {'name': "Interdiction -> Kill", 'value': str(self.interdiction_kills), 'inline': True},
            {'name': "Interdiction -> Death", 'value': str(self.interdiction_deaths), 'inline': True},
            {'name': "Interdicteds", 'value': str(self.interdicteds), 'inline': True},
            {'name': "Interdicted -> Kill", 'value': str(self.interdicted_kills), 'inline': True},
            {'name': "Interdicted -> Death", 'value': str(self.interdicted_deaths), 'inline': True},
            {'name': "Most Visited System", 'value': self.most_visited_system.name, 'inline': True},
            {'name': "Least Visited System", 'value': self.least_visited_system.name, 'inline': True},
        ]

    def __eq__(self, other):
        return isinstance(other, PVPStat) and hash(self) == hash(other)

    def __hash__(self):
        return hash(self.cmdr_id)


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


def get_pvp_cmdr(eddb_session, discord_id):
    """
    Get the PVPCmdr for a given discord user.

    Args:
        eddb_session: A session onto the EDDB db.
        discord_id: The discord id of the CMDR.

    Returns: The PVPCmdr if present, None otherwise.
    """
    try:
        cmdr = eddb_session.query(PVPCmdr).filter(PVPCmdr.id == discord_id).one()
    except sqla.exc.NoResultFound:
        cmdr = None

    return cmdr


def add_pvp_cmdr(eddb_session, discord_id, name, hex):
    """
    Ensure the one time setup of commander is performed.

    Args:
        eddb_session: A session onto the EDDB db.
        discord_id: The discord id of the CMDR.
        name: The name of the CMDR.

    Returns: The added PVPCmdr.
    """
    cmdr = PVPCmdr(id=discord_id, name=name, hex=hex)
    eddb_session.add(cmdr)
    eddb_session.commit()

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


def update_pvp_stats(eddb_session, cmdr_id):
    """
    Update the statistics for a commander. To be called after a major change to events tracked.

    Args:
        eddb_session: A session onto the EDDB db.
        cmdr_id: The cmdr's id.
    """
    count_system_id = sqla.func.count(PVPLocation.system_id)
    try:
        most_visited_id = eddb_session.query(PVPLocation.system_id, count_system_id).\
            group_by(PVPLocation.system_id).\
            order_by(count_system_id.desc()).\
            limit(1).\
            one()[0]
        least_visited_id = eddb_session.query(PVPLocation.system_id, count_system_id).\
            group_by(PVPLocation.system_id).\
            order_by(count_system_id).\
            limit(1).\
            one()[0]
    except sqla.exc.NoResultFound:
        most_visited_id = None
        least_visited_id = None

    kwargs = {
        'deaths': eddb_session.query(PVPDeath).filter(PVPDeath.cmdr_id == cmdr_id).count(),
        'kills': eddb_session.query(PVPKill).filter(PVPKill.cmdr_id == cmdr_id).count(),
        'interdictions': eddb_session.query(PVPInterdiction).filter(PVPInterdiction.cmdr_id == cmdr_id).count(),
        'interdicteds': eddb_session.query(PVPInterdicted).filter(PVPInterdicted.cmdr_id == cmdr_id).count(),
        'interdiction_deaths': eddb_session.query(PVPInterdictionDeath).
        filter(PVPInterdictionDeath.cmdr_id == cmdr_id).
        count(),
        'interdiction_kills': eddb_session.query(PVPInterdictionKill).filter(PVPInterdictionKill.cmdr_id == cmdr_id).count(),
        'interdicted_deaths': eddb_session.query(PVPInterdictedDeath).filter(PVPInterdictedDeath.cmdr_id == cmdr_id).count(),
        'interdicted_kills': eddb_session.query(PVPInterdictedKill).filter(PVPInterdictedKill.cmdr_id == cmdr_id).count(),
        'most_visited_system_id': most_visited_id,
        'least_visited_system_id': least_visited_id,
    }
    try:
        stat = eddb_session.query(PVPStat).\
            filter(PVPStat.cmdr_id == cmdr_id).\
            one()
    except sqla.exc.NoResultFound:
        stat = PVPStat(cmdr_id=cmdr_id)
        eddb_session.add(stat)
        eddb_session.flush()

    stat.update(**kwargs)
    return stat


def get_pvp_events(eddb_session, cmdr_id, last_n=10):
    """
    Fetch the most recent tracked events to display to a user.

    Args:
        eddb_session: A session onto the EDDB db.
        cmdr_id: The cmdr's id.
        last_n: The amount of recent events to be displayed.

    Returns: A list of db objects to display.
    """
    objs = []

    for cls in [PVPLocation, PVPKill, PVPDeath, PVPInterdiction, PVPInterdiction]:
        objs += eddb_session.query(cls).\
            filter(cls.cmdr_id == cmdr_id).\
            order_by(cls.event_at.desc()).\
            limit(last_n).\
            all()

    return sorted(objs, key=lambda x: x.event_at, reverse=True)[:last_n]


def drop_tables():  # pragma: no cover | destructive to test
    """
    Drop all tables related to this module.
    See is_safe_to_drop for validation on if a table should be dropped.
    """
    sqla.orm.session.close_all_sessions()
    for table in PVP_TABLES:
        try:
            table.__table__.drop(cogdb.eddb_engine)
        except sqla.exc.OperationalError:
            pass


def empty_tables():
    """
    Empty all pvp related tables.
    See is_safe_to_drop for validation on if a table should be dropped.
    """
    sqla.orm.session.close_all_sessions()
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        for table in PVP_TABLES:
            eddb_session.query(table).delete()


def is_safe_to_drop(tbl_name):
    """
    Check if the table is safe to drop.
    Anything in this module has pvp prefix on table.
    """
    return tbl_name.startswith('pvp_')


def recreate_tables():  # pragma: no cover | destructive to test
    """
    Recreate all tables in the database, mainly for schema changes and testing.
    """
    sqlalchemy.orm.session.close_all_sessions()
    drop_tables()
    Base.metadata.create_all(cogdb.eddb_engine)


def main():
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

            PVPInterdiction(id=1, cmdr_id=1, is_player=True, is_success=True, did_escape=False,
                            victim_name="LeSuck", victim_rank=3),
            PVPInterdiction(id=2, cmdr_id=1, is_player=True, is_success=True, did_escape=True,
                            victim_name="LeSuck", victim_rank=3),

            PVPInterdicted(id=1, cmdr_id=1, is_player=True, did_submit=False, did_escape=False,
                           interdictor_name="BadGuyWon", interdictor_rank=7),
            PVPInterdicted(id=2, cmdr_id=2, is_player=True, did_submit=True, did_escape=True,
                           interdictor_name="BadGuyWon", interdictor_rank=7),

        ])
        eddb_session.flush()
        eddb_session.add_all([
            PVPInterdictionKill(cmdr_id=1, pvp_interdiction_id=1, pvp_kill_id=1),
            PVPInterdictionDeath(cmdr_id=2, pvp_interdiction_id=2, pvp_death_id=2),
            PVPInterdictedKill(cmdr_id=3, pvp_interdicted_id=2, pvp_kill_id=3),
            PVPInterdictedDeath(cmdr_id=1, pvp_interdicted_id=1, pvp_death_id=1),
        ])
        eddb_session.commit()

        kill = eddb_session.query(PVPKill).limit(1).one()
        print(repr(kill))
        print(kill.cmdr)
        print(kill.event_date)
        death = eddb_session.query(PVPDeath).filter(PVPDeath.id == 1).one()
        print(death.killers)


PVP_TABLES = [
    PVPStat, PVPInterdictedKill, PVPInterdictedDeath, PVPInterdictionKill, PVPInterdictionDeath,
    PVPInterdicted, PVPInterdiction, PVPDeathKiller, PVPDeath, PVPKill, PVPLocation, PVPCmdr
]
if __name__ == "__main__":
    main()
