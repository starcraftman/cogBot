"""
EDDB Faction and related tables.
"""
import time
import sqlalchemy as sqla
from sqlalchemy.orm import relationship

from cogdb.eddb.common import Base, LEN
from cog.util import ReprMixin, TimestampMixin, UpdatableMixin


class Faction(ReprMixin, TimestampMixin, UpdatableMixin, Base):
    """
    All tracked information for a given faction in the universe
    """
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
    allegiance = relationship('Allegiance', viewonly=True)
    government = relationship('Government', viewonly=True)
    state = relationship('FactionState', viewonly=True)
    home_system = relationship(
        'System', uselist=False, back_populates='controlling_faction', lazy='select',
        primaryjoin='foreign(Faction.home_system_id) == System.id'
    )

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
    state = relationship('FactionState', viewonly=True, lazy='joined')
    influence = relationship(
        'Influence', uselist=False, lazy='select',
        primaryjoin='and_(foreign(Influence.faction_id) == FactionActiveState.faction_id, foreign(Influence.system_id) == FactionActiveState.system_id)'
    )

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
    state = relationship('FactionState', viewonly=True, lazy='joined')
    influence = relationship(
        'Influence', uselist=False, lazy='select', viewonly=True,
        primaryjoin='and_(foreign(Influence.faction_id) == FactionPendingState.faction_id, foreign(Influence.system_id) == FactionPendingState.system_id)'
    )

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
    state = relationship('FactionState', viewonly=True, lazy='joined')
    influence = relationship(
        'Influence', uselist=False, lazy='select', viewonly=True,
        primaryjoin='and_(foreign(Influence.faction_id) == FactionRecoveringState.faction_id, foreign(Influence.system_id) == FactionRecoveringState.system_id)'
    )

    def __eq__(self, other):
        return (isinstance(self, FactionRecoveringState)
                and isinstance(other, FactionRecoveringState)
                and self.__hash__() == other.__hash__())

    def __hash__(self):
        return hash(f"{self.faction_id}_{self.system_id}_{self.state_id}")
