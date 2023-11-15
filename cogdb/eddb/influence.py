"""
EDDB Influence table.
"""
import time

import sqlalchemy as sqla
from sqlalchemy.orm import relationship

from cogdb.eddb.common import Base
from cog.util import ReprMixin, TimestampMixin


EVENT_HISTORY_INFLUENCE = """
CREATE EVENT IF NOT EXISTS clean_history_influence
ON SCHEDULE
    EVERY 1 DAY
COMMENT "Check daily for HistoryInfluence entries older than 30 days."
DO
    DELETE FROM eddb.history_influence
    WHERE updated_at < (unix_timestamp() - (30 * 24 * 60 * 60));
"""


class Influence(ReprMixin, TimestampMixin, Base):
    """
    Represents the influence a Faction has withing a given System,
    includes the happiness and states affecting the faction.
    """
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
    happiness = relationship('FactionHappiness', lazy='joined', viewonly=True)
    system = relationship('System', viewonly=True)
    faction = relationship('Faction', viewonly=True)
    active_states = relationship(
        'FactionActiveState', cascade='all, delete, delete-orphan', lazy='joined',
        primaryjoin='and_(foreign(FactionActiveState.faction_id) == Influence.faction_id, foreign(FactionActiveState.system_id) == Influence.system_id)'
    )
    pending_states = relationship(
        'FactionPendingState', cascade='all, delete, delete-orphan', lazy='joined',
        primaryjoin='and_(foreign(FactionPendingState.faction_id) == Influence.faction_id, foreign(FactionPendingState.system_id) == Influence.system_id)'
    )
    recovering_states = relationship(
        'FactionRecoveringState', cascade='all, delete, delete-orphan', lazy='joined',
        primaryjoin='and_(foreign(FactionRecoveringState.faction_id) == Influence.faction_id, foreign(FactionRecoveringState.system_id) == Influence.system_id)'
    )

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


class HistoryTrack(ReprMixin, TimestampMixin, Base):
    """
    Set an entry to flag this system should be tracked.
    """
    __tablename__ = 'history_systems'
    _repr_keys = ['system_id', 'updated_at']

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    system = relationship('System', viewonly=True)

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
    system = relationship('System', viewonly=True)
    faction = relationship('Faction', viewonly=True)
    happiness = relationship('FactionHappiness', viewonly=True)

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
