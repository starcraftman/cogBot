"""
EDDB Conflict tracking trables.
"""
import time

import sqlalchemy as sqla
from sqlalchemy.orm import relationship

from cogdb.eddb.common import Base, LEN
from cog.util import ReprMixin, TimestampMixin, UpdatableMixin


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
    Defines a single Conflict between two factions present in the same System.
    The factions may be fighting over Stations (identified as stakes) and the loser
    of the conflict forfeits the Station to the winner.
    The status represents the current state at last update.
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
    system = relationship('System', viewonly=True)
    status = relationship(
        'ConflictState', viewonly=True, lazy='select',
        primaryjoin='foreign(Conflict.status_id) == ConflictState.id',
    )
    type = relationship(
        'ConflictState', viewonly=True, lazy='select',
        primaryjoin='foreign(Conflict.type_id) == ConflictState.id',
    )
    faction1 = relationship(
        'Faction', viewonly=True, lazy='select',
        primaryjoin='foreign(Conflict.faction1_id) == Faction.id',
    )
    faction2 = relationship(
        'Faction', viewonly=True, lazy='select',
        primaryjoin='foreign(Conflict.faction2_id) == Faction.id',
    )
    faction1_stake = relationship(
        'Station', viewonly=True, lazy='select',
        primaryjoin='foreign(Conflict.faction1_stake_id) == Station.id',
    )
    faction2_stake = relationship(
        'Station', viewonly=True, lazy='select',
        primaryjoin='foreign(Conflict.faction2_stake_id) == Station.id',
    )

    def __eq__(self, other):
        return (isinstance(self, Conflict) and isinstance(other, Conflict)
                and self.__hash__() == other.__hash__())

    def __hash__(self):
        return hash(f"{self.system_id}_{self.faction1_id}_{self.faction2_id}")
