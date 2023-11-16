"""
Store the information about how powers are voting for consolidation.
"""
import time

import sqlalchemy as sqla

from cogdb.eddb.common import Base
from cog.util import ReprMixin, TimestampMixin


class SpyVote(ReprMixin, TimestampMixin, Base):
    """
    Record of the current consolidation vote by a power.
    """
    __tablename__ = 'spy_votes'
    _repr_keys = ['power_id', 'vote', 'updated_at']

    power_id = sqla.Column(sqla.Integer, primary_key=True)
    vote = sqla.Column(sqla.Integer, default=0)  # Current consolidation
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    power = sqla.orm.relationship(
        'Power', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(Power.id) == SpyVote.power_id',
    )

    def __str__(self):
        """ A pretty one line to give all information. """
        return f"{self.power.text}: {self.vote}%, updated at {self.updated_date}"

    def __eq__(self, other):
        return isinstance(other, SpyVote) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.power_id}")
