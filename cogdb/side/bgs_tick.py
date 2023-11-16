"""
Mapping of BGSTick for remote side DB.
"""
import sqlalchemy as sqla

from cogdb.side.common import Base
from cog.util import ReprMixin


class BGSTick(ReprMixin, Base):
    """ Represents an upcoming BGS Tick (estimated). """
    __tablename__ = "bgs_tick"
    _repr_keys = ['day', 'tick', 'unix_from', 'unix_to']

    day = sqla.Column(sqla.Date, primary_key=True)  # Ignore not accurate
    tick = sqla.Column(sqla.DateTime)  # Actual expected tick
    unix_from = sqla.Column(sqla.Integer)
    unix_to = sqla.Column(sqla.Integer)

    def __eq__(self, other):
        return isinstance(self, BGSTick) and isinstance(other, BGSTick) and self.day == other.day
