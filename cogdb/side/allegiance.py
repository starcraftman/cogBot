"""
Mapping of Allegiance for remote side DB.
"""
import sqlalchemy as sqla

from cogdb.eddb.common import LEN
from cogdb.side.common import Base
from cog.util import ReprMixin


class Allegiance(ReprMixin, Base):
    """ Represents the allegiance of a faction. """
    __tablename__ = "allegiance"
    _repr_keys = ['id', 'text']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["allegiance"]))

    def __eq__(self, other):
        return (isinstance(self, Allegiance) and isinstance(other, Allegiance)
                and self.id == other.id)
