"""
EDDB Allegiance table.
"""
import sqlalchemy as sqla

from cogdb.eddb.common import Base, LEN
from cog.util import ReprMixin


class Allegiance(ReprMixin, Base):
    """
    Represents the allegiance of a Faction.
    """
    __tablename__ = "allegiance"
    _repr_keys = ['id', 'text', 'eddn']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["allegiance"]))
    eddn = sqla.Column(sqla.String(LEN["allegiance"]))

    def __eq__(self, other):
        return (isinstance(self, Allegiance) and isinstance(other, Allegiance)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)
