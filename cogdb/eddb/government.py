"""
EDDB Government table.
"""
import sqlalchemy as sqla

from cogdb.eddb.common import Base, LEN
from cog.util import ReprMixin


class Government(ReprMixin, Base):
    """
    The types of government a Faction may have.
    """
    __tablename__ = "gov_type"
    _repr_keys = ['id', 'text', 'eddn']

    id = sqla.Column(sqla.Integer, primary_key=True, nullable=True, autoincrement=False)
    text = sqla.Column(sqla.String(LEN["government"]), nullable=False)
    eddn = sqla.Column(sqla.String(LEN["eddn"]), default=None)

    def __eq__(self, other):
        return (isinstance(self, Government) and isinstance(other, Government)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)
