"""
EDDB Economy table.
"""
import sqlalchemy as sqla

from cogdb.eddb.common import Base, LEN
from cog.util import ReprMixin


class Economy(ReprMixin, Base):
    """
    The type of economy present for a given Station or System.
    """
    __tablename__ = "economies"
    _repr_keys = ['id', 'text', 'eddn']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["economy"]))
    eddn = sqla.Column(sqla.String(LEN["economy"]))

    def __eq__(self, other):
        return (isinstance(self, Economy) and isinstance(other, Economy)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)
