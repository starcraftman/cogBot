"""
Mapping of Government for remote side DB.
"""
import sqlalchemy as sqla

from cogdb.eddb.common import LEN
from cogdb.side.common import Base
from cog.util import ReprMixin


class Government(ReprMixin, Base):
    """ All faction government types. """
    __tablename__ = "gov_type"
    _repr_keys = ['id', 'text', 'eddn']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["government"]))
    eddn = sqla.Column(sqla.String(LEN["eddn"]))

    def __eq__(self, other):
        return (isinstance(self, Government) and isinstance(other, Government)
                and self.id == other.id)
