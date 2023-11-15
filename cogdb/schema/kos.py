"""
Represents the kill on sight entries in the sheets.
"""
import sqlalchemy as sqla

from cogdb.schema.common import Base, LEN

from cog.util import ReprMixin


class KOS(ReprMixin, Base):
    """
    Represents a the kos list.
    """
    __tablename__ = 'kos'
    _repr_keys = ['id', 'cmdr', 'squad', 'reason', 'is_friendly']

    id = sqla.Column(sqla.Integer, primary_key=True)
    cmdr = sqla.Column(sqla.String(LEN['name']), index=True, nullable=False)
    squad = sqla.Column(sqla.String(LEN['name']), nullable=False)
    reason = sqla.Column(sqla.String(LEN['reason']), nullable=False)
    is_friendly = sqla.Column(sqla.Boolean, default=False)

    def __eq__(self, other):
        return isinstance(other, KOS) and (self.cmdr) == (other.cmdr)

    def __hash__(self):
        return hash(self.cmdr)

    @property
    def friendly(self):
        """ Return whether this entry is for a FRIENDLY or a KILL """
        return 'FRIENDLY' if self.is_friendly else 'KILL'
