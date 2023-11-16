"""
Mapping of Power and PowerState for remote side DB.
"""
import sqlalchemy as sqla

from cogdb.eddb.common import LEN
from cogdb.side.common import Base
from cog.util import ReprMixin


class Power(ReprMixin, Base):
    """ Represents a powerplay leader. """
    __tablename__ = "powers"
    _repr_keys = ['id', 'text', 'abbrev']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["power"]))
    abbrev = sqla.Column(sqla.String(LEN["power_abv"]))

    def __eq__(self, other):
        return (isinstance(self, Power) and isinstance(other, Power)
                and self.id == other.id)


class PowerState(ReprMixin, Base):
    """
    Represents the power state of a system (i.e. control, exploited).

    |  0 | None      |
    | 16 | Control   |
    | 32 | Exploited |
    | 48 | Contested |
    """
    __tablename__ = "power_state"
    _repr_keys = ['id', 'text']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["power_state"]))

    def __eq__(self, other):
        return (isinstance(self, PowerState) and isinstance(other, PowerState)
                and self.id == other.id)
