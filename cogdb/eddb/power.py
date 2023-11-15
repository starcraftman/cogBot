"""
EDDB Power and PowerState tables.
"""
import sqlalchemy as sqla
from sqlalchemy.orm import relationship

from cogdb.eddb.common import Base, LEN
from cog.util import ReprMixin


class Power(ReprMixin, Base):
    """ Represents a powerplay leader. """
    __tablename__ = "powers"
    _repr_keys = ['id', 'text', 'eddn', 'abbrev', 'home_system_name']

    id = sqla.Column(sqla.Integer, primary_key=True, nullable=True, autoincrement=False)
    text = sqla.Column(sqla.String(LEN["power"]))
    eddn = sqla.Column(sqla.String(LEN["power"]))
    abbrev = sqla.Column(sqla.String(LEN["power_abv"]))
    home_system_name = sqla.Column(sqla.String(LEN["system"]))

    # Relationships
    home_system = relationship(
        'System', uselist=False, lazy='select', viewonly=True,
        primaryjoin='foreign(System.name) == Power.home_system_name',
    )

    def __eq__(self, other):
        return (isinstance(self, Power) and isinstance(other, Power)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class PowerState(ReprMixin, Base):
    """
    Represents the power state of a system (i.e. control, exploited).
    """
    __tablename__ = "power_state"
    _repr_keys = ['id', 'text', 'eddn']

    id = sqla.Column(sqla.Integer, primary_key=True, nullable=True, autoincrement=False)
    text = sqla.Column(sqla.String(LEN["power_state"]))
    eddn = sqla.Column(sqla.String(LEN["power_state"]))

    def __eq__(self, other):
        return (isinstance(self, PowerState) and isinstance(other, PowerState)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)
