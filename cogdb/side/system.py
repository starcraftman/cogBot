"""
Mapping of System and SystemAge for remote side DB.
"""
import math

import sqlalchemy as sqla
from sqlalchemy import func as sqlfunc
from sqlalchemy.ext.hybrid import hybrid_method

from cogdb.eddb.common import LEN
from cogdb.side.common import Base
from cog.util import ReprMixin


class System(ReprMixin, Base):
    """ Repesents a system in the universe. """
    __tablename__ = "systems"
    _repr_keys = ['id', 'name', 'population', 'income', 'hudson_upkeep',
                  'needs_permit', 'update_factions', 'power_id', 'edsm_id',
                  'security_id', 'power_state_id', 'controlling_faction_id',
                  'control_system_id', 'x', 'y', 'z', 'dist_to_nanomam']

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN["system"]))
    population = sqla.Column(sqla.BigInteger)
    income = sqla.Column(sqla.Integer)
    hudson_upkeep = sqla.Column(sqla.Integer)
    needs_permit = sqla.Column(sqla.Integer)
    update_factions = sqla.Column(sqla.Integer)
    edsm_id = sqla.Column(sqla.Integer)
    power_id = sqla.Column(sqla.Integer, sqla.ForeignKey('powers.id'),)
    security_id = sqla.Column(sqla.Integer, sqla.ForeignKey('security.id'),)
    power_state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('power_state.id'))
    controlling_faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'),)
    control_system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'),)
    x = sqla.Column(sqla.Numeric(10, 5, None, False))
    y = sqla.Column(sqla.Numeric(10, 5, None, False))
    z = sqla.Column(sqla.Numeric(10, 5, None, False))
    dist_to_nanomam = sqla.Column(sqla.Numeric(7, 2, None, False))

    def __eq__(self, other):
        return isinstance(self, System) and isinstance(other, System) and self.id == other.id

    @property
    def log_pop(self):
        """ The log base 10 of the population. For terse representation. """
        return f'{math.log(self.population, 10):.1f}'

    @hybrid_method
    def dist_to(self, other):
        """
        Compute the distance from this system to other.
        """
        dist = 0
        for let in ['x', 'y', 'z']:
            temp = getattr(other, let) - getattr(self, let)
            dist += temp * temp

        return math.sqrt(dist)

    @dist_to.expression
    def dist_to(self, other):
        """
        Compute the distance from this system to other.
        """
        return sqlfunc.sqrt((other.x - self.x) * (other.x - self.x)
                            + (other.y - self.y) * (other.y - self.y)
                            + (other.z - self.z) * (other.z - self.z))

    def calc_upkeep(self, system):
        """ Approximates the default upkeep. """
        dist = self.dist_to(system)
        return round(20 + 0.001 * (dist * dist), 1)

    def calc_fort_trigger(self, system):
        """ Approximates the default fort trigger. """
        dist = self.dist_to(system)
        return round(5000 - 5 * dist + 0.4 * (dist * dist))

    def calc_um_trigger(self, system):
        """" Aproximates the default undermining trigger. """
        return round(5000 + (2750000 / math.pow(self.dist_to(system), 1.5)))


class SystemAge(ReprMixin, Base):
    """ Represents the age of eddn data received for control/system pair. """
    __tablename__ = "v_age"
    _repr_keys = ['control', 'system', 'age']

    control = sqla.Column(sqla.String(LEN["system"]), primary_key=True)
    system = sqla.Column(sqla.String(LEN["system"]), primary_key=True)
    age = sqla.Column(sqla.Integer)

    def __eq__(self, other):
        return (isinstance(self, SystemAge) and isinstance(other, SystemAge)
                and self.control == other.control and self.system == other.system)
