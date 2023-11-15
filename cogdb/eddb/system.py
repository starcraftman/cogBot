"""
EDDB System and related tables.
"""
import math
import time

import sqlalchemy as sqla
from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method

from cogdb.eddb.common import Base, LEN
from cog.util import ReprMixin, TimestampMixin, UpdatableMixin

# State ids: 16 control, 32 exploited, 48 contested
VIEW_CONTESTEDS = """
CREATE or REPLACE VIEW eddb.v_systems_contested
AS
    SELECT s.system_id as system_id, exp.name as system,
           s.control_id as control_id, con.name as control,
           s.power_id as power_id, p.text as power
    FROM systems_controlled as s
    INNER JOIN systems as exp ON s.system_id = exp.id
    INNER JOIN systems as con ON s.control_id = con.id
    INNER JOIN powers as p ON s.power_id = p.id
    INNER JOIN power_state as ps ON s.power_state_id = ps.id
    WHERE s.power_state_id = 48
    ORDER BY exp.name;
"""
VIEW_SYSTEM_CONTROLS = """
CREATE or REPLACE VIEW eddb.v_systems_controlled
AS
    SELECT s.system_id as system_id, exp.name as system,
           s.power_state_id as power_state_id, ps.text as power_state,
           s.control_id as control_id, con.name as control,
           s.power_id as power_id, p.text as power
    FROM systems_controlled as s
    INNER JOIN systems as exp ON s.system_id = exp.id
    INNER JOIN systems as con ON s.control_id = con.id
    INNER JOIN powers as p ON s.power_id = p.id
    INNER JOIN power_state as ps ON s.power_state_id = ps.id
    ORDER BY con.name, exp.name;
"""


class System(ReprMixin, TimestampMixin, UpdatableMixin, Base):
    """
    Repesents a system in the universe.

    See SystemControlV for complete control information, especially for contesteds.
    """
    __tablename__ = "systems"
    _repr_keys = [
        'id', 'name', 'population', 'needs_permit', 'updated_at', 'power_id', 'edsm_id',
        'primary_economy_id', 'secondary_economy_id', 'security_id', 'power_state_id',
        'controlling_minor_faction_id', 'x', 'y', 'z'
    ]

    id = sqla.Column(sqla.Integer, primary_key=True)
    ed_system_id = sqla.Column(sqla.BigInteger, index=True)
    controlling_minor_faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), nullable=True)
    power_id = sqla.Column(sqla.Integer, sqla.ForeignKey('powers.id'), default=0)
    power_state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('power_state.id'), default=0)
    primary_economy_id = sqla.Column(sqla.Integer, sqla.ForeignKey('economies.id'), default=10)
    secondary_economy_id = sqla.Column(sqla.Integer, sqla.ForeignKey('economies.id'), default=10)
    security_id = sqla.Column(sqla.Integer, sqla.ForeignKey('security.id'))

    name = sqla.Column(sqla.String(LEN["system"]), index=True)
    population = sqla.Column(sqla.BigInteger)
    needs_permit = sqla.Column(sqla.Integer)
    edsm_id = sqla.Column(sqla.Integer)
    x = sqla.Column(sqla.Numeric(10, 5, None, False))
    y = sqla.Column(sqla.Numeric(10, 5, None, False))
    z = sqla.Column(sqla.Numeric(10, 5, None, False))
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    primary_economy = relationship(
        'Economy', uselist=False, viewonly=True, lazy='select',
        primaryjoin='foreign(System.primary_economy_id) == Economy.id'
    )
    secondary_economy = relationship(
        'Economy', uselist=False, viewonly=True, lazy='select',
        primaryjoin='foreign(System.primary_economy_id) == Economy.id'
    )
    power = relationship('Power', viewonly=True)
    power_state = relationship('PowerState', viewonly=True)
    security = relationship('Security', viewonly=True)
    active_states = relationship(
        'FactionActiveState', viewonly=True, lazy='select',
        primaryjoin='and_(remote(FactionActiveState.system_id) == foreign(System.id), remote(FactionActiveState.faction_id) == foreign(System.controlling_minor_faction_id))',
    )
    allegiance = relationship(
        'Allegiance', uselist=False, viewonly=True, lazy='select',
        primaryjoin='and_(System.controlling_minor_faction_id == remote(Faction.id), foreign(Faction.allegiance_id) == foreign(Allegiance.id))',
    )
    government = relationship(
        'Government', uselist=False, viewonly=True, lazy='select',
        primaryjoin='and_(System.controlling_minor_faction_id == remote(Faction.id), foreign(Faction.government_id) == foreign(Government.id))',
    )
    controls = relationship(
        'System', uselist=True, viewonly=True, lazy='select', order_by='System.name',
        primaryjoin='and_(foreign(System.id) == remote(SystemControl.system_id), foreign(SystemControl.control_id) == remote(System.id))'
    )
    exploiteds = relationship(
        'System', uselist=True, viewonly=True, lazy='select', order_by='System.name',
        primaryjoin='and_(foreign(System.id) == remote(SystemControl.control_id), foreign(SystemControl.system_id) == remote(System.id))'
    )
    contesteds = relationship(
        'System', uselist=True, viewonly=True, lazy='select', order_by='System.name',
        primaryjoin='and_(foreign(System.id) == remote(SystemContestedV.control_id), foreign(SystemContestedV.system_id) == remote(System.id))',
    )
    controlling_faction = relationship(
        'Faction', uselist=False, lazy='select',
        primaryjoin='foreign(System.controlling_minor_faction_id) == Faction.id')
    stations = relationship(
        'Station', back_populates='system', uselist=True, lazy='select',
        primaryjoin='System.id == foreign(Station.system_id)',
    )

    @hybrid_property
    def is_populated(self):
        """
        Is the system populated?
        """
        return self.population and self.population > 0

    @is_populated.expression
    def is_populated(self):
        """
        Compute the distance from this system to other.
        """
        return self.population is not None and self.population > 0

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
        return sqla.func.sqrt((other.x - self.x) * (other.x - self.x)
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

    def calc_um_trigger(self, system, reinforced=0):
        """" Aproximates the default undermining trigger. """
        normal_trigger = round(5000 + (2750000 / math.pow(self.dist_to(system), 1.5)))
        return round(normal_trigger * (1 + (reinforced / 100)))

    def __eq__(self, other):
        return isinstance(self, System) and isinstance(other, System) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class SystemControl(ReprMixin, Base):
    """
    This table stores all pairs of systems and their controls.
    Importantly for this consideration a control system is not paired with itself.
    Use this system mainly for joins of the IDs, for queries use the related
    augmented views SystemControlV, SystemContestedV.
    """
    __tablename__ = "systems_controlled"
    _repr_keys = ['system_id', 'power_state_id', 'control_id', 'power_id']

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    control_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    power_id = sqla.Column(sqla.Integer, sqla.ForeignKey('powers.id'))
    power_state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('power_state.id'))

    def __eq__(self, other):
        return isinstance(self, SystemControl) and isinstance(other, SystemControl) and \
            hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.system_id}_{self.control_id}")


class SystemContestedV(ReprMixin, Base):
    """
    This table is a __VIEW__. See VIEW_CONTESTEDS.

    This view simply selects down only those contested systems.
    """
    __tablename__ = 'v_systems_contested'
    _repr_keys = ['system_id', 'system', 'control_id', 'control', 'power_id', 'power']

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    power_id = sqla.Column(sqla.Integer)
    control_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True, )

    system = sqla.Column(sqla.String(LEN['system']))
    control = sqla.Column(sqla.String(LEN['system']))
    power = sqla.Column(sqla.String(LEN['power']))

    def __eq__(self, other):
        return (isinstance(self, SystemContestedV) and isinstance(other, SystemContestedV)
                and self.__hash__() == other.__hash__())

    def __hash__(self):
        return hash(f"{self.id}_{self.control_id}")


class SystemControlV(ReprMixin, Base):
    """
    This table is a __VIEW__. See VIEW_SYSTEM_CONTROLS.

    This view augments SystemControl with joined text information.
    """
    __tablename__ = "v_systems_controlled"
    _repr_keys = [
        'system_id', 'system', 'power_state_id', 'power_state',
        'control_id', 'control', 'power_id', 'power'
    ]

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True, )
    power_state_id = sqla.Column(sqla.Integer)
    control_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True, )
    power_id = sqla.Column(sqla.Integer)

    system = sqla.Column(sqla.String(LEN["system"]))
    power_state = sqla.Column(sqla.String(LEN['power_state']))
    control = sqla.Column(sqla.String(LEN["system"]))
    power = sqla.Column(sqla.String(LEN['power']))

    def __str__(self):
        return f"{self.system} controlled by {self.control} ({self.power})"

    def __eq__(self, other):
        return isinstance(self, SystemControlV) and isinstance(other, SystemControlV) and \
            hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.system_id}_{self.control_id}")
