"""
EDDB Module and ModuleGroup, based on eddb.io
"""
import sqlalchemy as sqla
from sqlalchemy.orm import relationship

from cogdb.eddb.common import Base, LEN
from cog.util import ReprMixin, UpdatableMixin


class Module(ReprMixin, UpdatableMixin, Base):
    """ A module for a ship. """
    __tablename__ = "eddb_modules"
    _repr_keys = ['id', 'name', 'group_id', 'size', 'rating', 'mass', 'price', 'ship', 'weapon_mode']

    id = sqla.Column(sqla.Integer, primary_key=True)
    group_id = sqla.Column(sqla.Integer, sqla.ForeignKey('eddb_module_groups.id'), nullable=False)

    size = sqla.Column(sqla.Integer)  # Equal to in game size, 1-8.
    rating = sqla.Column(sqla.String(1))  # Rating is A-E
    price = sqla.Column(sqla.Integer, default=0)
    mass = sqla.Column(sqla.Integer, default=0)
    name = sqla.Column(sqla.String(LEN["module"]))  # Pacifier
    ship = sqla.Column(sqla.String(LEN["ship"]))  # Module sepfically for this ship
    weapon_mode = sqla.Column(sqla.String(LEN["weapon_mode"]))  # Fixed, Gimbal or Turret

    # Relationships
    group = relationship(
        'ModuleGroup', uselist=False, back_populates='modules', lazy='select'
    )

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


class ModuleGroup(ReprMixin, Base):
    """ A group for a Module. """
    __tablename__ = "eddb_module_groups"
    _repr_keys = ['id', 'name', 'category', 'category_id']

    id = sqla.Column(sqla.Integer, primary_key=True)
    category_id = sqla.Column(sqla.Integer)

    category = sqla.Column(sqla.String(LEN["module_category"]))
    name = sqla.Column(sqla.String(LEN["module_group"]))  # Name of module group, i.e. "Beam Laser"

    # Relationships
    modules = relationship(
        'Module', cascade='all, delete, delete-orphan', back_populates='group', lazy='select'
    )

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)
