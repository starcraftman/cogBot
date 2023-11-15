"""
EDDB New Module tables, based on spansh dump.
"""
import functools
import sqlalchemy as sqla
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint

from cogdb.eddb.common import Base, LEN
from cog.util import ReprMixin


@functools.total_ordering
class SModule(ReprMixin, Base):
    """ A ship module sold in a shipyard. """
    __tablename__ = 'spansh_modules'
    _repr_keys = [
        'id', 'group_id', "ship_id", "name", "symbol", "mod_class", "rating"
    ]

    id = sqla.Column(sqla.Integer, primary_key=True)  # moduleId
    group_id = sqla.Column(sqla.Integer, sqla.ForeignKey("spansh_module_groups.id"), nullable=False)
    ship_id = sqla.Column(sqla.Integer)

    name = sqla.Column(sqla.String(LEN["module"]))
    symbol = sqla.Column(sqla.String(200))
    mod_class = sqla.Column(sqla.Integer, default=1)
    rating = sqla.Column(sqla.String(5), default=1)

    # Relationships
    group = relationship(
        'SModuleGroup', uselist=False, back_populates='modules', lazy='joined'
    )

    @property
    def text(self):
        """ Alias for name. """
        return self.name

    def __eq__(self, other):
        return (isinstance(self, SModule) and isinstance(other, SModule)
                and hash(self) == hash(other))

    def __lt__(self, other):
        return (isinstance(self, SModule) and isinstance(other, SModule)
                and self.name < other.name)

    def __hash__(self):
        return self.id


@functools.total_ordering
class SModuleGroup(ReprMixin, Base):
    """
    The group identifying a SModule.
    """
    __tablename__ = "spansh_module_groups"
    _repr_keys = ['id', 'name']

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN["module_group"]))

    # Relationships
    modules = relationship(
        'SModule', cascade='save-update, delete, delete-orphan', back_populates='group', lazy='select'
    )

    @property
    def text(self):
        """ Alias for name. """
        return self.name

    def __eq__(self, other):
        return (isinstance(self, SModuleGroup) and isinstance(other, SModuleGroup)
                and hash(self) == hash(other))

    def __lt__(self, other):
        return (isinstance(self, SModuleGroup) and isinstance(other, SModuleGroup)
                and self.name < other.name)

    def __hash__(self):
        return self.id


class SModuleSold(ReprMixin, Base):
    """
    An entry represents a SModule that is sold at a Station.
    Updated_at can be found on station.
    """
    __tablename__ = 'spansh_modules_sold'
    __table_args__ = (
        UniqueConstraint('station_id', 'module_id', name='spansh_station_module_unique'),
    )
    _repr_keys = [
        'id', 'station_id', 'module_id',
    ]

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    station_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey("stations.id"), nullable=False)
    module_id = sqla.Column(sqla.Integer, sqla.ForeignKey("spansh_modules.id"), nullable=False)

    # Relationships
    module = relationship('SModule', uselist=False, viewonly=True, lazy='joined')

    def __eq__(self, other):
        return (isinstance(self, SModuleSold) and isinstance(other, SModuleSold)
                and hash(self) == hash(other))

    def __hash__(self):
        return hash(f'{self.station_id}_{self.module_id}')
