"""
EDDB New Commodity tables, based on spansh dump.
"""
import functools
import sqlalchemy as sqla
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint

from cogdb.eddb.common import Base, LEN
from cog.util import ReprMixin


@functools.total_ordering
class SCommodity(ReprMixin, Base):
    """
    A spansh commodity that has a name and group as well
    as a mean_price and eddn names that identify it.
    """
    __tablename__ = 'spansh_commodities'
    _repr_keys = ['id', 'group_id', "name", "eddn", "eddn2", "mean_price"]

    id = sqla.Column(sqla.Integer, primary_key=True)  # commodityId
    group_id = sqla.Column(sqla.Integer, sqla.ForeignKey("spansh_commodity_groups.id"), nullable=False)
    name = sqla.Column(sqla.String(LEN["commodity"]))
    eddn = sqla.Column(sqla.String(LEN["commodity"]))
    eddn2 = sqla.Column(sqla.String(LEN["commodity"]))
    mean_price = sqla.Column(sqla.Integer, default=0)

    # Relationships
    group = relationship(
        'SCommodityGroup', uselist=False, back_populates='commodities', lazy='joined'
    )

    @property
    def text(self):
        """ Alias for name. """
        return self.name

    def __eq__(self, other):
        return (isinstance(self, SCommodity) and isinstance(other, SCommodity)
                and hash(self) == hash(other))

    def __lt__(self, other):
        return (isinstance(self, SCommodity) and isinstance(other, SCommodity)
                and self.name < other.name)

    def __hash__(self):
        return self.id


@functools.total_ordering
class SCommodityGroup(ReprMixin, Base):
    """
    A group identifying related SCommodity objects.
    """
    __tablename__ = "spansh_commodity_groups"
    _repr_keys = ['id', 'name']

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN["commodity_group"]))

    # Relationships
    commodities = relationship(
        'SCommodity', cascade='save-update, delete, delete-orphan', back_populates='group', lazy='select'
    )

    @property
    def text(self):
        """ Alias for name. """
        return self.name

    def __eq__(self, other):
        return (isinstance(self, SCommodityGroup) and isinstance(other, SCommodityGroup)
                and hash(self) == hash(other))

    def __lt__(self, other):
        return (isinstance(self, SCommodityGroup) and isinstance(other, SCommodityGroup)
                and self.name < other.name)

    def __hash__(self):
        return self.id


class SCommodityPricing(ReprMixin, Base):
    """
    The pricing information of a single SCommodity sold at a particular Station.
    Updated_at timestmap can be found on station, all commodities should be updated at same time as station.
    The demand, supply, buy and sell price are available.
    """
    __tablename__ = 'spansh_commodity_pricing'
    __table_args__ = (
        UniqueConstraint('station_id', 'commodity_id', name='spansh_station_commodity_unique'),
    )
    _repr_keys = [
        'id', 'station_id', 'commodity_id', "demand", "supply", "buy_price", "sell_price"
    ]

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    station_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey("stations.id"), nullable=False)
    commodity_id = sqla.Column(sqla.Integer, sqla.ForeignKey("spansh_commodities.id"), nullable=False)

    demand = sqla.Column(sqla.Integer, default=0)
    supply = sqla.Column(sqla.Integer, default=0)
    buy_price = sqla.Column(sqla.Integer, default=0)
    sell_price = sqla.Column(sqla.Integer, default=0)

    # Relationships
    commodity = relationship('SCommodity', uselist=False, viewonly=True, lazy='joined')

    @property
    def text(self):
        """ Alias for name. """
        return self.commodity.name

    def __eq__(self, other):
        return (isinstance(self, SCommodityPricing) and isinstance(other, SCommodityPricing)
                and hash(self) == hash(other))

    def __hash__(self):
        return hash(f'{self.station_id}_{self.commodity_id}')
