"""
EDDB Commodity, based on eddb.io
"""
import time

import sqlalchemy as sqla
from sqlalchemy.orm import relationship

from cogdb.eddb.common import Base, LEN
from cog.util import ReprMixin, UpdatableMixin


class Commodity(ReprMixin, UpdatableMixin, Base):
    """ A commodity sold at a station. """
    __tablename__ = 'eddb_commodities'
    _repr_keys = [
        'id', 'category_id', "name", "average_price", "is_rare", "is_non_marketable",
        "max_buy_price", "max_sell_price", "min_buy_price", "min_sell_price", "updated_at"
    ]

    id = sqla.Column(sqla.Integer, primary_key=True)
    category_id = sqla.Column(sqla.Integer,
                              sqla.ForeignKey("eddb_commodity_categories.id"), nullable=False)

    name = sqla.Column(sqla.String(LEN["commodity"]))
    average_price = sqla.Column(sqla.Integer, default=0)
    min_buy_price = sqla.Column(sqla.Integer, default=0)
    min_sell_price = sqla.Column(sqla.Integer, default=0)
    max_buy_price = sqla.Column(sqla.Integer, default=0)
    max_sell_price = sqla.Column(sqla.Integer, default=0)
    is_non_marketable = sqla.Column(sqla.Boolean, default=False)
    is_rare = sqla.Column(sqla.Boolean, default=False)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    category = relationship(
        'CommodityCat', uselist=False, back_populates='commodities', lazy='select'
    )

    def __eq__(self, other):
        return (isinstance(self, Commodity) and isinstance(other, Commodity)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class CommodityCat(ReprMixin, Base):
    """ The category for a commodity """
    __tablename__ = "eddb_commodity_categories"
    _repr_keys = ['id', 'name']

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN["commodity_category"]))

    # Relationships
    commodities = relationship(
        'Commodity', cascade='all, delete, delete-orphan', back_populates='category', lazy='select'
    )

    def __eq__(self, other):
        return (isinstance(self, CommodityCat) and isinstance(other, CommodityCat)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)
