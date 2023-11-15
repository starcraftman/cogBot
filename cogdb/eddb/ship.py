"""
EDDB Ship and ShipSold tables.
"""
import sqlalchemy as sqla
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint

from cogdb.eddb.common import Base, LEN
from cog.util import ReprMixin, UpdatableMixin


class Ship(ReprMixin, Base):
    """
    The types of ships available in game.
    """
    __tablename__ = 'ships'
    _repr_keys = ['id', 'text', 'traffic_text']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["ship"]))
    traffic_text = sqla.Column(sqla.String(LEN["ship"]))
    eddn = sqla.Column(sqla.String(LEN["ship"]))

    def __str__(self):
        """ A pretty one line to give all information. """
        return f"Ship: {self.text}"

    def __eq__(self, other):
        return isinstance(other, Ship) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.id}")


class ShipSold(ReprMixin, UpdatableMixin, Base):
    """
    Represents a Ship sold at a Station.
    """
    __tablename__ = 'station_ships_sold'
    __table_args__ = (
        UniqueConstraint('station_id', 'ship_id', name='station_ship_sold_unique'),
    )
    _repr_keys = [
        'id', 'station_id', 'ship_id',
    ]

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    station_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey("stations.id"), nullable=False)
    ship_id = sqla.Column(sqla.Integer, sqla.ForeignKey("ships.id"), nullable=False)

    ship = relationship('Ship', uselist=False, viewonly=True, lazy='joined')
    station = relationship('Station', uselist=False, viewonly=True, lazy='joined')

    def __eq__(self, other):
        return (isinstance(self, ShipSold) and isinstance(other, ShipSold)
                and hash(self) == hash(other))

    def __hash__(self):
        return hash(f'{self.station_id}_{self.ship_id}')
