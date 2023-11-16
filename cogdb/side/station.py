"""
Mapping of Station and StationType for remote side DB.
"""
import sqlalchemy as sqla

from cogdb.eddb.common import LEN
from cogdb.side.common import Base
from cog.util import ReprMixin


class Station(ReprMixin, Base):
    """ Represents a station in a system. """
    __tablename__ = "stations"
    _repr_keys = [
        'id', 'name', 'distance_to_star', 'system_id', 'station_type_id',
        'settlement_size_id', 'settlement_security_id', 'controlling_faction_id', 'updated_at'
    ]

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN["station"]))
    updated_at = sqla.Column(sqla.Integer)
    distance_to_star = sqla.Column(sqla.Integer)
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'))
    station_type_id = sqla.Column(sqla.Integer, sqla.ForeignKey('station_type.id'))
    settlement_size_id = sqla.Column(sqla.Integer, sqla.ForeignKey('settlement_size.id'),)
    settlement_security_id = sqla.Column(sqla.Integer, sqla.ForeignKey('settlement_security.id'),)
    controlling_faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'))

    def __eq__(self, other):
        return (isinstance(self, Station) and isinstance(other, Station)
                and self.id == other.id)


class StationType(ReprMixin, Base):
    """ The type of a station. """
    __tablename__ = "station_type"
    _repr_keys = ['id', 'text']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["station_type"]))

    def __eq__(self, other):
        return (isinstance(self, StationType) and isinstance(other, StationType)
                and self.id == other.id)
