"""
EDDB Station and related tables.
"""
import time

import sqlalchemy as sqla
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint

from cogdb.eddb.common import Base, LEN
from cog.util import ReprMixin, TimestampMixin, UpdatableMixin


class Station(ReprMixin, TimestampMixin, UpdatableMixin, Base):
    """
    Represents a single station in game.
    A station can be one of two major types:
        A player carrier, who can travel between systems and has a unique name.
        A stationary base that has a unique name in system, but that name may be reused elsewhere in the universe.

    Stations can have a series of:
        StationFeatures - describing the features on a station such as material traders and brokers.

    """
    __tablename__ = "stations"
    __table_args__ = (
        UniqueConstraint('name', 'system_id', name='station_name_system_id_unique'),
    )
    _repr_keys = [
        'id', 'name', 'distance_to_star', 'max_landing_pad_size',
        'type_id', 'system_id', 'controlling_minor_faction_id', 'updated_at'
    ]

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    controlling_minor_faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'))
    system_id = sqla.Column(sqla.Integer)
    type_id = sqla.Column(sqla.Integer, nullable=False)

    name = sqla.Column(sqla.String(LEN["station"]), index=True)
    distance_to_star = sqla.Column(sqla.Integer, default=0)
    is_planetary = sqla.Column(sqla.Boolean, default=False)
    max_landing_pad_size = sqla.Column(sqla.String(LEN["station_pad"]))
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    features = relationship('StationFeatures', uselist=False, viewonly=True)
    type = relationship(
        'StationType', uselist=False, viewonly=True,
        primaryjoin='foreign(Station.id) == remote(StationType.id)',
    )
    station_economies = relationship(
        'StationEconomy', uselist=True, viewonly=True, lazy='select',
        primaryjoin='foreign(Station.id) == remote(StationEconomy.id)',
    )
    economies = relationship(
        'Economy', uselist=True, viewonly=True, lazy='select',
        primaryjoin='and_(foreign(Station.id) == remote(StationEconomy.id), foreign(StationEconomy.economy_id) == Economy.id)',
    )
    faction = relationship('Faction', uselist=False, viewonly=True)
    allegiance = relationship(
        'Allegiance', uselist=False, viewonly=True, lazy='select',
        primaryjoin='and_(Station.controlling_minor_faction_id == remote(Faction.id), foreign(Faction.allegiance_id) == foreign(Allegiance.id))',
    )
    government = relationship(
        'Government', uselist=False, viewonly=True, lazy='select',
        primaryjoin='and_(Station.controlling_minor_faction_id == remote(Faction.id), foreign(Faction.government_id) == foreign(Government.id))',
    )
    system = relationship(
        'System', back_populates='stations', lazy='select',
        primaryjoin='System.id == foreign(Station.system_id)',
    )

    def __eq__(self, other):
        return isinstance(self, Station) and isinstance(other, Station) and self.id == other.id

    def __hash__(self):
        return hash(self.id)

    @classmethod
    def carrier(cls, *, name, station_id, system_id, distance_to_star=0):
        """
        Factory method to create player carrier stations.
        All carriers are stations but have the following specific or constant fields:
            name is unique globally and of form WWW-WWW where W is alphanumeric
            controlling_minor_faction_id = 77170
            type_id = 24
            is_planetary = False
            max_landing_pad_size = "L" (carriers have 8 L, 4 M and 4 S)

        Args:
            cls: This class.
            name: The name of the station.
            station_id: The id of the the carrier.
            system_id: The id of the system the carrier is currently seen in.
            distance_to_star: The distance from the star the carrier was observed at.

        Returns: A Station object preconfigured with constants.
        """
        return cls(**{
            'name': name,
            'id': station_id,
            'system_id': system_id,
            'distance_to_star': distance_to_star,
            'controlling_minor_faction_id': 77170,
            'type_id': 24,
            'is_planetary': False,
            'max_landing_pad_size': "L",
        })


class StationFeatures(ReprMixin, TimestampMixin, UpdatableMixin, Base):
    """ The features at a station. """
    __tablename__ = "station_features"
    _repr_keys = [
        'id', 'apexinterstellar', 'blackmarket', 'carriermanagement', 'carriervendor',
        'commodities', 'dock', 'engineer', 'market', 'materialtrader', 'outfitting',
        'rearm', 'refuel', 'repair', 'shipyard', 'techBroker',
        'universal_cartographics', 'updated_at'
    ]

    id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('stations.id'), primary_key=True)

    apexinterstellar = sqla.Column(sqla.Boolean)
    blackmarket = sqla.Column(sqla.Boolean)
    carriermanagement = sqla.Column(sqla.Boolean)
    carriervendor = sqla.Column(sqla.Boolean)
    commodities = sqla.Column(sqla.Boolean)
    dock = sqla.Column(sqla.Boolean)
    engineer = sqla.Column(sqla.Boolean)
    market = sqla.Column(sqla.Boolean)
    materialtrader = sqla.Column(sqla.Boolean)
    outfitting = sqla.Column(sqla.Boolean)
    rearm = sqla.Column(sqla.Boolean)
    refuel = sqla.Column(sqla.Boolean)
    repair = sqla.Column(sqla.Boolean)
    shipyard = sqla.Column(sqla.Boolean)
    techBroker = sqla.Column(sqla.Boolean)
    universal_cartographics = sqla.Column(sqla.Boolean)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Realtionships
    station = relationship('Station', uselist=False, viewonly=True)

    @staticmethod
    def kwargs(station_id, updated_at):
        """
        Simple function to create a kwargs object
        where all features are false. To be updated by looking at current station services.

        Returns: A dictionary of kwargs for a StationFeatures object.
        """
        return {
            'id': station_id,
            'apexinterstellar': False,
            'blackmarket': False,
            'carriermanagement': False,
            'carriervendor': False,
            'commodities': False,
            'dock': False,
            'engineer': False,
            'market': False,
            'materialtrader': False,
            'outfitting': False,
            'rearm': False,
            'refuel': False,
            'repair': False,
            'shipyard': False,
            'techBroker': False,
            'universal_cartographics': False,
            'updated_at': updated_at,
        }

    def __eq__(self, other):
        return (isinstance(self, StationFeatures) and isinstance(other, StationFeatures)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class StationType(ReprMixin, Base):
    """ The type of a station, like Outpost and so on. """
    __tablename__ = "station_types"
    _repr_keys = ['id', 'text', 'eddn', 'is_planetary', 'max_landing_pad_size']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["station_type"]))
    eddn = sqla.Column(sqla.String(LEN["station_type"]))
    is_planetary = sqla.Column(sqla.Boolean, default=False)
    max_landing_pad_size = sqla.Column(sqla.String(LEN["station_pad"]))

    def __eq__(self, other):
        return (isinstance(self, StationType) and isinstance(other, StationType)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


class StationEconomy(ReprMixin, Base):
    """ The economy of a station, can have up to 2 usually. """
    __tablename__ = "station_economies"
    _repr_keys = ['id', 'economy_id', 'primary', 'proportion']

    id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('stations.id'), primary_key=True)
    economy_id = sqla.Column(sqla.Integer, sqla.ForeignKey('economies.id'), primary_key=True)
    primary = sqla.Column(sqla.Boolean, primary_key=True, default=False)
    proportion = sqla.Column(sqla.Float)

    def __eq__(self, other):
        return (isinstance(self, StationEconomy) and isinstance(other, StationEconomy)
                and self.__hash__() == other.__hash__())

    def __hash__(self):
        return hash(f"{self.id}_{self.economy_id}")


class CarrierSighting(ReprMixin, TimestampMixin, Base):
    """ Repesents a carrier sighting in the universe. """
    __tablename__ = "carrier_sightings"
    __table_args__ = (
        UniqueConstraint('carrier_id', 'system_id', 'created_at', name='carrier_id_system_id_created_at_unique'),
    )
    _repr_keys = [
        'id', 'system_id', 'distance_to_star', 'created_at'
    ]

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    carrier_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('stations.id'))
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'))
    distance_to_star = sqla.Column(sqla.Integer)
    created_at = sqla.Column(sqla.Integer, default=time.time)

    # Relationships
    carrier = relationship('Station', uselist=False, viewonly=True)
    system = relationship('System', uselist=False, viewonly=True)

    def __eq__(self, other):
        return isinstance(self, CarrierSighting) and isinstance(other, CarrierSighting) and self.id == other.id

    def __hash__(self):
        return hash(self.id)
