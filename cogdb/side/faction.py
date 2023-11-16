"""
Mapping of Faction, FactionHistory and FactionState for remote side DB.
"""
import sqlalchemy as sqla

from cogdb.eddb.common import LEN
from cogdb.side.common import Base
from cog.util import ReprMixin


class Faction(ReprMixin, Base):
    """ Information about a faction. """
    __tablename__ = "factions"
    _repr_keys = ['id', 'name', 'state_id', 'government_id', 'allegiance_id', 'home_system',
                  'is_player_faction', 'updated_at']

    id = sqla.Column(sqla.Integer, primary_key=True)
    updated_at = sqla.Column(sqla.Integer)
    name = sqla.Column(sqla.String(LEN["faction"]))
    home_system = sqla.Column(sqla.Integer)
    is_player_faction = sqla.Column(sqla.Integer)
    state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_state.id'))
    government_id = sqla.Column(sqla.Integer, sqla.ForeignKey('gov_type.id'))
    allegiance_id = sqla.Column(sqla.Integer, sqla.ForeignKey('allegiance.id'))

    def __eq__(self, other):
        return isinstance(self, Faction) and isinstance(other, Faction) and self.id == other.id


class FactionHistory(ReprMixin, Base):
    """ Historical information about a faction. """
    __tablename__ = "factions_history"
    _repr_keys = ['id', 'name', 'state_id', 'government_id', 'allegiance_id', 'home_system',
                  'is_player_faction', 'updated_at']

    id = sqla.Column(sqla.Integer, primary_key=True)
    updated_at = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN["faction"]))
    home_system = sqla.Column(sqla.Integer)
    is_player_faction = sqla.Column(sqla.Integer)
    state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_state.id'))
    government_id = sqla.Column(sqla.Integer, sqla.ForeignKey('gov_type.id'))
    allegiance_id = sqla.Column(sqla.Integer, sqla.ForeignKey('allegiance.id'))

    def __eq__(self, other):
        return isinstance(self, Faction) and isinstance(other, Faction) and self.id == other.id


class FactionState(ReprMixin, Base):
    """ The state a faction is in. """
    __tablename__ = "faction_state"
    _repr_keys = ['id', 'text', 'eddn']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(LEN["faction_state"]))
    eddn = sqla.Column(sqla.String(LEN["eddn"]))

    def __eq__(self, other):
        return (isinstance(self, FactionState) and isinstance(other, FactionState)
                and self.id == other.id)
