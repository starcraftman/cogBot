"""
Mapping of Influence and InfluenceHistory for remote side DB.
"""
import datetime

import sqlalchemy as sqla

from cogdb.eddb.common import LEN
from cogdb.side.common import Base
from cog.util import ReprMixin


class Influence(ReprMixin, Base):
    """ Represents influence of a faction in a system. """
    __tablename__ = "influence"
    _repr_keys = ['system_id', 'faction_id', 'state_id', 'pending_state_id', 'influence', 'is_controlling_faction',
                  'updated_at']

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), primary_key=True)
    influence = sqla.Column(sqla.Numeric(7, 4, None, False))
    is_controlling_faction = sqla.Column(sqla.Integer)
    updated_at = sqla.Column(sqla.Integer)
    state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_state.id'))
    pending_state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_state.id'))

    def __eq__(self, other):
        return (isinstance(self, Influence) and isinstance(other, Influence)
                and self.system_id == other.system_id
                and self.faction_id == other.faction_id)

    @property
    def date(self):
        """ Get the datetime object of the timestamp. """
        return datetime.datetime.fromtimestamp(self.updated_at)

    @property
    def short_date(self):
        """ Get a short day/month representation of timestamp. """
        return f'{self.date.day}/{self.date.month}'


class InfluenceHistory(ReprMixin, Base):
    """ Represents influence of a faction in a system. """
    __tablename__ = "influence_history"
    _repr_keys = ['system_id', 'faction_id', 'state_id', 'pending_state_id', 'influence', 'is_controlling_faction',
                  'updated_at']

    system_id = sqla.Column(sqla.Integer, primary_key=True)
    faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), primary_key=True)
    influence = sqla.Column(sqla.Numeric(7, 4, None, False))
    is_controlling_faction = sqla.Column(sqla.Integer)
    updated_at = sqla.Column(sqla.Integer, primary_key=True)
    state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_state.id'))
    pending_state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('faction_state.id'))

    def __eq__(self, other):
        return (isinstance(self, Influence) and isinstance(other, Influence)
                and self.system_id == other.system_id and self.faction_id == other.faction_id)

    @property
    def date(self):
        """ Get the datetime object of the timestamp. """
        return datetime.datetime.fromtimestamp(self.updated_at)

    @property
    def short_date(self):
        """ Get a short day/month representation of timestamp. """
        return f'{self.date.day}/{self.date.month}'
