"""
Global configuration flags for this database.
"""
import datetime
import enum

import sqlalchemy as sqla
from sqlalchemy.orm import validates

from cogdb.schema.common import Base
import cog.tbl
import cog.util
from cog.util import ReprMixin


MAX_VOTE_VALUE = 50


class EVoteType(enum.Enum):
    """
    Type of Vote of a user.
        cons - A vote for power consolidation.
        prep - A vote for power preparation.
    """
    cons = 1
    prep = 2


class Vote(ReprMixin, Base):
    """
    Store Vote of a given DiscordUser (id) during the current cycle.
    """
    __tablename__ = 'powerplay_votes'
    _repr_keys = ['id', 'vote', 'amount', 'updated_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    vote = sqla.Column(sqla.Enum(EVoteType), default=EVoteType.cons, primary_key=True)
    amount = sqla.Column(sqla.Integer, default=0)
    updated_at = sqla.Column(sqla.DateTime, default=datetime.datetime.utcnow)  # All dates UTC

    # Relationships
    discord_user = sqla.orm.relationship(
        'DiscordUser', uselist=False, viewonly=True,
        primaryjoin='foreign(Vote.id) == DiscordUser.id'
    )

    def __str__(self):
        """ A pretty one line to give all information. """
        name = self.discord_user.display_name if self.discord_user else "You"
        return f"**{name}**: voted {self.amount} {self.vote_type}."

    def __eq__(self, other):
        return isinstance(other, Vote) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.id}-{self.vote}")

    @property
    def vote_type(self):
        """ Convenience to convert the enum into a string representation. """
        return str(self.vote).split('.', maxsplit=1)[-1].capitalize()

    def update_amount(self, amount):
        """
        Update the object with new amount.
        """
        self.amount += int(amount)
        self.updated_at = datetime.datetime.utcnow()
        return self

    @validates('updated_at')
    def validate_updated_at(self, _, value):
        """ Validation function for updated_at. """
        if not value or not isinstance(value, datetime.datetime) or (self.updated_at and value < self.updated_at):
            raise cog.exc.ValidationFail("Date invalid or was older than current value.")

        return value

    @validates('amount')
    def validate_amount(self, key, value):
        """ Validation function for amount. """
        try:
            if value < 0 or value > MAX_VOTE_VALUE:
                raise cog.exc.ValidationFail(f"Bounds check failed for: {key} with value {value}")
        except TypeError:
            pass

        return value
