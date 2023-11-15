"""
Global configuration flags for this database.
"""
import datetime

import sqlalchemy as sqla
from sqlalchemy.orm import validates

from cogdb.schema.common import Base

import cog.tbl
import cog.util
from cog.util import ReprMixin


class Global(ReprMixin, Base):
    """
    A simple storage table for any globals per cycle.
    """
    __tablename__ = 'globals'
    _repr_keys = ['id', 'cycle', 'consolidation']

    id = sqla.Column(sqla.Integer, primary_key=True)
    cycle = sqla.Column(sqla.Integer, default=0)
    consolidation = sqla.Column(sqla.Integer, default=0)  # FIXME: Deprecated, remove in future from db.
    show_almost_done = sqla.Column(sqla.Boolean, default=False)
    show_vote_goal = sqla.Column(sqla.Boolean, default=False)
    vote_goal = sqla.Column(sqla.Integer, default=0)
    updated_at = sqla.Column(sqla.DateTime(timezone=False), default=datetime.datetime.utcnow)  # All dates UTC

    def __str__(self):
        """ A pretty one line to give all information. """
        return f"Cycle {self.cycle}: Consolidation Vote: {self.consolidation}%"

    def __eq__(self, other):
        return isinstance(other, Global) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.cycle}")

    def update(self, **kwargs):
        """
        Update the object with expected kwargs.

        kwargs:
            cycle: The current cycle number.
            consolidation: The consolidation % of the curent vote.
            show_almost_done: Manual override to show almost done.
            updated_at: The new date time to set for this update. (Required)

        Raises:
            ValidationFail - The kwargs did not contain updated_at or it was not suitable.
        """
        if 'updated_at' not in kwargs:
            raise cog.exc.ValidationFail("Expected key 'updated_at' is missing.")

        self.updated_at = kwargs['updated_at']
        for key in ['cycle', 'consolidation']:
            try:
                setattr(self, key, kwargs[key])
            except (KeyError, cog.exc.ValidationFail):
                pass

    @validates('cycle')
    def validate_cycle(self, key, value):
        """ Validation function for cycle. """
        try:
            if value < 1:
                raise cog.exc.ValidationFail(f"Bounds check failed for: {key} with value {value}")
        except TypeError:
            pass

        return value

    @validates('consolidation')
    def validate_consolidation(self, key, value):
        """ Validation function for consolidation. """
        try:
            if value < 0 or value > 100:
                raise cog.exc.ValidationFail(f"Bounds check failed for: {key} with value {value}")
        except TypeError:
            pass

        return value

    @validates('vote_goal')
    def validate_vote_goal(self, key, value):
        """ Validation function for vote_goal. """
        try:
            if value < 0 or value > 100:
                raise cog.exc.ValidationFail(f"Bounds check failed for: {key} with value {value}")
        except TypeError:
            pass

        return value

    @validates('updated_at')
    def validate_updated_at(self, _, value):
        """ Validation function for updated_at. """
        if not value or not isinstance(value, datetime.datetime) or (self.updated_at and value < self.updated_at):
            raise cog.exc.ValidationFail("Date invalid or was older than current value.")

        return value
