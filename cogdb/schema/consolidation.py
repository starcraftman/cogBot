"""
Track the current consolidation of the power and vote totals.
"""
import datetime

import sqlalchemy as sqla
from sqlalchemy.orm import validates

from cogdb.schema.common import Base
import cog.tbl
import cog.util
from cog.util import ReprMixin


class Consolidation(ReprMixin, Base):
    """
    The consolidation vote for the faction power and the total
    votes by members for consolidation and prep tracked at a given moment in time.
    These points can be used to track impact of votes and when they impact the overal consolidation.
    """
    __tablename__ = 'consolidation_tracker'
    _repr_keys = ['id', 'cycle', 'amount', 'cons_total', 'prep_total', 'updated_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    amount = sqla.Column(sqla.Integer, default=0)
    cons_total = sqla.Column(sqla.Integer, default=0)
    prep_total = sqla.Column(sqla.Integer, default=0)
    updated_at = sqla.Column(sqla.DateTime, default=datetime.datetime.utcnow, unique=True)  # All dates UTC

    def __str__(self):
        """ A pretty one line to give all information. """
        return f"Consolidation {self.amount}% at {self.updated_at}."

    def __eq__(self, other):
        return isinstance(other, Consolidation) and hash(self) == hash(other)

    def __hash__(self):
        return hash(self.id)

    @validates('cons_total', 'prep_total')
    def validate_totals(self, key, value):
        """ Validation function for cons_total and prep_total. """
        try:
            if value < 0:
                raise cog.exc.ValidationFail(f"Bounds check failed for: {key} with value {value}")
        except TypeError:
            pass

        return value

    @validates('amount')
    def validate_amount(self, key, value):
        """ Validation function for amount. """
        try:
            if value < 0 or value > 100:
                raise cog.exc.ValidationFail(f"Bounds check failed for: {key} with value {value}")
        except TypeError:
            pass

        return value
