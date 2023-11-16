"""
Monitor the traffic of different ships entering a given System.
"""
import time

import sqlalchemy as sqla

from cogdb.eddb.common import Base, LEN, TWO_WEEK_SECONDS
from cog.util import ReprMixin

EVENT_SPY_TRAFFIC = f"""
CREATE EVENT IF NOT EXISTS clean_spy_traffic
ON SCHEDULE
    EVERY 1 DAY
COMMENT "Check daily for SpyTraffic entries older than 14 days."
DO
    DELETE FROM eddb.spy_traffic
    WHERE updated_at < (unix_timestamp() - {TWO_WEEK_SECONDS});
"""


class SpyTraffic(ReprMixin, Base):
    """
    Represents the amount of Ship reported as passing into a System at the given updated_at timestamp.
    """
    __tablename__ = 'spy_traffic'
    _repr_keys = ['id', 'system', 'ship_id', 'cnt', 'updated_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    ship_id = sqla.Column(sqla.Integer, sqla.ForeignKey('ships.id'))
    cnt = sqla.Column(sqla.Integer)
    system = sqla.Column(sqla.String(LEN["system"]), nullable=False, default="")
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    ship = sqla.orm.relationship(
        'Ship', uselist=False, lazy='joined', viewonly=True,
    )

    def __str__(self):
        """ A pretty one line to give all information. """
        ship_text = self.ship.text if self.ship else str(self.ship_id)
        return f"{self.system} {ship_text}: {self.cnt}"

    def __eq__(self, other):
        return isinstance(other, SpyTraffic) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.id}")
