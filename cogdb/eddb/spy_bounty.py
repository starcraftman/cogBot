"""
Monitor the bounties in a given System.
"""
import re
import time

import sqlalchemy as sqla
from sqlalchemy.ext.hybrid import hybrid_property

import cogdb
from cogdb.eddb.common import Base, LEN, TWO_WEEK_SECONDS
from cogdb.eddb.ship import Ship
from cogdb.eddb.spy_vote import SpyVote
from cog.util import ReprMixin, TimestampMixin

BOUNTY_CATEGORY_MAP = {
    'faction': 1,  # Local faction crimes
    'power': 2,  # The power, i.e. Hudson
    'super': 3,  # The super, i.e. Fed / Imp / Alliance
}
# An event that will clean up older entries from bounties
EVENT_SPY_TOP5 = f"""
CREATE EVENT IF NOT EXISTS clean_spy_top5
ON SCHEDULE
    EVERY 1 DAY
COMMENT "Check daily for SpyBounty entries older than 14 days."
DO
    DELETE FROM eddb.spy_top5
    WHERE updated_at < (unix_timestamp() - {TWO_WEEK_SECONDS});
"""


class SpyBounty(ReprMixin, TimestampMixin, Base):
    """
    Track a single bounty entry in a given system for a power.
    The cmdr, ship and seen system and station will be kept.
    """
    __tablename__ = 'spy_top5'
    _repr_keys = [
        'id', 'category', 'system', 'pos', 'cmdr_name', 'ship_name',
        'last_seen_system', 'last_seen_station', 'bounty', 'ship_id', 'updated_at'
    ]

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    ship_id = sqla.Column(sqla.Integer, sqla.ForeignKey('ships.id'))
    power_id = sqla.Column(sqla.Integer, nullable=True)

    system = sqla.Column(sqla.String(LEN["system"]), nullable=False, default="")
    pos = sqla.Column(sqla.Integer, default=1)  # Should only be [1, 5]
    cmdr_name = sqla.Column(sqla.String(LEN["cmdr_name"]), nullable=False, default="")
    ship_name = sqla.Column(sqla.String(LEN["ship_name"]), nullable=False, default="")
    last_seen_system = sqla.Column(sqla.String(LEN["system"]), nullable=False, default="")
    last_seen_station = sqla.Column(sqla.String(LEN["station"]), nullable=False, default="")
    bounty = sqla.Column(sqla.BigInteger, default=0)
    category = sqla.Column(sqla.Integer, default=BOUNTY_CATEGORY_MAP['power'])
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    ship = sqla.orm.relationship(
        'Ship', uselist=False, lazy='joined', viewonly=True,
    )
    power = sqla.orm.relationship(
        'Power', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(Power.id) == SpyBounty.power_id',
    )

    def __str__(self):
        """ A pretty one line to give all information. """
        ship_text = self.ship.text if self.ship else str(self.ship_id)
        return f"""#{self.pos} {self.cmdr_name} last seen in {self.last_seen_system}/{self.last_seen_station} ({ship_text})
Has {self.bounty:,} in bounty, updated at {self.updated_date}"""

    def __eq__(self, other):
        return isinstance(other, SpyVote) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.id}_{self.pos}")

    @staticmethod
    def from_bounty_post(post, *, power_id=None, ship_map=None):
        """
        Generate a SpyBounty object from an existing bounty object.
        For empty entries (i.e. commanderId 0) will generate an effectively empty object

        Args:
            post: A dictionary with information from bounty post
            power_id: The power id of the system. If none, this is local
            ship_map: The map of ship names to Ship.ids

        Returns: A SpyBounty object.
        """
        name, ship_name, station, system, ship_id = '', '', '', '', None
        if not ship_map:
            ship_map = ship_type_to_id_map()

        if post['commanderId'] != 0:
            system = post['lastLocation']
            if ' - ' in system:
                system, station = system.split(' - ')

            name = post['name']
            mat = re.match(r'CMDR (.*) \((.*)\)', name)
            if mat:
                name = mat.group(1)
                parts = mat.group(2).split('"')
                if len(parts) == 3:
                    ship_name = parts[1]
                    try:
                        ship_id = ship_map[parts[0].strip()]
                    except KeyError:
                        pass

        kwargs = {
            'cmdr_name': name,
            'ship_name': ship_name,
            'ship_id': ship_id,
            'power_id': power_id,
            'last_seen_system': system,
            'last_seen_station': station,
            'system': post['system'],
            'pos': post['pos'],
            'bounty': post['value'],
            'category': BOUNTY_CATEGORY_MAP[post['category']],
        }
        if 'updated_at' in post:  # Mainly for testing, unsure if useful in production
            kwargs['updated_at'] = post['updated_at']

        return SpyBounty(**kwargs)

    @hybrid_property
    def is_faction(self):
        """
        Is the bounty for faction?
        """
        return self.category == BOUNTY_CATEGORY_MAP['faction']

    @hybrid_property
    def is_power(self):
        """
        Is the bounty for power (Hudson)?
        """
        return self.category == BOUNTY_CATEGORY_MAP['power']

    @hybrid_property
    def is_super(self):
        """
        Is the bounty for superpower (Federal)?
        """
        return self.category == BOUNTY_CATEGORY_MAP['super']


def ship_type_to_id_map(traffic_text=False):
    """
    Returns a simple map from ship type to the id in Ship table.
    """
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        ships = eddb_session.query(Ship).\
            all()
        attrname = 'traffic_text' if traffic_text else 'text'
        mapped = {getattr(ship, attrname): ship.id for ship in ships}

        try:
            del mapped[None]
        except KeyError:
            pass

        return mapped
