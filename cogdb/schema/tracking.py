"""
The initial tracking system for carriers.
"""
import datetime

import sqlalchemy as sqla

from cogdb.schema.common import Base, LEN
from cog.util import ReprMixin


EVENT_CARRIER = """
CREATE EVENT IF NOT EXISTS clean_carriers
ON SCHEDULE
    EVERY 1 DAY
COMMENT "If no updates in 4 days drop"
DO
    DELETE FROM {}.carriers_ids
    WHERE
        (DATEDIFF(NOW(), carriers_ids.updated_at) > 4)
            and
        (carriers_ids.override = 0)
"""
TRACK_SYSTEM_SEP = ", "


class TrackSystem(ReprMixin, Base):
    """
    Track a system for carriers.
    """
    __tablename__ = 'carriers_systems'
    _repr_keys = ['system', 'distance']

    system = sqla.Column(sqla.String(LEN['name']), primary_key=True)
    distance = sqla.Column(sqla.Integer, default=15, nullable=False)

    def __str__(self):
        return f"Tracking systems <= {self.distance}ly from {self.system}"

    def __eq__(self, other):
        return isinstance(other, TrackSystem) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.system}")


class TrackSystemCached(ReprMixin, Base):
    """
    Computed systems that are the total coverage of TrackSystem directives.
    This set of system names is recomputed on every addition or removal.
    """
    __tablename__ = 'carriers_systems_cached'
    _repr_keys = ['system', 'overlaps_with']

    system = sqla.Column(sqla.String(LEN['name']), primary_key=True)
    overlaps_with = sqla.Column(sqla.String(LEN['reason']), default="", nullable=False)

    def __eq__(self, other):
        return isinstance(other, TrackSystemCached) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.system}")

    def add_overlap(self, centre):
        """
        Add the overlap to this system.
        """
        if self.overlaps_with:
            self.overlaps_with += TRACK_SYSTEM_SEP
        self.overlaps_with += centre

    def remove_overlap(self, centre):
        """
        Remove the overlap to this system.

        Returns: True if this object should now be deleted.
        """
        centres = self.overlaps_with.split(TRACK_SYSTEM_SEP)
        centres = [x for x in centres if x.lower() != centre.lower()]
        self.overlaps_with = TRACK_SYSTEM_SEP.join(centres)

        return self.overlaps_with == ""


# TODO: This table may be deprecated now considering eddb.CarrierSighting
class TrackByID(ReprMixin, Base):
    """
    Track where a carrier is by storing id and last known system.
    """
    __tablename__ = 'carriers_ids'
    _repr_keys = ['id', 'squad', 'system', 'last_system', 'override', 'updated_at']

    header = ["ID", "Squad", "System", "Last System"]

    id = sqla.Column(sqla.String(LEN['carrier']), primary_key=True)
    squad = sqla.Column(sqla.String(LEN['name']), default="")
    system = sqla.Column(sqla.String(LEN['name']), default="")
    last_system = sqla.Column(sqla.String(LEN['name']), default="")
    # This flag indicates user requested this ID ALWAYS be tracked, regardless of location.
    override = sqla.Column(sqla.Boolean, default=False)
    updated_at = sqla.Column(sqla.DateTime, default=datetime.datetime.utcnow, index=True)  # All dates UTC

    track_system = sqla.orm.relationship(
        'TrackSystemCached', uselist=False, viewonly=True,
        primaryjoin="foreign(TrackByID.system) == TrackSystemCached.system"
    )

    def __str__(self):
        """ A pretty one line to give all information. """
        overlaps = ""
        if self.track_system:
            overlaps = f", near {self.track_system.overlaps_with}"
        info = {
            'squad': self.squad if self.squad else "No Group",
            'system': self.system if self.system else "No Info",
            'last_system': self.last_system if self.last_system else "No Info",
        }

        return f"{self.id} [{info['squad']}] jumped {info['last_system']} => {info['system']}{overlaps}"

    def __eq__(self, other):
        return isinstance(other, TrackByID) and hash(self) == hash(other)

    def __hash__(self):
        return hash(self.id)

    def table_line(self):
        """ Returns a line for table formatting. """
        return (self.id, self.squad, self.system, self.last_system)

    def spotted(self, new_system):
        """
        Tracked has been spotted in a new system. Update accordingly.
        """
        self.last_system = self.system
        self.system = new_system
