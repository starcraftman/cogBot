"""
History of commands issued by users of the bot towards the sheets tracking
important information such as fortification and undermining.
"""
import enum
import time

import sqlalchemy as sqla
from sqlalchemy.orm import relationship

from cogdb.schema.common import Base, LEN
import cog.tbl
import cog.util
from cog.util import ReprMixin, TimestampMixin


class ESheetType(enum.Enum):
    """ Type of sheet the transaction modified. """
    fort = 1
    um = 2
    snipe = 3


class SheetRecord(ReprMixin, TimestampMixin, Base):
    """
    A record of a single command a user entered to modify
    an important google sheet. Tracks the user issuing, the command
    the sheet direction and when it was flushed.
    """
    __tablename__ = 'history_sheet_transactions'
    _repr_keys = ['id', 'discord_id', 'channel_id', 'sheet_src', 'cycle', 'command',
                  'flushed_sheet', 'created_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    discord_id = sqla.Column(sqla.BigInteger, nullable=False)
    channel_id = sqla.Column(sqla.BigInteger, nullable=False)
    sheet_src = sqla.Column(sqla.Enum(ESheetType), default=ESheetType.fort)
    cycle = sqla.Column(sqla.Integer, default=cog.util.current_cycle)
    command = sqla.Column(sqla.String(LEN['command']), default="")
    flushed_sheet = sqla.Column(sqla.Boolean, default=False)
    created_at = sqla.Column(sqla.Integer, default=time.time)

    # Relationships
    user = relationship(
        'DiscordUser', uselist=False, viewonly=True, lazy='joined',
        primaryjoin='foreign(SheetRecord.discord_id) == DiscordUser.id'
    )

    def __eq__(self, other):
        return (isinstance(self, SheetRecord) and isinstance(other, SheetRecord)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)
