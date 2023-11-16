"""
Store all discord users that have used this bot.
Can be associated with Fort and UM objects as well as SheetRecords for more information.
"""
import sqlalchemy as sqla
from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_property

from cogdb.schema.common import Base, LEN
from cog.util import ReprMixin


class DiscordUser(ReprMixin, Base):
    """
    A DiscordUser is a member of the guild that uses any command on this bot.
    Store the display_name on creation of this record and their preferred
    name and cry for sheets.
    This object acts as a main linkage point to request records in other tables.
    """
    __tablename__ = 'discord_users'
    _repr_keys = ['id', 'display_name', 'pref_name', 'pref_cry']

    id = sqla.Column(sqla.BigInteger, primary_key=True)  # Discord id
    display_name = sqla.Column(sqla.String(LEN['name']))  # FIXME: Remove this
    pref_name = sqla.Column(sqla.String(LEN['name']), index=True, nullable=False)  # pref_name == display_name until change
    pref_cry = sqla.Column(sqla.String(LEN['name']), default='')

    # Relationships
    fort_user = relationship(
        'FortUser', uselist=False, viewonly=True,
        primaryjoin='foreign(DiscordUser.pref_name) == FortUser.name'
    )
    fort_merits = relationship(
        'FortDrop', lazy='select', uselist=True, viewonly=True,
        primaryjoin='and_(foreign(DiscordUser.pref_name) == remote(FortUser.name), foreign(FortUser.id) == FortDrop.user_id)'
    )
    um_user = relationship(
        'UMUser', uselist=False, viewonly=True,
        primaryjoin="and_(foreign(DiscordUser.pref_name) == UMUser.name, UMUser.sheet_src == 'main')"
    )
    um_merits = relationship(
        'UMHold', lazy='select', uselist=True, viewonly=True,
        primaryjoin="and_(foreign(DiscordUser.pref_name) == remote(UMUser.name), foreign(UMUser.id) == UMHold.user_id, UMUser.sheet_src == 'main')"
    )
    snipe_user = relationship(
        'UMUser', uselist=False, viewonly=True,
        primaryjoin="and_(foreign(DiscordUser.pref_name) == UMUser.name, UMUser.sheet_src == 'snipe')"
    )
    snipe_merits = relationship(
        'UMHold', lazy='select', uselist=True, viewonly=True,
        primaryjoin="and_(foreign(DiscordUser.pref_name) == remote(UMUser.name), foreign(UMUser.id) == UMHold.user_id, UMUser.sheet_src == 'snipe')"
    )

    def __eq__(self, other):
        return isinstance(other, DiscordUser) and self.id == other.id

    def __hash__(self):
        return hash(self.id)

    @hybrid_property
    def mention(self):
        """ Mention this user in a response. """
        return f"<@{self.id}>"

    @mention.expression
    def mention(cls):
        """ Mention this user in a response. """
        return sqla.func.concat("<@", sqla.func.cast(cls.id, sqla.String), ">")

    @hybrid_property
    def total_merits(self):
        """ The total merits a user has done this cycle. """
        return self.fort_user.dropped + self.um_user.held + self.um_user.redeemed
