"""
Permissions for bot commands are managed here.

AdminPerm   - Grant administrative permissions to a given user.
ChannelPerm - Grant a command only in a selected channel.
RolePerm    - Grant a command only to those with a given role.
"""
import datetime

import sqlalchemy as sqla

from cogdb.schema.common import Base, LEN
import cog.tbl
import cog.util
from cog.util import ReprMixin


class AdminPerm(ReprMixin, Base):
    """
    Table that lists admins. Essentially just a boolean.
    All admins are equal, except for removing other admins, then seniority is considered by date.
    This shouldn't be a problem practically.
    """
    __tablename__ = 'perms_admins'
    _repr_keys = ['id', 'date']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    date = sqla.Column(sqla.DateTime, default=datetime.datetime.utcnow)  # All dates UTC

    def remove(self, session, other):
        """
        Remove an existing admin.
        """
        if self.date > other.date:
            raise cog.exc.InvalidPerms("You are not the senior admin. Refusing.")
        session.delete(other)
        session.commit()

    def __eq__(self, other):
        return isinstance(other, AdminPerm) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class ChannelPerm(ReprMixin, Base):
    """
    A channel permission to restrict cmd to listed channels.
    """
    __tablename__ = 'perms_channels'
    _repr_keys = ['cmd', 'guild_id', 'channel_id']

    cmd = sqla.Column(sqla.String(LEN['action_name']), primary_key=True)
    guild_id = sqla.Column(sqla.BigInteger, primary_key=True)
    channel_id = sqla.Column(sqla.BigInteger, primary_key=True)

    def __eq__(self, other):
        return isinstance(other, ChannelPerm) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.cmd}_{self.guild_id}_{self.channel_id}")


class RolePerm(ReprMixin, Base):
    """
    A role permission to restrict cmd to listed roles.
    """
    __tablename__ = 'perms_roles'
    _repr_keys = ['cmd', 'guild_id', 'role_id']

    cmd = sqla.Column(sqla.String(LEN['action_name']), primary_key=True)
    guild_id = sqla.Column(sqla.BigInteger, primary_key=True)
    role_id = sqla.Column(sqla.BigInteger, primary_key=True)

    def __eq__(self, other):
        return isinstance(other, RolePerm) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.cmd}_{self.guild_id}_{self.role_id}")
