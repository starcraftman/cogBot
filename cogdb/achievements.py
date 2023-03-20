"""
Module to store and assign achievements.

TODO:
Regarding achievements, do you have suggestions for examples?

I assume one set is kill count and kos kill count. Perhaps fort and UM totals too.
Storage of achievements is easy. I guess other thing is to look at defining roles for them and allowing bot to add/remove em.

I'll probably need to allow user to show/remove achievements,
either by allowing them to choose what roles added but also maybe a display function.
"""
import datetime
import json
import sys
import time

import sqlalchemy as sqla
import sqlalchemy.exc as sqla_e
from sqlalchemy.schema import UniqueConstraint

import cogdb
import cogdb.eddb
import cogdb.query
from cogdb.eddb import Base
from cog.util import ReprMixin, TimestampMixin
import pvp.schema

LEN = {
    'achievement_description': 500,
    'achievement_kwargs': 1000,
    'achievement_name': 100,  # Hard limit for roles
}
VALID_STATS = [
    'deaths', 'kills', 'kos_kills', 'escaped_interdicteds',
    'interdicteds', 'interdicted_deaths', 'interdicted_kills',
    'interdictions', 'interdiction_deaths', 'interdiction_kills',
]


class Achievement(ReprMixin, TimestampMixin, Base):
    """ Represents an achievement assigned to a user. """
    __tablename__ = "achievements"
    __table_args__ = (
        UniqueConstraint('discord_id', 'achievement_type_id', name='_achievement_id_type_unique'),
    )
    _repr_keys = ['id', 'discord_id', 'achievement_type_id', 'created_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    discord_id = sqla.Column(sqla.BigInteger)
    achievement_type_id = sqla.Column(sqla.Integer, sqla.ForeignKey('achievement_types.id'))
    created_at = sqla.Column(sqla.Integer, default=time.time)

    type_ = sqla.orm.relationship('AchievementType', viewonly=True, lazy='joined')

    def __eq__(self, other):
        return (isinstance(self, Achievement) and isinstance(other, Achievement)
                and self.id == other.id)

    def __hash__(self):
        return self.id


class AchievementType(ReprMixin, TimestampMixin, Base):
    __tablename__ = "achievement_types"
    _repr_keys = [
        'id', 'role_name', 'role_colour', 'role_description',
        'check_func', 'check_kwargs', 'created_at'
    ]

    id = sqla.Column(sqla.Integer, primary_key=True)

    role_name = sqla.Column(sqla.String(LEN["achievement_name"]), index=True, unique=True)
    role_colour = sqla.Column(sqla.String(6), default='')  # Hex strings, no leading 0x: B20000
    role_description = sqla.Column(sqla.String(LEN["achievement_description"]))
    check_func = sqla.Column(sqla.String(LEN["achievement_name"]), nullable=False)
    check_kwargs = sqla.Column(sqla.String(LEN["achievement_kwargs"]), nullable=False)
    created_at = sqla.Column(sqla.Integer, default=time.time)

    @property
    def role_color(self):
        """ Just alternate name. """
        return self.role_colour

    @property
    def hex_value(self):
        """ Simple conversion of hex to integer value. """
        return int(self.role_colour, 16)

    def check(self, **kwargs):
        """
        Check if the requirements of this achievement met.

        Args:
            kwargs: Passed in args that will be combined with the fixed kwargs for check.

        Returns: True IFF the check passed.
        """
        kwargs.update(json.loads(self.check_kwargs))
        func = getattr(sys.modules[__name__], self.check_func)
        return func(**kwargs)

    def __eq__(self, other):
        return (isinstance(self, AchievementType) and isinstance(other, AchievementType)
                and self.id == other.id)

    def __hash__(self):
        return self.id


def remove_achievement_type(eddb_session, *, role_name):
    """
    Remove an existing AchievementType, implicitly removes achievements assigned.

    Args:
        eddb_session: A session onto the EDDB database.
        role_name: The unique name of the achievement currently assigned.
    """
    type_subq = eddb_session.query(AchievementType.id).\
        filter(AchievementType.role_name == role_name).\
        scalar()
    eddb_session.query(Achievement).\
        filter(Achievement.achievement_type_id == type_subq).\
        delete()
    eddb_session.query(AchievementType).\
        filter(AchievementType.role_name == role_name).\
        delete()


def update_achievement_type(eddb_session, *, role_name, new_role_name=None, role_colour=None,
                            role_description=None, check_func=None, **kwargs):
    """
    Update (or add if not present) an AchievementType to the database.

    Args:
        eddb_session: A session onto the EDDB database.
        role_name: The unique name of the achievement currently assigned.
        new_role_name: A new unique name for the achievement, ensure it isn't in use.
        role_colour: A hex value to assign to the achievement role.
        role_description: A description to assign to the achievement.
        check_func: A name of a check function available in cogdb.achievements.
        kwargs: A dictionary of kwargs to pass to the check_func when a check is made.

    Raises:
        ValueError - There was a problem with arguments.
            1) Collision with new_name and existing names.
            2) All args are required for new additions.

    Returns: The updated AchievementType.
    """
    try:
        achieve = eddb_session.query(AchievementType).\
            filter(AchievementType.role_name == role_name).\
            one()

        # Check before changing for collisions on new name
        if new_role_name:
            try:
                eddb_session.query(AchievementType).\
                    filter(AchievementType.role_name == new_role_name).\
                    one()
                raise ValueError("Achievement: role_name is already in use. Rename existing role or choose new name.")
            except sqla.exc.NoResultFound:
                achieve.role_name = new_role_name

        achieve.role_colour = role_colour if role_colour else achieve.role_colour
        achieve.role_description = role_description if role_description else achieve.role_description
        achieve.check_func = check_func if check_func else achieve.check_func
        achieve.kwargs = json.dumps(kwargs) if len(kwargs) else achieve.kwargs
    except sqla.exc.NoResultFound as exc:
        if not role_colour or not role_description or not check_func or not kwargs:
            raise ValueError("Achievement: Missing one or more required args for creation.") from exc

        achieve = AchievementType(
            role_name=role_name, role_colour=role_colour, role_description=role_description,
            check_func=check_func, check_kwargs=json.dumps(kwargs)
        )
        eddb_session.add(achieve)
        eddb_session.commit()

    return achieve


def check_pvpstat_greater(*, stats, stat_name, amount):
    """
    Compute the pvp stats for CMDR with discord_id.
    Once done, check if the statistic field hs a value greater than amount.

    Args:
        stats: A computed set of stats for a CMDR, a dictionary.
        stat_name: The name of a numerical statistic computed for CMDR.
        amount: The threshold of the achievement, passes when >= to amount.

    Returns: True only if the achievement check is satisfied.
    """
    return stats[stat_name] >= amount


def check_pvpcmdr_in_events(*, discord_id, eddb_session, pvp_event, target_cmdr):
    """
    Check if a specific other CMDR present in a CMDRs events, like kills or deaths.

    Args:
        discord_id: The discord ID of the CMDR being checked.
        eddb_session: A session onto the EDDB database.
        pvp_event: The type of PVP event to look through, like PVPKill.
        target_cmdr: Name of a given OTHER cmdr that was involved in the event.

    Returns: True only if the achievement check is satisfied.
    """
    return pvp.schema.list_of_events(
        eddb_session,
        cmdr_id=discord_id, events=[getattr(pvp.schema, pvp_event)], target_cmdr=target_cmdr
    )


def check_pvpcmdr_age(*, discord_id, eddb_session, required_days):
    """
    Check if a CMDR has been an active PVPCMDR for a given time.

    Args:
        discord_id: The discord ID of the CMDR being checked.
        eddb_session: A session onto the EDDB database.
        required_days: Satisfied when the number of days is older than this.

    Returns: True only if the achievement check is satisfied.
    """
    pvpcmdr = pvp.schema.get_pvp_cmdr(eddb_session, cmdr_id=discord_id)
    time_diff = time.time() - pvpcmdr.created_at
    return datetime.timedelta(seconds=time_diff).days >= required_days


def check_duser_forts(*, discord_id, session, amount):
    """
    Check if a CMDR has forted a certain amount in the current cycle.

    Args:
        discord_id: The discord ID of the CMDR being checked.
        session: A session onto the database.
        amount: The threshold of the achievement, passes when >= to amount.

    Returns: True only if the achievement check is satisfied.
    """
    try:
        duser = cogdb.query.get_duser(session, discord_id)
        return duser.fort_user.dropped >= amount
    except AttributeError:
        return False


def check_duser_um(*, discord_id, session, amount):
    """
    Check if a CMDR has undermined a certain amount in the current cycle.

    Args:
        discord_id: The discord ID of the CMDR being checked.
        session: A session onto the database.
        amount: The threshold of the achievement, passes when >= to amount.

    Returns: True only if the achievement check is satisfied.
    """
    try:
        duser = cogdb.query.get_duser(session, discord_id)
        total_um = 0
        for hold in duser.um_merits:
            total_um += hold.held + hold.redeemed

        return total_um >= amount
    except AttributeError:
        return False


def check_duser_snipe(*, discord_id, session, amount):
    """
    Check if a CMDR has sniped a certain amount in the current cycle.

    Args:
        discord_id: The discord ID of the CMDR being checked.
        session: A session onto the database.
        amount: The threshold of the achievement, passes when >= to amount.

    Returns: True only if the achievement check is satisfied.
    """
    try:
        duser = cogdb.query.get_duser(session, discord_id)
        total_um = 0
        for hold in duser.snipe_merits:
            total_um += hold.held + hold.redeemed

        return total_um >= amount
    except AttributeError:
        return False


def drop_tables():  # pragma: no cover | destructive to test
    """
    Drop the spy tables entirely.
    """
    sqla.orm.session.close_all_sessions()
    for table in ACHIEVEMENT_TABLES:
        try:
            table.__table__.drop(cogdb.eddb_engine)
        except sqla_e.OperationalError:
            pass


def empty_tables():
    """
    Ensure all spy tables are empty.
    """
    sqla.orm.session.close_all_sessions()
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        for table in ACHIEVEMENT_TABLES:
            eddb_session.query(table).delete()


def recreate_tables():  # pragma: no cover | destructive to test
    """
    Recreate all tables in the related to this module, mainly for schema changes and testing.
    Always reload preloads.
    """
    sqla.orm.session.close_all_sessions()
    drop_tables()
    Base.metadata.create_all(cogdb.eddb_engine)


def main():  # pragma: no cover | destructive to test
    """
    Main function to load the test data during development.
    """
    recreate_tables()


ACHIEVEMENT_TABLES = [Achievement, AchievementType]
# Ensure the tables are created before use when this imported
if cogdb.TEST_DB:
    recreate_tables()
else:
    Base.metadata.create_all(cogdb.eddb_engine)


if __name__ == "__main__":
    main()
