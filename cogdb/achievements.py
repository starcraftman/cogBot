"""
Module to store and assign achievements.

Every AchievementType broadly defines the role assigned, name and description.
Each AchievementType is part of a role_set and has a priority.
Higher priority achievements take precedence over lower priority ones.

Achievement simply stores the unlock for individual CMDRs.

Much of the rest relates to determining what roles a user should have given set/priority rules.
"""
import contextlib
import datetime
import json
import sys
import tempfile
import time

import sqlalchemy as sqla
import sqlalchemy.exc as sqla_e
from sqlalchemy.schema import UniqueConstraint

import cogdb
import cogdb.eddb
import cogdb.query
from cogdb.eddb import Base
import cog.exc
from cog.util import ReprMixin, TimestampMixin
import pvp.schema

LEN = {
    'achievement_description': 500,
    'achievement_kwargs': 1000,
    'achievement_name': 100,  # Hard limit for roles
    'achievement_func': 50,
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
        UniqueConstraint('discord_id', 'type_id', name='_achievement_id_type_unique'),
    )
    _repr_keys = ['id', 'discord_id', 'type_id', 'created_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    discord_id = sqla.Column(sqla.BigInteger)
    type_id = sqla.Column(sqla.Integer, sqla.ForeignKey('achievement_types.id'))
    created_at = sqla.Column(sqla.Integer, default=time.time)

    type = sqla.orm.relationship('AchievementType', viewonly=True, lazy='joined')

    def __eq__(self, other):
        return (isinstance(self, Achievement) and isinstance(other, Achievement)
                and self.id == other.id)

    def __hash__(self):
        return self.id


class AchievementType(ReprMixin, TimestampMixin, Base):
    __tablename__ = "achievement_types"
    __table_args__ = (
        UniqueConstraint('role_set', 'role_priority', 'guild_id', name='_role_set_priority_unique'),
        UniqueConstraint('role_name', 'guild_id', name='_role_name_guild_id_unique'),
    )
    _repr_keys = [
        'id', 'role_name', 'role_colour', 'role_description',
        'role_set', 'role_priority', 'guild_id', 'check_func', 'check_kwargs', 'created_at'
    ]

    id = sqla.Column(sqla.Integer, primary_key=True)
    guild_id = sqla.Column(sqla.BigInteger)

    role_name = sqla.Column(sqla.String(LEN["achievement_name"]), index=True, unique=True)
    role_colour = sqla.Column(sqla.String(6), default='')  # Hex strings, no leading 0x: B20000
    role_description = sqla.Column(sqla.String(LEN["achievement_description"]))
    role_set = sqla.Column(sqla.String(LEN["achievement_name"]))
    role_priority = sqla.Column(sqla.Integer, default=1)
    check_func = sqla.Column(sqla.String(LEN["achievement_func"]), nullable=False)
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

    def describe(self):
        """ Simple description of achievement. """
        return f'{self.role_name}: {self.role_description}'

    def __eq__(self, other):
        return (isinstance(self, AchievementType) and isinstance(other, AchievementType)
                and self.id == other.id)

    def __hash__(self):
        return self.id


def get_achievement_types_by_set(eddb_session):
    """
    Get a complete set of all existing AchievementTypes.
    The AchievementTypes will be ordered into a dictionary indexed on role_set.
    Each entry in the dictionary will have a list of all AchievementTypes in that role_set.
    The order of entries in the role_set will be in ascending priority.

    Args:
        eddb_session: A session onto the EDDB database.

    Returns: A dictionary mapping role_sets onto lists of AchievementTypes in those sets.
    """
    achievements = eddb_session.query(AchievementType).\
        order_by(AchievementType.role_set, AchievementType.role_priority).\
        all()
    results = {}
    for achievement in achievements:
        try:
            results[achievement.role_set] += [achievement]
        except KeyError:
            results[achievement.role_set] = [achievement]

    return results


def get_user_achievements_by_set(eddb_session, *, discord_id):
    """
    Get a complete set of all Achievements for a single user.
    The Achievements will be ordered into a dictionary indexed on role_set.
    Each entry in the dictionary will have a list of all Achievements in that role_set.
    The order of entries in the role_set will be in ascending priority.

    Args:
        eddb_session: A session onto the EDDB database.
        discord_id: The discord id of a user, narrow to only those achievements unlocked by user when present.

    Returns: A dictionary mapping role_sets onto lists of AchievementTypes in those sets.
    """
    achievements = eddb_session.query(Achievement).\
        join(AchievementType, Achievement.type_id == AchievementType.id).\
        filter(Achievement.discord_id == discord_id).\
        order_by(AchievementType.role_set, AchievementType.role_priority).\
        all()
    results = {}
    for achievement in achievements:
        try:
            results[achievement.type.role_set] += [achievement]
        except KeyError:
            results[achievement.type.role_set] = [achievement]

    return results


def roles_for_user(eddb_session, *, discord_id):
    """
    Determine the active set of roles to apply to a member.

    Args:
        eddb_session: A session onto the EDDB database.
        discord_id: The discord ID of a given CMDR.

    Returns: to_remove, to_add
        to_remove: The set of AchievementTypes whose roles should NOT be applied to the user.
        to_add: The set of AchievementTypes whose roles should be applied to the user.
    """
    existing = get_user_achievements_by_set(eddb_session, discord_id=discord_id)
    to_remove, to_add = {}, {}
    for achievements in existing.values():
        for a_type in [x.type for x in achievements[:-1]]:
            try:
                to_remove[a_type.guild_id] += [a_type.role_name]
            except KeyError:
                to_remove[a_type.guild_id] = [a_type.role_name]
        for a_type in [x.type for x in achievements[-1:]]:
            try:
                to_add[a_type.guild_id] += [a_type.role_name]
            except KeyError:
                to_add[a_type.guild_id] = [a_type.role_name]

    return to_remove, to_add


def remove_achievement_type(eddb_session, *, role_name, guild_id):
    """
    Remove an existing AchievementType, implicitly removes achievements assigned.

    Args:
        eddb_session: A session onto the EDDB database.
        role_name: The unique name of the achievement currently assigned.
    """
    type_subq = eddb_session.query(AchievementType.id).\
        filter(AchievementType.role_name == role_name,
               AchievementType.guild_id == guild_id).\
        scalar()
    eddb_session.query(Achievement).\
        filter(Achievement.type_id == type_subq).\
        delete()
    eddb_session.query(AchievementType).\
        filter(AchievementType.role_name == role_name,
               AchievementType.guild_id == guild_id).\
        delete()


def update_achievement_type(eddb_session, *, guild_id, role_name, new_role_name=None, role_colour=None,
                            role_description=None, check_func=None, check_kwargs=None):
    """
    Update (or add if not present) an AchievementType to the database.

    Args:
        eddb_session: A session onto the EDDB database.
        role_name: The unique name of the achievement currently assigned.
        new_role_name: A new unique name for the achievement, ensure it isn't in use.
        role_colour: A hex value to assign to the achievement role.
        role_description: A description to assign to the achievement.
        guild_id: The ID of the guild setting up the achievement.
        check_func: A name of a check function available in cogdb.achievements.
        check_kwargs: A dictionary of kwargs to pass to the check_func when a check is made.

    Raises:
        cog.exc.InvalidCommandArgs - There was a problem with arguments.
            1) Collision with new_name and existing names.
            2) All args are required for new additions.

    Returns: The updated AchievementType.
    """
    try:
        achieve = eddb_session.query(AchievementType).\
            filter(AchievementType.role_name == role_name,
                   AchievementType.guild_id == guild_id).\
            one()

        # Check before changing for collisions on new name
        if new_role_name:
            try:
                eddb_session.query(AchievementType).\
                    filter(AchievementType.guild_id == guild_id,
                           AchievementType.role_name == new_role_name).\
                    one()
                raise cog.exc.InvalidCommandArgs("Achievement: role_name is already in use. Rename existing role or choose new name.")
            except sqla.exc.NoResultFound:
                achieve.role_name = new_role_name

        achieve.role_colour = role_colour if role_colour else achieve.role_colour
        achieve.role_description = role_description if role_description else achieve.role_description
        achieve.guild_id = guild_id if guild_id else achieve.guild_id
        achieve.check_func = check_func if check_func else achieve.check_func
        achieve.check_kwargs = json.dumps(check_kwargs) if check_kwargs else achieve.check_kwargs
    except sqla.exc.NoResultFound as exc:
        if not role_colour or not role_description or not check_func or not check_kwargs:
            raise cog.exc.InvalidCommandArgs("Achievement: Missing one or more required args for creation.") from exc

        achieve = AchievementType(
            role_name=role_name, role_colour=role_colour, role_description=role_description,
            guild_id=guild_id, check_func=check_func, check_kwargs=json.dumps(check_kwargs)
        )
        eddb_session.add(achieve)
    eddb_session.commit()

    return achieve


def verify_achievements(*, session, eddb_session, discord_id, check_func_filter=None):
    """
    Check for new achievements a given user has not acquired.
    Any new achievements will be added to the database and
    the new AchievementType for each will be returend in a list.

    Args:
        eddb_session: A session onto the database.
        eddb_session: A session onto the EDDB database.
        discord_id: The discord ID of the CMDR being checked.
        check_func_filter: Limit new achievements to a subset of check_func names.

    Returns: A list of newly unlocked AchievementTypes
    """
    new_achievements = []
    achieved = {
        x[0] for x in eddb_session.query(Achievement.type_id).
        filter(Achievement.discord_id == discord_id).
        all()
    }
    not_achieved = eddb_session.query(AchievementType).\
        filter(AchievementType.id.not_in(achieved))
    if check_func_filter:
        not_achieved = not_achieved.filter(AchievementType.check_func.like(f'%{check_func_filter}%'))
    not_achieved = not_achieved.order_by(AchievementType.id).all()

    kwargs = {
        'discord_id': discord_id,
        'session': session,
        'eddb_session': eddb_session,
        'stats': pvp.schema.compute_pvp_stats(eddb_session, cmdr_ids=[discord_id]),
    }
    for achievement in not_achieved:
        if achievement.check(**kwargs):
            eddb_session.add(Achievement(discord_id=discord_id, type_id=achievement.id))
            new_achievements += [achievement]

    return new_achievements


def check_pvpstat_greater(*, stats, stat_name, amount, **_):
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


def check_pvpcmdr_in_events(*, discord_id, eddb_session, pvp_event, target_cmdr, **_):
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


def check_pvpcmdr_age(*, discord_id, eddb_session, required_days, **_):
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


def check_duser_forts(*, discord_id, session, amount, **_):
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


def check_duser_um(*, discord_id, session, amount, **_):
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


def check_duser_snipe(*, discord_id, session, amount, **_):
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


@contextlib.contextmanager
def create_role_summary(grouped_achievements):
    """
    Given a grouped set of AchievementTypes, create a temporarytext file listing all role sets
    and the roles and priorities under them.
    This is a context manager.

    Args:
        grouped_achievements: The achievements grouped by role set, see get_achievements_by_set.

    Returns: A filename as a string.
    """
    with tempfile.NamedTemporaryFile(mode='w') as tfile:
        tfile.write("__Existing Role Sets__\n")
        for role_set, achievement_types in grouped_achievements.items():
            if not role_set:
                continue
            tfile.write(f"\nRole Set: {role_set}\n")
            for achievement in achievement_types:
                tfile.write(f"    {achievement.role_name}: {achievement.role_priority}\n")
        tfile.flush()

        yield tfile.name


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
