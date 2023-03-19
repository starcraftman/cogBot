"""
Module to store and assign achievements.

TODO:
Regarding achievements, do you have suggestions for examples?

I assume one set is kill count and kos kill count. Perhaps fort and UM totals too.
Storage of achievements is easy. I guess other thing is to look at defining roles for them and allowing bot to add/remove em.

I'll probably need to allow user to show/remove achievements,
either by allowing them to choose what roles added but also maybe a display function.
"""
import time
import sqlalchemy as sqla
import sqlalchemy.exc as sqla_e
from sqlalchemy.schema import UniqueConstraint

import cogdb
from cogdb.eddb import Base
from cog.util import ReprMixin, TimestampMixin

LEN = {
    'achievement_name': 50,
    'achievement_description': 500,
}


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
        return hash(self.id)


class AchievementType(ReprMixin, TimestampMixin, Base):
    __tablename__ = "achievement_types"
    _repr_keys = ['id', 'name', 'description', 'created_at']

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN["achievement_name"]))
    description = sqla.Column(sqla.String(LEN["achievement_description"]))
    created_at = sqla.Column(sqla.Integer, default=time.time)

    def __eq__(self, other):
        return (isinstance(self, AchievementType) and isinstance(other, AchievementType)
                and self.id == other.id)

    def __hash__(self):
        return self.id


def preload_achievement_types(session):
    """ Preload all achievement types to a table. """
    session.add_all([
        AchievementType(id=1, name='FirstKill', description='First confirmed kill reported.'),
        AchievementType(id=2, name='EvenDozen', description='You got a dozen kills.'),
    ])


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
