"""
All database related code resides under this module.
Only one rule, no sql text.

Useful Documentation
--------------------
ORM  tutorial:
    http://docs.sqlalchemy.org/en/latest/orm/tutorial.html
Relationships:
    http://docs.sqlalchemy.org/en/latest/orm/basic_relationships.html
Relationship backrefs:
    http://docs.sqlalchemy.org/en/latest/orm/backref.html#relationships-backref
"""
import logging
import os
import sys

import sqlalchemy
import sqlalchemy.event
import sqlalchemy.exc
import sqlalchemy.orm

import cog.util

# Old engine, just in case
# engine = sqlalchemy.create_engine('sqlite://', echo=False)

MYSQL_SPEC = 'mysql+pymysql://{user}:{pass}@{host}/{db}?charset=utf8mb4'
CREDS = cog.util.get_config('dbs', 'main')

TEST_DB = False
if 'pytest' in sys.modules:
    CUR_DB = 'test'
    CREDS['db'] = CUR_DB
    TEST_DB = True
else:
    CUR_DB = os.environ.get('COG_TOKEN', 'dev')
    CREDS['db'] = CUR_DB

engine = sqlalchemy.create_engine(MYSQL_SPEC.format(**CREDS), echo=False, pool_recycle=3600, pool_size=20)
Session = sqlalchemy.orm.sessionmaker(bind=engine)
logging.getLogger(__name__).error('Main Engine Selected: %s', engine)

# Local eddb server
CREDS['db'] = "eddb"
eddb_engine = sqlalchemy.create_engine(MYSQL_SPEC.format(**CREDS), echo=False, pool_recycle=3600, pool_size=20)
EDDBSession = sqlalchemy.orm.sessionmaker(bind=eddb_engine)

# Remote server tracking bgs
CREDS = cog.util.get_config('dbs', 'side')
side_engine = sqlalchemy.create_engine(MYSQL_SPEC.format(**CREDS), echo=False, pool_recycle=3600, pool_size=10)
SideSession = sqlalchemy.orm.sessionmaker(bind=side_engine)

CREDS = None


# Local engine connections should not cross process boundary.
@sqlalchemy.event.listens_for(engine, 'connect')
def event_connect(dbapi_connecion, connection_record):
    """ Store PID. """
    connection_record.info['pid'] = os.getpid()


@sqlalchemy.event.listens_for(engine, 'checkout')
def event_checkout(dbapi_connecion, connection_record, connection_proxy):
    """ Invalidate engine connection when in different process. """
    pid = os.getpid()
    if connection_record.info['pid'] != pid:
        connection_record.connection = None
        connection_proxy.connection = None

        raise sqlalchemy.exc.DisconnectionError(
            'Connection record belongs to pid {}'
            'attempting to check out in pid {}'.format(connection_record.info['pid'], pid))


def fresh_sessionmaker(db=None):
    """
    If in another process, create a new connection setup for new sessions.

    args:
        db: The database to select with mysql, by default COG_TOKEN.
    """
    creds = cog.util.get_config('dbs', 'main')
    if not db:
        db = os.environ.get('COG_TOKEN', 'dev')
    creds['db'] = db

    eng = sqlalchemy.create_engine(MYSQL_SPEC.format(**creds), echo=False, pool_recycle=3600)
    return sqlalchemy.orm.sessionmaker(bind=eng)
