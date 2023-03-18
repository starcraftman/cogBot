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
import asyncio
import logging
import os
import sys
from contextlib import contextmanager

import sqlalchemy
import sqlalchemy.event
import sqlalchemy.exc
import sqlalchemy.orm
from sqlalchemy.pool import NullPool

import cog.util

# Old engine, just in case
# engine = sqlalchemy.create_engine('sqlite://', echo=False)

MYSQL_SPEC = 'mysql+pymysql://{user}:{pass}@{host}/{db}?charset=utf8mb4'
CREDS = cog.util.CONF.dbs.main.unwrap

TEST_DB = False
if 'pytest' in sys.modules:
    CUR_DB = 'test'
    CREDS['db'] = CUR_DB
    TEST_DB = True
else:
    CUR_DB = os.environ.get('TOKEN', 'dev')
    CREDS['db'] = CUR_DB

#  engine = sqlalchemy.create_engine(MYSQL_SPEC.format(**CREDS), echo=False, pool_pre_ping=True, pool_recycle=3600, pool_size=20)
engine = sqlalchemy.create_engine(MYSQL_SPEC.format(**CREDS), echo=False, poolclass=NullPool)
Session = sqlalchemy.orm.sessionmaker(bind=engine)
logging.getLogger(__name__).error('Main Engine Selected: %s', engine)

# Local eddb server
CREDS['db'] = "eddb"
#  eddb_engine = sqlalchemy.create_engine(MYSQL_SPEC.format(**CREDS), echo=False, pool_pre_ping=True, pool_recycle=3600, pool_size=20)
eddb_engine = sqlalchemy.create_engine(MYSQL_SPEC.format(**CREDS), echo=False, poolclass=NullPool)
EDDBSession = sqlalchemy.orm.sessionmaker(bind=eddb_engine)

# Remote server tracking bgs
CREDS = cog.util.CONF.dbs.side.unwrap
#  side_engine = sqlalchemy.create_engine(MYSQL_SPEC.format(**CREDS), echo=False, pool_recycle=900, pool_size=10)
side_engine = sqlalchemy.create_engine(MYSQL_SPEC.format(**CREDS), echo=False, poolclass=NullPool)
SideSession = sqlalchemy.orm.sessionmaker(bind=side_engine)

CREDS = None


# Local engine connections should not cross process boundary.
@sqlalchemy.event.listens_for(engine, 'connect')
def event_connect(_, connection_record):
    """ Store PID. """
    connection_record.info['pid'] = os.getpid()


@sqlalchemy.event.listens_for(engine, 'checkout')
def event_checkout(_, connection_record, connection_proxy):
    """ Invalidate engine connection when in different process. """
    pid = os.getpid()
    if connection_record.info['pid'] != pid:
        connection_record.connection = None
        connection_proxy.connection = None

        raise sqlalchemy.exc.DisconnectionError(
            f"Connection record belongs to pid {connection_record.info['pid']} attempting to check out in pid {pid}")


async def monitor_pools(delay=120):
    """
    Runs forever and just logs the status of each pool.
    """
    while True:
        await asyncio.sleep(delay)
        log = logging.getLogger(__name__)
        for name, eng in zip(("local", "eddb", "side"), (engine, eddb_engine, side_engine)):
            log.info("POOL %s: %s", name, eng.pool.status())


@contextmanager
def session_scope(*args, **kwargs):
    """
    Provide a transactional scope around a series of operations.
    """
    session_maker = args[0]
    session = session_maker(**kwargs)
    try:
        yield session
        session.commit()
    except:  # noqa: E722
        session.rollback()
        raise
    finally:
        session.close()
