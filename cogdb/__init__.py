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

# Old engine declarations, just in case
# engine = sqlalchemy.create_engine('sqlite://', echo=False)
# engine = sqlalchemy.create_engine(MYSQL_SPEC.format(**CREDS), echo=False, pool_pre_ping=True, pool_recycle=3600, pool_size=20)

MYSQL_SPEC = 'mysql+pymysql://{user}:{pass}@{host}/{db}?charset=utf8mb4'
CREDS = cog.util.CONF.dbs.unwrap
TEST_DB = 'pytest' in sys.modules
if TEST_DB:
    CREDS['main']['db'] = 'test'
else:
    CREDS['main']['db'] = os.environ.get('TOKEN', 'dev')
CUR_DB = CREDS['main']['db']

engine = sqlalchemy.create_engine(
    MYSQL_SPEC.format(**CREDS['main']),
    echo=False, poolclass=NullPool, connect_args={'connect_timeout': 3}
)
Session = sqlalchemy.orm.sessionmaker(bind=engine)
logging.getLogger(__name__).error('Main Engine Selected: %s', engine)

# Local eddb server
eddb_engine = sqlalchemy.create_engine(
    MYSQL_SPEC.format(**CREDS['eddb']),
    echo=False, poolclass=NullPool, connect_args={'connect_timeout': 3}
)
EDDBSession = sqlalchemy.orm.sessionmaker(bind=eddb_engine)
logging.getLogger(__name__).error('EDDB Engine Selected: %s', eddb_engine)

# Remote server tracking bgs
side_engine = sqlalchemy.create_engine(MYSQL_SPEC.format(
    **CREDS['side']), echo=False, poolclass=NullPool
)
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
