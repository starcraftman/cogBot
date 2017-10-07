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
from __future__ import absolute_import, print_function
import logging
import os
import sys

import sqlalchemy
import sqlalchemy.orm

import cog.util

# Old engine, just in case
# engine = sqlalchemy.create_engine('sqlite://', echo=False)

MYSQL_SPEC = 'mysql+pymysql://{user}:{pass}@{host}/{db}?charset=utf8mb4'
CREDS = cog.util.get_config('dbs', 'main')

TEST_DB = False
if 'pytest' in sys.modules:
    CREDS['db'] = 'test'
    TEST_DB = True
else:
    CREDS['db'] = os.environ.get('COG_TOKEN', 'dev')

engine = sqlalchemy.create_engine(MYSQL_SPEC.format(**CREDS), echo=False)
Session = sqlalchemy.orm.sessionmaker(bind=engine)
logging.getLogger('cogdb').info('Main Engine: %s', engine)
print('Main Engine Selected: ', engine)

# Remote server tracking bgs
CREDS = cog.util.get_config('dbs', 'side')
side_engine = sqlalchemy.create_engine(MYSQL_SPEC.format(**CREDS), echo=False)
SideSession = sqlalchemy.orm.sessionmaker(bind=side_engine)

CREDS = None
