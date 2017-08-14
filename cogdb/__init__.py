"""
All database related code resides inside this module.

Useful Documentation
--------------------
Reference tutorial
  http://docs.sqlalchemy.org/en/latest/orm/tutorial.html
Relationships:
  http://docs.sqlalchemy.org/en/latest/orm/basic_relationships.html#relationship-patterns
Linking Relationships:
  http://docs.sqlalchemy.org/en/latest/orm/backref.html#relationships-backref
Customing Collections Access:
  http://docs.sqlalchemy.org/en/latest/orm/collections.html#custom-collections
Constraints:
  http://docs.sqlalchemy.org/en/latest/core/constraints.html#unique-constraint
"""
from __future__ import absolute_import, print_function

import sqlalchemy
import sqlalchemy.orm

import cog.share

# Use memory engine when testing else the regular production db.
# TODO Move to mysql like engine, for each COG_TOKEN make different db to use.
engine = sqlalchemy.create_engine('sqlite://', echo=False)

Session = sqlalchemy.orm.sessionmaker(bind=engine)

# Remote server tracking bgs
creds = cog.share.get_config('dbs', 'side')
side_engine = sqlalchemy.create_engine('mysql+pymysql://{user}:{pass}@{host}/{db}'.format(**creds),
                                       echo=False)
SideSession = sqlalchemy.orm.sessionmaker(bind=side_engine)
