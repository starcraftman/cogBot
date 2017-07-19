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
import os

import sqlalchemy
import sqlalchemy.orm

import cog.share

# Use memory engine when testing else the regular production db.
if os.environ.get('COG_TOKEN') == 'prod':
    spec = 'sqlite:///' + cog.share.rel_to_abs(cog.share.get_config('paths', 'db'))
else:
    spec = 'sqlite://'
engine = sqlalchemy.create_engine(spec, echo=False)

Session = sqlalchemy.orm.sessionmaker(bind=engine)
