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

import cogdb.schema
from cogdb.schema import Fort, System, User

mem_engine = sqlalchemy.create_engine('sqlite://', echo=False)
cogdb.schema.Base.metadata.create_all(cogdb.mem_engine)

# Base.metadata.create_all(engine)
Session = sqlalchemy.orm.sessionmaker(bind=mem_engine)