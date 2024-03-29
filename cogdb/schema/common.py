"""
Base - The common SQLAlchemy base for all objects in this schema.
LEN  - The maximum length of varchar fields in this database schema.
"""
import sqlalchemy.ext.declarative

# The maximum length of variable strings in database
LEN = {
    'carrier': 7,
    'action_name': 25,
    'command': 2000,
    'name': 100,
    'reason': 400,
    'sheet_col': 5,
}

# The base for all classes outside of eddb database.
Base = sqlalchemy.ext.declarative.declarative_base()
