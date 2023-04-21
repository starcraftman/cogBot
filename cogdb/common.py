"""
Common functionality to share amongs all database code.
"""
import json
import os

import cog.util

PRELOAD_DIR = cog.util.rel_to_abs('data', 'preload')


def dump_dbobjs_to_file(*, cls, db_objs):
    """
    Dump a list of database objects directly to a preload file.
    The class in question must implement cog.util.ReprMixin

    Args:
        cls: The database class in question to preload.
        db_objs; If provided, these objects will be dumped instead of those in db.

    Raises:
        ValueError - The cls cannot be dumped.
    """
    if not issubclass(cls, cog.util.ReprMixin):
        raise ValueError(f"Cannot dump {cls.__name__}, it doesn't implement cog.util.ReprMixin")

    with open(os.path.join(PRELOAD_DIR, f'{cls.__name__}.json'), 'w', encoding='utf-8') as fout:
        objects = [x.to_kwargs() for x in db_objs]
        json.dump(objects, fout, indent=2, sort_keys=True)


def dump_table_to_file(session, *, cls):
    """
    Dump the contents of the database to a preload file.
    The class in question must implement cog.util.ReprMixin

    Args:
        session: A session onto the database storing the cls.
        cls: The database class in question to preload.

    Raises:
        ValueError - The cls cannot be dumped.
    """
    if not issubclass(cls, cog.util.ReprMixin):
        raise ValueError(f"Cannot dump {cls.__name__}, it doesn't implement cog.util.ReprMixin")

    with open(os.path.join(PRELOAD_DIR, f'{cls.__name__}.json'), 'w', encoding='utf-8') as fout:
        objects = [x.to_kwargs() for x in session.query(cls)]
        json.dump(objects, fout, indent=2, sort_keys=True)


def preload_table_from_file(session, *, cls):
    """
    Preload a table with the constant information recorded to
    the preload file for the given class.
    Preload will only take place if the table is empty.

    Args:
        session: A session onto the database storing the cls.
        cls: The database class in question to preload.
    """
    if not session.query(cls).first():
        with open(os.path.join(PRELOAD_DIR, cls.__name__ + '.json'), 'r', encoding='utf-8') as fin:
            objects = [cls(**x) for x in json.load(fin)]
            session.add_all(objects)
            session.flush()
