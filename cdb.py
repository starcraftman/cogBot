#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Database related code, for caching excel sheet

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

import sqlalchemy as sqa
import sqlalchemy.orm as sqa_orm
# from sqlalchemy.orm.collections import attribute_mapped_collection as sqa_attr_map
import sqlalchemy.ext.declarative as sqa_dec


Base = sqa_dec.declarative_base()


class User(Base):
    """
    Every user of bot, has an entry here. Discord name must be unique.
    """
    __tablename__ = 'users'

    id = sqa.Column(sqa.Integer, primary_key=True)
    discord_name = sqa.Column(sqa.String, unique=True)
    sheet_name = sqa.Column(sqa.String)
    sheet_row = sqa.Column(sqa.Integer)

    def __repr__(self):
        args = {
            'discord': self.discord_name,
            'sheet': self.sheet_name,
            'row': self.sheet_row,
        }
        return "<User(discord_name='{discord}', sheet_name='{sheet}', "\
               "sheet_row='{row}')>".format(**args)

    def __str__(self):
        return "ID='{}', ".format(self.id) + self.__repr__()


class System(Base):
    """
    Maintain the state of each system as retreieved from the spreadsheet.
    """
    __tablename__ = 'systems'

    id = sqa.Column(sqa.Integer, primary_key=True)
    name = sqa.Column(sqa.String, unique=True)
    sheet_col = sqa.Column(sqa.String)
    sheet_order = sqa.Column(sqa.Integer)
    current = sqa.Column(sqa.Integer)
    cmdr_merits = sqa.Column(sqa.Integer)
    trigger = sqa.Column(sqa.Integer)
    undermine = sqa.Column(sqa.Float)

    def __repr__(self):
        args = {
            'name': self.name,
            'order': self.sheet_order,
            'col': self.sheet_col,
            'cur': self.current,
            'merits': self.cmdr_merits,
            'trig': self.trigger,
            'under': self.undermine,
        }
        return "<System(name='{name}', sheet_order='{order}', sheet_col='{col}', "\
                "merits='{merits}', current='{cur}', trigger='{trig}', "\
                "undermine='{under}')>".format(**args)

    def __str__(self):
        return "ID='{}', ".format(self.id) + self.__repr__()


class Fort(Base):
    """
    Every drop made by a user creates a fort entry here.
    User maintains a sub collection of these for easy access.
    """
    __tablename__ = 'forts'

    id = sqa.Column(sqa.Integer, primary_key=True)
    user_id = sqa.Column(sqa.Integer, sqa.ForeignKey('users.id'))
    system_id = sqa.Column(sqa.String, sqa.ForeignKey('systems.id'))
    amount = sqa.Column(sqa.Integer)

    def __repr__(self):
        args = {
            'user': self.user.discord_name,
            'system': self.system.name,
            'amount': self.amount,
        }
        return "<Fort(user='{user}', "\
                "system='{system}', amount='{amount}')>".format(**args)

    def __str__(self):
        return "ID='{}', ".format(self.id) + self.__repr__()


Fort.user = sqa_orm.relationship('User', back_populates='forts')
Fort.system = sqa_orm.relationship('System', back_populates='forts')
User.forts = sqa_orm.relationship('Fort',
                                  # collection_class=sqa_attr_map('system.name'),
                                  cascade='all, delete, delete-orphan',
                                  back_populates='user')
System.forts = sqa_orm.relationship('Fort',
                                    # collection_class=sqa_attr_map('user.discord_name'),
                                    cascade='all, delete, delete-orphan',
                                    back_populates='system')


def main():
    engine = sqa.create_engine('sqlite:///:memory:', echo=False)
    Base.metadata.create_all(engine)
    Session = sqa_orm.sessionmaker(bind=engine)

    users = (
        User(discord_name='gearsandcogs', sheet_name='GearsandCogs', sheet_row=15),
        User(discord_name='rjwhite', sheet_name='rjwhite', sheet_row=16),
        User(discord_name='vampyregtx', sheet_name='vampyregtx', sheet_row=17),
    )

    session = Session()
    session.add_all(users)
    session.commit()

    systems = (
        System(name='Frey', sheet_col='F', sheet_order=1, current=0,
               cmdr_merits=0, trigger=7400, undermine=0),
        System(name='Adeo', sheet_col='G', sheet_order=2, current=0,
               cmdr_merits=0, trigger=5400, undermine=0),
        System(name='Sol', sheet_col='H', sheet_order=3, current=0,
               cmdr_merits=0, trigger=6000, undermine=0),
    )

    session.add_all(systems)
    session.commit()

    forts = (
        Fort(user_id=users[0].id, system_id=systems[0].id, amount=700),
        Fort(user_id=users[1].id, system_id=systems[0].id, amount=700),
        Fort(user_id=users[0].id, system_id=systems[2].id, amount=1400),
        Fort(user_id=users[2].id, system_id=systems[1].id, amount=2100),
        Fort(user_id=users[2].id, system_id=systems[0].id, amount=300),
    )

    session.add_all(forts)
    session.commit()

    import pprint
    def mprint(*args):
        args = [str(x) for x in args]
        pprint.pprint(*args)

    for user in session.query(User).order_by(User.discord_name).all():
        mprint(user)
        mprint(user.forts)

    for sys in session.query(System).order_by(System.name):
        mprint(sys)
        mprint(sys.forts)

    for fort in session.query(Fort).order_by(Fort.system_id):
        mprint(fort)
        mprint(fort.user)
        mprint(fort.system)


if __name__ == "__main__":
    main()

