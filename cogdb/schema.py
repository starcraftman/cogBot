"""
Manage the database and its tables.
"""
from __future__ import absolute_import, print_function

import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm
import sqlalchemy.ext.declarative

import cogdb


Base = sqlalchemy.ext.declarative.declarative_base()


class User(Base):
    """
    Every user of bot, has an entry here. Discord name must be unique.
    """
    __tablename__ = 'users'

    id = sqla.Column(sqla.Integer, primary_key=True)
    sheet_name = sqla.Column(sqla.String, unique=True)
    sheet_row = sqla.Column(sqla.Integer)

    def __repr__(self):
        args = {
            'sheet': self.sheet_name,
            'row': self.sheet_row,
        }
        return "<User(sheet_name='{sheet}', sheet_row='{row}')>".format(**args)

    def __str__(self):
        return "ID='{}', ".format(self.id) + self.__repr__()

    @property
    def merits(self):
        amount = 0
        for fort in self.forts:
            amount += fort.amount

        return amount


class Fort(Base):
    """
    Every drop made by a user creates a fort entry here.
    User maintains a sub collection of these for easy access.
    """
    __tablename__ = 'forts'

    id = sqla.Column(sqla.Integer, primary_key=True)
    user_id = sqla.Column(sqla.Integer, sqla.ForeignKey('users.id'))
    system_id = sqla.Column(sqla.String, sqla.ForeignKey('systems.id'))
    amount = sqla.Column(sqla.Integer)

    def __repr__(self):
        if getattr(self, 'user', None):
            user = self.user.sheet_name
        else:
            user = 'UID-{}'.format(self.user_id)

        if getattr(self, 'system', None):
            system = self.system.name
        else:
            system = 'SID-{}'.format(self.system_id)

        args = {
            'user': user,
            'system': system,
            'amount': self.amount,
        }
        return "<Fort(user='{user}', "\
               "system='{system}', amount='{amount}')>".format(**args)

    def __str__(self):
        return "ID='{}', ".format(self.id) + self.__repr__()


class System(Base):
    """
    Represent a single system for fortification.
    Object can be flushed and queried from the database.

    data: List to be unpacked: ump, trigger, cmdr_merits, status, notes):
    Data tuple is to be used to make a table, with header

    args:
        id: Set by the database, unique id.
        name: Name of the system. (string)
        current_status: Current reported status from galmap/users. (int)
        cmdr_merits: Total merits dropped by cmdrs. (int)
        trigger: Total trigger of merits required. (int)
        undermine: Percentage of undermining of the system. (float)
        notes: Any notes attached to the system. (string)
        sheet_col: The name of the column in the excel. (string)
        sheet_order: Order systems should be ordered. (int)
    """
    __tablename__ = 'systems'

    header = ['System', 'Trigger', 'Missing', 'UM', 'Notes']

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String, unique=True)
    sheet_col = sqla.Column(sqla.String)
    sheet_order = sqla.Column(sqla.Integer)
    fort_status = sqla.Column(sqla.Integer)
    cmdr_merits = sqla.Column(sqla.Integer)
    trigger = sqla.Column(sqla.Integer)
    undermine = sqla.Column(sqla.Float)
    notes = sqla.Column(sqla.String)

    def __repr__(self):
        """ Dump object data. """
        args = {
            'name': self.name,
            'order': self.sheet_order,
            'col': self.sheet_col,
            'cur': self.fort_status,
            'merits': self.cmdr_merits,
            'trig': self.trigger,
            'under': self.undermine,
            'notes': self.notes
        }
        return "<System(name='{name}', sheet_order='{order}', sheet_col='{col}', "\
               "merits='{merits}', fort_status='{cur}', trigger='{trig}', "\
               "undermine='{under}', notes='{notes}')>".format(**args)

    def __str__(self):
        return "ID='{}', ".format(self.id) + self.__repr__()

    @property
    def f_status(self):
        return self.fort_status

    @f_status.setter
    def set_f_status(self, new_value):
        if new_value > self.trigger:
            self.fort_status = self.trigger

    @property
    def c_merits(self):
        return self.cmdr_merits

    @c_merits.setter
    def set_c_merits(self, new_value):
        if new_value > self.trigger:
            self.cmdr_merits = self.trigger

    @property
    def current_status(self):
        """ Simply return max fort status reported. """
        return max(self.fort_status, self.cmdr_merits)

    @property
    def skip(self):
        """ The system should be skipped. """
        notes = self.notes.lower()
        return 'leave' in notes or 'skip' in notes

    @property
    def is_fortified(self):
        """ The remaining supplies to fortify """
        return self.current_status >= self.trigger

    @property
    def is_undermined(self):
        """ The system has been undermined """
        return self.undermine >= 99

    @property
    def missing(self):
        """ The remaining supplies to fortify """
        return max(0, self.trigger - self.current_status)

    @property
    def completion(self):
        """ The fort completion percentage """
        try:
            comp_cent = self.current_status / self.trigger * 100
        except ZeroDivisionError:
            comp_cent = 0

        return '{:.1f}'.format(comp_cent)

    @property
    def data_tuple(self):
        """
        Return a tuple of important data to be formatted for table output.
        Each element should be mapped to separate column.
        See header.
        """
        status = '{:>4}/{:4} ({:2}%)'.format(self.current_status,
                                             self.trigger,
                                             self.completion)

        if self.skip:
            missing = 'N/A'
            notes = self.notes + ' Do NOT fortify!'
        else:
            missing = '{:>4}'.format(self.missing)
            notes = self.notes

        return (self.name, status, missing, '{:.1f}%'.format(self.undermine), notes)


def make_file_engine(abs_path):
    """
    Make an sqlite file engine.

    Args:
        abs_path: Absolute path to the database.
    """
    return sqla.create_engine('sqlite:////{}'.format(abs_path), echo=False)


def drop_all_tables():
    """
    Drop all tables.
    """
    session = cogdb.Session()
    session.query(User).delete()
    session.query(System).delete()
    session.query(Fort).delete()
    session.commit()


def recreate_tables():
    """
    Recreate all tables after start.
    """
    drop_all_tables()
    Base.metadata.create_all(cogdb.mem_engine)


# Relationships
Fort.user = sqla_orm.relationship('User', back_populates='forts')
Fort.system = sqla_orm.relationship('System', back_populates='forts')
User.forts = sqla_orm.relationship('Fort',
                                   # collection_class=sqa_attr_map('system.name'),
                                   cascade='all, delete, delete-orphan',
                                   back_populates='user')
System.forts = sqla_orm.relationship('Fort',
                                     # collection_class=sqa_attr_map('user.sheet_name'),
                                     cascade='all, delete, delete-orphan',
                                     back_populates='system')


Base.metadata.create_all(cogdb.mem_engine)


def main():
    """
    Exists only as old example code.
    """
    session = cogdb.Session()

    users = (
        User(sheet_name='GearsandCogs', sheet_row=15),
        User(sheet_name='rjwhite', sheet_row=16),
        User(sheet_name='vampyregtx', sheet_row=17),
    )

    session.add_all(users)
    session.commit()

    systems = (
        System(name='Frey', sheet_col='F', sheet_order=1, fort_status=0,
               cmdr_merits=0, trigger=7400, undermine=0),
        System(name='Adeo', sheet_col='G', sheet_order=2, fort_status=0,
               cmdr_merits=0, trigger=5400, undermine=0),
        System(name='Sol', sheet_col='H', sheet_order=3, fort_status=0,
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

    def mprint(*args):
        args = [str(x) for x in args]
        print(*args)

    pad = ' ' * 3

    for user in session.query(User).order_by(User.sheet_name).all():
        mprint(user)
        mprint(pad, user.forts)

    for sys in session.query(System).order_by(System.name):
        mprint(sys)
        mprint(pad, sys.forts)

    for fort in session.query(Fort).order_by(Fort.system_id):
        mprint(fort)
        mprint(pad, fort.user)
        mprint(pad, fort.system)


if __name__ == "__main__":
    main()
