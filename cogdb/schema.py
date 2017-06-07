"""
Manage the database and its tables.
"""
from __future__ import absolute_import, print_function

import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm
import sqlalchemy.ext.declarative

import cog.exc
import cogdb


Base = sqlalchemy.ext.declarative.declarative_base()


class Command(Base):
    """
    Represents a command that was issued. Track all commands for now.
    """
    # TODO: Possible rollback/undo later of commands.
    __tablename__ = 'commands'
    id = sqla.Column(sqla.Integer, primary_key=True)
    cmd_str = sqla.Column(sqla.String)
    date = sqla.Column(sqla.DateTime)
    discord_id = sqla.Column(sqla.String, sqla.ForeignKey('dusers.discord_id'))

    def __repr__(self):
        if getattr(self, 'duser', None):
            duser = "display_name='{}'".format(self.duser.display_name)
        else:
            duser = "discord_id='{}'".format(self.discord_id)

        args = {
            'duser': duser,
            'cmd_str': self.cmd_str,
            'date': self.date,
        }
        return "<Command({duser}, cmd_str='{cmd_str}', "\
            "date='{date}')>".format(**args)

    def __str__(self):
        return "ID='{}', ".format(self.id) + self.__repr__()

    def __eq__(self, other):
        return (self.cmd_str, self.discord_id, self.date) == (
            other.cmd_str, other.discord_id, other.date)


class DUser(Base):
    """
    Database to store discord users and their permanent preferences.
    """
    __tablename__ = 'dusers'

    id = sqla.Column(sqla.Integer, primary_key=True)
    discord_id = sqla.Column(sqla.String, unique=True)
    display_name = sqla.Column(sqla.String)
    capacity = sqla.Column(sqla.Integer)
    sheet_name = sqla.Column(sqla.String, sqla.ForeignKey('susers.sheet_name'))

    def __repr__(self):
        args = {
            'discord_id': self.discord_id,
            'display_name': self.display_name,
            'capacity': self.capacity,
            'sheet_name': self.sheet_name,
        }
        return "<DUser(display_name='{display_name}', discord_id='{discord_id}', "\
            "capacity='{capacity}', sheet_name='{sheet_name}')>".format(**args)

    def __str__(self):
        return "ID='{}', ".format(self.id) + self.__repr__()

    def __eq__(self, other):
        return (self.discord_id, self.display_name, self.capacity, self.sheet_name) == (
            other.discord_id, other.display_name, other.capacity, other.sheet_name)


class SUser(Base):
    """
    Every user of bot, has an entry here. Discord name must be unique.
    """
    __tablename__ = 'susers'

    id = sqla.Column(sqla.Integer, primary_key=True)
    sheet_name = sqla.Column(sqla.String, unique=True)
    sheet_row = sqla.Column(sqla.Integer)

    def __repr__(self):
        args = {
            'sheet': self.sheet_name,
            'row': self.sheet_row,
        }
        return "<SUser(sheet_name='{sheet}', sheet_row='{row}')>".format(**args)

    def __str__(self):
        return "ID='{}', ".format(self.id) + self.__repr__()

    def __eq__(self, other):
        return (self.sheet_name, self.sheet_row) == (other.sheet_name, other.sheet_row)

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
    user_id = sqla.Column(sqla.Integer, sqla.ForeignKey('susers.id'))
    system_id = sqla.Column(sqla.String, sqla.ForeignKey('systems.id'))
    amount = sqla.Column(sqla.Integer)

    def __repr__(self):
        if getattr(self, 'suser', None):
            user = self.suser.sheet_name
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

    def __eq__(self, other):
        return (self.user_id, self.system_id, self.amount) == (
            other.user_id, other.system_id, other.amount)


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
        return self.undermine >= 0.99

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
    def table_row(self):
        """
        Return a tuple of important data to be formatted for table output.
        Each element should be mapped to separate column.
        See header.
        """
        status = '{:>4}/{:4} ({:2}%)'.format(self.current_status,
                                             self.trigger,
                                             self.completion)
        missing = 'N/A' if self.skip else '{:>4}'.format(self.missing)

        return (self.name, status, missing, '{:.1f}%'.format(self.undermine), self.notes)

    def __eq__(self, other):
        return (self.name, self.sheet_col, self.sheet_order, self.fort_status,
                self.cmdr_merits, self.trigger, self.undermine, self.notes) == (
                    other.name, other.sheet_col, other.sheet_order, other.fort_status,
                    other.cmdr_merits, other.trigger, other.undermine, other.notes)


def parse_int(word):
    try:
        return int(word)
    except ValueError:
        return 0


def parse_float(word):
    try:
        return float(word)
    except ValueError:
        return 0.0


def system_result_dict(lines, order, column):
    """
    Simple adapter that parses the data and puts it into kwargs to
    be used when initializing the System object.

    lines: A list of the following
        0   - undermine % (comes as float 0.0 - 1.0)
        1   - completion % (comes as float 0.0 - 1.0)
        2   - fortification trigger
        3   - missing merits
        4   - merits dropped by commanders
        5   - status updated manually (defaults to '', map to 0)
        6   - undermine updated manually (defaults to '', map to 0)
        7   - distance from hq (float, always set)
        8   - notes (defaults '')
        9   - system name
    order: The order of this data set relative others.
    column: The column string this data belongs in.
    """
    try:
        if lines[9] == '':
            raise cog.exc.IncorrectData

        return {
            'undermine': parse_float(lines[0]),
            'trigger': parse_int(lines[2]),
            'cmdr_merits': lines[4],
            'fort_status': parse_int(lines[5]),
            'notes': lines[8],
            'name': lines[9],
            'sheet_col': column,
            'sheet_order': order,
        }
    except (IndexError, TypeError):
        raise cog.exc.IncorrectData


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
    session.query(DUser).delete()
    session.query(SUser).delete()
    session.query(Command).delete()
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
Fort.suser = sqla_orm.relationship('SUser', uselist=False, back_populates='forts')
Fort.system = sqla_orm.relationship('System', uselist=False, back_populates='forts')
SUser.forts = sqla_orm.relationship('Fort',
                                    # collection_class=sqa_attr_map('system.name'),
                                    cascade='all, delete, delete-orphan',
                                    back_populates='suser')
System.forts = sqla_orm.relationship('Fort',
                                     # collection_class=sqa_attr_map('user.sheet_name'),
                                     cascade='all, delete, delete-orphan',
                                     back_populates='system')
DUser.cmds = sqla_orm.relationship('Command',
                                   # collection_class=sqa_attr_map('user.sheet_name'),
                                   cascade='all, delete, delete-orphan',
                                   back_populates='duser')
Command.duser = sqla_orm.relationship('DUser', back_populates='cmds')
DUser.suser = sqla_orm.relationship('SUser', uselist=False, back_populates='duser')
SUser.duser = sqla_orm.relationship('DUser', uselist=False, back_populates='suser')

Base.metadata.create_all(cogdb.mem_engine)


def main():
    """
    Exists only as old example code.
    """
    import datetime as date
    session = cogdb.Session()

    dusers = (
        DUser(discord_id='197221', display_name='GearsandCogs', capacity=0, sheet_name='GearsandCogs'),
        DUser(discord_id='299221', display_name='rjwhite', capacity=0, sheet_name='rjwhite'),
        DUser(discord_id='293211', display_name='vampyregtx', capacity=0, sheet_name='vampyregtx'),
    )
    session.add_all(dusers)
    session.commit()

    cmds = (
        Command(discord_id=dusers[0].discord_id, cmd_str='info Shepron', date=date.datetime.now()),
        Command(discord_id=dusers[0].discord_id, cmd_str='drop 700', date=date.datetime.now()),
        Command(discord_id=dusers[1].discord_id, cmd_str='ban rjwhite', date=date.datetime.now()),
    )
    session.add_all(cmds)
    session.commit()

    susers = (
        SUser(sheet_name='GearsandCogs', sheet_row=15),
        SUser(sheet_name='rjwhite', sheet_row=16),
        SUser(sheet_name='vampyregtx', sheet_row=17),
    )
    session.add_all(susers)
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
        Fort(user_id=susers[0].id, system_id=systems[0].id, amount=700),
        Fort(user_id=susers[1].id, system_id=systems[0].id, amount=700),
        Fort(user_id=susers[0].id, system_id=systems[2].id, amount=1400),
        Fort(user_id=susers[2].id, system_id=systems[1].id, amount=2100),
        Fort(user_id=susers[2].id, system_id=systems[0].id, amount=300),
    )
    session.add_all(forts)
    session.commit()

    def mprint(*args):
        args = [str(x) for x in args]
        print(*args)

    pad = ' ' * 3

    print('Commands----------')
    for cmd in session.query(Command):
        mprint(cmd)
        mprint(pad, cmd.duser)

    print('DUsers----------')
    for user in session.query(DUser):
        mprint(user)
        mprint(pad, user.suser)

    print('SUsers----------')
    for user in session.query(SUser):
        mprint(user)
        mprint(pad, user.forts)
        mprint(pad, user.duser)

    print('Systems----------')
    for sys in session.query(System):
        mprint(sys)
        mprint(pad, sys.forts)

    print('Forts----------')
    for fort in session.query(Fort):
        mprint(fort)
        mprint(pad, fort.suser)
        mprint(pad, fort.system)


if __name__ == "__main__":
    main()
