"""
Manage the database and its tables.
"""
from __future__ import absolute_import, print_function
import functools

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
    __tablename__ = 'commands'

    id = sqla.Column(sqla.Integer, primary_key=True)
    cmd_str = sqla.Column(sqla.String)
    date = sqla.Column(sqla.DateTime)
    discord_id = sqla.Column(sqla.String, sqla.ForeignKey('discord_users.discord_id'))

    def __repr__(self):
        args = {}
        for key in ['cmd_str', 'date', 'discord_id']:
            args[key] = getattr(self, key)

        return self.__class__.__name__ + "(discord_id={discord_id!r}, cmd_str={cmd_str!r}, "\
            "date={date!r})".format(**args)

    def __str__(self):
        duser = ''
        if getattr(self, 'duser', None):
            duser = "display_name={!r}, ".format(self.duser.display_name)

        return "id={}, {}{!r}".format(self.id, duser, self)

    def __eq__(self, other):
        return (self.cmd_str, self.discord_id, self.date) == (
            other.cmd_str, other.discord_id, other.date)


class DUser(Base):
    """
    Database to store discord users and their permanent preferences.
    """
    __tablename__ = 'discord_users'

    discord_id = sqla.Column(sqla.String, primary_key=True)
    display_name = sqla.Column(sqla.String)
    pref_name = sqla.Column(sqla.String)  # pref_name == display_name until user changes it
    capacity = sqla.Column(sqla.Integer, default=0)
    cattle_id = sqla.Column(sqla.Integer, sqla.ForeignKey('sheet_users.id'))

    def __repr__(self):
        keys = ['discord_id', 'display_name', 'pref_name', 'capacity', 'cattle_id']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "DUser({})".format(', '.join(kwargs))

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        return self.discord_id == other.discord_id

    def set_cattle(self, suser):
        """
        Set the users cattle sheet row.
        """
        self.cattle_id = suser.id


class SUser(Base):
    """
    Track all infomration about the user in a row of the cattle sheet.
    """
    __tablename__ = 'sheet_users'

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String, unique=True)
    cry = sqla.Column(sqla.String, default='')
    row = sqla.Column(sqla.Integer)

    def __repr__(self):
        args = {}
        for key in ['name', 'row', 'cry']:
            args[key] = getattr(self, key)

        return "SUser(name={name!r}, row={row!r}, cry={cry!r})".format(**args)

    def __str__(self):
        return "id={!r}, {!r}".format(self.id, self)

    def __eq__(self, other):
        return (self.name, self.row) == (other.name, other.row)

    @property
    def merits(self):
        return functools.reduce(lambda x, y: x.amount + y.amount, self.forts)


class Fort(Base):
    """
    Every drop made by a user creates a fort entry here.
    User maintains a sub collection of these for easy access.
    """
    __tablename__ = 'forts'

    id = sqla.Column(sqla.Integer, primary_key=True)
    amount = sqla.Column(sqla.Integer)
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'))
    user_id = sqla.Column(sqla.Integer, sqla.ForeignKey('sheet_users.id'))

    def __repr__(self):
        keys = ['system_id', 'user_id', 'amount']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "Fort({})".format(', '.join(kwargs))

    def __str__(self):
        system = ''
        if getattr(self, 'system', None):
            system = "system_name={!r}, ".format(self.system.name)

        suser = ''
        if getattr(self, 'suser', None):
            suser = "suser_name={!r}, ".format(self.suser.name)

        return "id={!r}, {}{}{!r}".format(self.id, system, suser, self)

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
        fort_status: Current reported status from galmap/users. (int)
        cmdr_merits: Total merits dropped by cmdrs. (int)
        trigger: Total trigger of merits required. (int)
        undermine: Percentage of undermining of the system. (float)
        notes: Any notes attached to the system. (string)
        sheet_col: The name of the column in the excel. (string)
        sheet_order: Order systems should be ordered. (int)
    """
    __tablename__ = 'systems'

    header = ['System', 'Missing', 'Merits (Fort%/UM%)', 'Notes']

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String, unique=True)
    cmdr_merits = sqla.Column(sqla.Integer)
    fort_status = sqla.Column(sqla.Integer)
    trigger = sqla.Column(sqla.Integer)
    um_status = sqla.Column(sqla.Integer, default=0)
    undermine = sqla.Column(sqla.Float, default=0.0)
    distance = sqla.Column(sqla.Float)
    notes = sqla.Column(sqla.String, default='')
    sheet_col = sqla.Column(sqla.String)
    sheet_order = sqla.Column(sqla.Integer)

    def __repr__(self):
        keys = ['name', 'cmdr_merits', 'fort_status', 'trigger', 'um_status',
                'undermine', 'distance', 'notes', 'sheet_col', 'sheet_order']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "System({})".format(', '.join(kwargs))

    def __str__(self):
        return "id={!r}, {!r}".format(self.id, self)

    def __eq__(self, other):
        return (self.name, self.fort_status, self.cmdr_merits, self.trigger,
                self.undermine, self.um_status, self.distance, self.notes) == (
                    other.name, other.fort_status, other.cmdr_merits, other.trigger,
                    other.undermine, self.um_status, self.distance, other.notes)

    @property
    def ump(self):
        """ Return the undermine percentage, stored as decimal. """
        return '{:.1f}'.format(self.undermine * 100)

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
        return self.undermine >= 1.00

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
        status = '{:>4}/{:4} ({}%/{}%)'.format(self.current_status, self.trigger,
                                               self.completion, self.ump)

        return (self.name, '{:>4}'.format(self.missing), status, self.notes)

    def set_status(self, new_status):
        """
        Update the fort_status and um_status of this System based on new_status.
        Format of new_status: fort_status[:um_status]
        """
        for val, attr in zip(new_status.split(':'), ['fort_status', 'um_status']):
            setattr(self, attr, val)

    def short_display(self, missing=True):
        """
        Return a useful short representation of System.
        """
        msg = '{} :Fortifying: {}/{}'.format(self.name, self.current_status, self.trigger)

        if missing and self.missing and self.missing < 1400:
            msg += '\nMissing: ' + str(self.missing)

        return msg


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
            raise cog.exc.SheetParsingError

        return {
            'undermine': parse_float(lines[0]),
            'trigger': parse_int(lines[2]),
            'cmdr_merits': lines[4],
            'fort_status': parse_int(lines[5]),
            'um_status': parse_int(lines[6]),
            'distance': parse_float(lines[7]),
            'notes': lines[8],
            'name': lines[9],
            'sheet_col': column,
            'sheet_order': order,
        }
    except (IndexError, TypeError):
        raise cog.exc.SheetParsingError


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
    for cls in [Fort, Command, SUser, System, DUser]:
        session.query(cls).delete()
    session.commit()


def drop_scanned_tables():
    """
    Drop only tables related to data parsed from sheet.
    """
    session = cogdb.Session()
    for cls in [Fort, SUser, System]:
        session.query(cls).delete()
    session.commit()


def recreate_tables():
    """
    Recreate all tables after start.
    """
    drop_all_tables()
    Base.metadata.create_all(cogdb.mem_engine)


# Relationships
Fort.suser = sqla_orm.relationship('SUser', uselist=False, back_populates='forts')
SUser.forts = sqla_orm.relationship('Fort',
                                    # collection_class=sqa_attr_map('system.name'),
                                    cascade='all, delete, delete-orphan',
                                    back_populates='suser')
Fort.system = sqla_orm.relationship('System', uselist=False, back_populates='forts')
System.forts = sqla_orm.relationship('Fort',
                                     # collection_class=sqa_attr_map('user.name'),
                                     cascade='all, delete, delete-orphan',
                                     back_populates='system')
DUser.cmds = sqla_orm.relationship('Command',
                                   # collection_class=sqa_attr_map('user.name'),
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
        DUser(discord_id='197221', pref_name='GearsandCogs', capacity=0),
        DUser(discord_id='299221', pref_name='rjwhite', capacity=0),
        DUser(discord_id='293211', pref_name='vampyregtx', capacity=0),
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
        SUser(name='GearsandCogs', row=15),
        SUser(name='rjwhite', row=16),
        SUser(name='vampyregtx', row=17),
    )

    session.add_all(susers)
    session.commit()
    for ind in range(0, 3):
        dusers[ind].set_cattle(susers[ind])
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

    print('DiscordUsers----------')
    for user in session.query(DUser):
        mprint(user)
        mprint(pad, user.suser)

    print('SheetUsers----------')
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
