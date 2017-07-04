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
    um_id = sqla.Column(sqla.Integer, sqla.ForeignKey('um_sheet_users.id'))

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

    def set_um(self, suser):
        """
        Set the users um sheet row.
        """
        self.um_id = suser.id


class SUser(Base):
    """
    Track all infomration about the user in a row of the cattle sheet.
    """
    __tablename__ = 'sheet_users'

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True)
    name = sqla.Column(sqla.String, unique=True)
    cry = sqla.Column(sqla.String, default='')
    row = sqla.Column(sqla.Integer)

    def __repr__(self):
        keys = ['name', 'row', 'cry']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "SUser({})".format(', '.join(kwargs))

    def __str__(self):
        return "id={!r}, {!r}".format(self.id, self)

    def __eq__(self, other):
        return (self.name, self.row) == (other.name, other.row)

    @property
    def merits(self):
        total = 0
        for drop in self.drops:
            total += drop.amount

        return total


class SUserUM(Base):
    """
    Track all infomration about the user in a row of the cattle sheet.
    """
    __tablename__ = 'um_sheet_users'

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True)
    name = sqla.Column(sqla.String, unique=True)
    cry = sqla.Column(sqla.String, default='')
    row = sqla.Column(sqla.Integer)

    def __repr__(self):
        keys = ['name', 'row', 'cry']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "SUserUM({})".format(', '.join(kwargs))

    def __str__(self):
        return "id={!r}, {!r}".format(self.id, self)

    def __eq__(self, other):
        return (self.name, self.row) == (other.name, other.row)

    @property
    def merits(self):
        total = 0
        for hold in self.holds:
            total += hold.redeemed

        return total


class Drop(Base):
    """
    Every drop made by a user creates a fort entry here.
    User maintains a sub collection of these for easy access.
    """
    __tablename__ = 'merits'

    id = sqla.Column(sqla.Integer, primary_key=True)
    amount = sqla.Column(sqla.Integer)
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), nullable=False)
    user_id = sqla.Column(sqla.Integer, sqla.ForeignKey('sheet_users.id'), nullable=False)

    def __repr__(self):
        keys = ['system_id', 'user_id', 'amount']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "Drop({})".format(', '.join(kwargs))

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


class Hold(Base):
    __tablename__ = 'um_merits'

    id = sqla.Column(sqla.Integer, primary_key=True)
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('um_systems.id'), nullable=False)
    user_id = sqla.Column(sqla.Integer, sqla.ForeignKey('um_sheet_users.id'), nullable=False)
    held = sqla.Column(sqla.Integer)
    redeemed = sqla.Column(sqla.Integer)

    def __repr__(self):
        keys = ['system_id', 'user_id', 'held', 'redeemed']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "Hold({})".format(', '.join(kwargs))

    def __str__(self):
        system = ''
        if getattr(self, 'system', None):
            system = "system_name={!r}, ".format(self.system.name)

        suser = ''
        if getattr(self, 'suser', None):
            suser = "suser_name={!r}, ".format(self.suser.name)

        return "id={!r}, {}{}{!r}".format(self.id, system, suser, self)

    def __eq__(self, other):
        return (self.system_id, self.user_id, self.held, self.redeemed) == (
            other.system_id, other.user_id, other.held, other.redeemed)


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


class SystemUM(Base):
    """
    Any system we intend on undermining, may be a simple enemy
    control system or an expansion we want to support/oppose.
    """
    __tablename__ = 'um_systems'

    # Enum
    T_CONTROL = 'control'
    T_OPPOSE = 'oppose'
    T_EXPAND = 'expand'

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String, unique=True)
    sheet_col = sqla.Column(sqla.String)
    completion = sqla.Column(sqla.Float)
    goal = sqla.Column(sqla.Integer)
    cmdr_merits = sqla.Column(sqla.Integer)
    missing = sqla.Column(sqla.Integer)
    security = sqla.Column(sqla.String)
    notes = sqla.Column(sqla.String)
    close_control = sqla.Column(sqla.String)
    progress_us = sqla.Column(sqla.Integer)
    progress_them = sqla.Column(sqla.Float)

    # Expansion/Opposition only, top cells
    trigger = sqla.Column(sqla.Integer)
    margin = sqla.Column(sqla.Float)
    type = sqla.Column(sqla.Integer)  # Some subtle differences, not enough for subclasses

    def __repr__(self):
        keys = ['name', 'type', 'sheet_col', 'completion', 'goal', 'cmdr_merits', 'missing',
                'progress_us', 'progress_them', 'trigger', 'margin',
                'security', 'notes', 'close_control']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "SystemUM({})".format(', '.join(kwargs))

    def __str__(self):
        return "id={!r}, {!r}".format(self.id, self)

    def __eq__(self, other):
        return (self.name, self.completion, self.goal, self.cmdr_merits, self.missing) == (
            other.name, other.completion, other.goal, other.cmdr_merits, other.missing)


def kwargs_um_system(cells, sheet_col):
    """
    Return keyword args parsed from cell frame.

    Format !D1:E11:
        1: Title | Title
        2: Exp Trigger/Opp. Tigger | % safety margin  -> If cells blank, not expansion system.
        3: Leading by xx% OR behind by xx% (
        4: Estimated Goal (integer)
        5: CMDR Merits (Total merits)
        6: Missing Merits
        7: Security Level | Notes
        8: Closest Control (string)
        9: System Name (string)
        10: Our Progress (integer) | Type String (Ignore)
        11: Enemy Progress (percentage) | Type String (Ignore)
    """
    main_col, sec_col = cells[0], cells[1]
    try:
        if main_col[8] == '' or 'template' in main_col[8].lower():
            raise cog.exc.SheetParsingError

        if main_col[0].startswith('Exp.'):
            sys_type = SystemUM.T_EXPAND
        elif main_col[0] != '':
            sys_type = SystemUM.T_OPPOSE
        else:
            sys_type = SystemUM.T_CONTROL

        return {
            'trigger': parse_int(main_col[1]),
            'margin': parse_float(sec_col[1]),
            'completion': parse_float(main_col[2].split()[-1][:-1]) / 100,
            'goal': parse_int(main_col[3]),
            'cmdr_merits': parse_int(main_col[4]),
            'missing': parse_int(main_col[5]),
            'security': main_col[6],
            'notes': sec_col[6],
            'close_control': main_col[7],
            'name': main_col[8],
            'progress_us': parse_int(main_col[9]),
            'progress_them': parse_float(main_col[10]),
            'sheet_col': sheet_col,
            'type': sys_type,
        }
    except (IndexError, TypeError):
        raise cog.exc.SheetParsingError


def kwargs_fort_system(lines, order, column):
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
    for cls in [Drop, Hold, Command, System, SystemUM, SUser, SUserUM, DUser]:
        session.query(cls).delete()
    session.commit()


def drop_scanned_tables():
    """
    Drop only tables related to data parsed from sheet.
    """
    session = cogdb.Session()
    for cls in [Drop, Hold, System, SystemUM, SUser, SUserUM]:
        session.query(cls).delete()
    session.commit()


def recreate_tables():
    """
    Recreate all tables after start.
    """
    drop_all_tables()
    Base.metadata.create_all(cogdb.mem_engine)


# Relationships
DUser.cmds = sqla_orm.relationship('Command',
                                   # collection_class=sqa_attr_map('user.name'),
                                   cascade='all, delete, delete-orphan',
                                   back_populates='duser')
Command.duser = sqla_orm.relationship('DUser', back_populates='cmds')

# Fortification relations
DUser.suser = sqla_orm.relationship('SUser', uselist=False, back_populates='duser')
SUser.duser = sqla_orm.relationship('DUser', uselist=False, back_populates='suser')
Drop.suser = sqla_orm.relationship('SUser', uselist=False, back_populates='drops')
SUser.drops = sqla_orm.relationship('Drop',
                                    # collection_class=sqa_attr_map('system.name'),
                                    cascade='all, delete, delete-orphan',
                                    back_populates='suser')
Drop.system = sqla_orm.relationship('System', uselist=False, back_populates='drops')
System.drops = sqla_orm.relationship('Drop',
                                     # collection_class=sqa_attr_map('user.name'),
                                     cascade='all, delete, delete-orphan',
                                     back_populates='system')

# Undermining relations
DUser.suserum = sqla_orm.relationship('SUserUM', uselist=False, back_populates='duser')
SUserUM.duser = sqla_orm.relationship('DUser', uselist=False, back_populates='suserum')
Hold.suser = sqla_orm.relationship('SUserUM', uselist=False, back_populates='holds')
SUserUM.holds = sqla_orm.relationship('Hold',
                                      # collection_class=sqa_attr_map('system.name'),
                                      cascade='all, delete, delete-orphan',
                                      back_populates='suser')
Hold.system = sqla_orm.relationship('SystemUM', uselist=False, back_populates='holds')
SystemUM.holds = sqla_orm.relationship('Hold',
                                       # collection_class=sqa_attr_map('user.name'),
                                       cascade='all, delete, delete-orphan',
                                       back_populates='system')


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

    susers_um = (
        SUserUM(name='GearsandCogs', row=20),
        SUserUM(name='rjwhite', row=13),
        SUserUM(name='vampyregtx', row=16),
    )

    session.add_all(susers + susers_um)
    session.commit()
    for ind in range(0, 3):
        dusers[ind].set_cattle(susers[ind])
        dusers[ind].set_um(susers_um[ind])
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

    drops = (
        Drop(user_id=susers[0].id, system_id=systems[0].id, amount=700),
        Drop(user_id=susers[1].id, system_id=systems[0].id, amount=700),
        Drop(user_id=susers[0].id, system_id=systems[2].id, amount=1400),
        Drop(user_id=susers[2].id, system_id=systems[1].id, amount=2100),
        Drop(user_id=susers[2].id, system_id=systems[0].id, amount=300),
    )
    session.add_all(drops)
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
        mprint(pad, user.drops)
        mprint(pad, user.duser)

    print('Systems----------')
    for sys in session.query(System):
        mprint(sys)
        mprint(pad, sys.drops)

    print('Drops----------')
    for drop in session.query(Drop):
        mprint(drop)
        mprint(pad, drop.suser)
        mprint(pad, drop.system)


if __name__ == "__main__":
    main()
