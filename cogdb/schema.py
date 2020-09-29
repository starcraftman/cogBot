"""
Define the database schema and some helpers.

N.B. Schema defaults only applied once object commited.
"""
import datetime

import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm
import sqlalchemy.ext.declarative

import cog.exc
import cog.tbl
import cogdb


# TODO: System hierarchy mapped to single table. Fair bit of overlap here.
# Example
# SystemBase --> SystemFort -> SystemPrep
#           \--> UMSystem  --> UMControl
#                         \--> UMExpand --> UMOppose
# TODO: Maybe make Merit -> FortMerit, UMMerit

LEN_CMD = 15  # Max length of a subclass of cog.actions
LEN_DID = 30
LEN_NAME = 100
LEN_FACTION = 10
LEN_SHEET = 15
Base = sqlalchemy.ext.declarative.declarative_base()


class EUMType():
    """ Type of undermine system. """
    control = 'control'
    expand = 'expanding'
    oppose = 'opposing'


class EFortType():
    """ Type of fort system. """
    fort = 'fort'
    prep = 'prep'


class PermAdmin(Base):
    """
    Table that lists admins. Essentially just a boolean.
    All admins are equal, except for removing other admins, then seniority is considered by date.
    This shouldn't be a problem practically.
    """
    __tablename__ = 'perms_admins'

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    date = sqla.Column(sqla.DateTime, default=datetime.datetime.now(datetime.timezone.utc))  # All dates UTC

    def remove(self, session, other):
        """
        Remove an existing admin.
        """
        if self.date > other.date:
            raise cog.exc.InvalidPerms("You are not the senior admin. Refusing.")
        session.delete(other)
        session.commit()

    def __repr__(self):
        keys = ['id', 'date']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return self.id == other.id


class PermChannel(Base):
    """
    A channel permission to restrict cmd to listed channels.
    """
    __tablename__ = 'perms_channels'

    cmd = sqla.Column(sqla.String(LEN_CMD), primary_key=True)
    server = sqla.Column(sqla.String(30), primary_key=True)
    channel = sqla.Column(sqla.String(40), primary_key=True)

    def __repr__(self):
        keys = ['cmd', 'server', 'channel']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        return repr(self)

    def __eq__(self, other):
        return isinstance(other, PermChannel) and (str(self) == str(other))


class PermRole(Base):
    """
    A role permission to restrict cmd to listed roles.
    """
    __tablename__ = 'perms_roles'

    cmd = sqla.Column(sqla.String(LEN_CMD), primary_key=True)
    server = sqla.Column(sqla.String(30), primary_key=True)
    role = sqla.Column(sqla.String(40), primary_key=True)

    def __repr__(self):
        keys = ['cmd', 'server', 'role']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        return repr(self)

    def __eq__(self, other):
        return isinstance(other, PermRole) and (str(self) == str(other))


class DUser(Base):
    """
    Table to store discord users and their permanent preferences.

    These are what the user would prefer be put into sheets.
    """
    __tablename__ = 'discord_users'

    id = sqla.Column(sqla.BigInteger, primary_key=True)  # Discord id
    display_name = sqla.Column(sqla.String(LEN_NAME))
    pref_name = sqla.Column(sqla.String(LEN_NAME), unique=True, nullable=False)  # pref_name == display_name until change
    pref_cry = sqla.Column(sqla.String(LEN_NAME), default='')

    # Relationships
    fort_user = sqla.orm.relationship('FortUser', uselist=False)
    um_user = sqla.orm.relationship('UMUser', uselist=False)

    def __repr__(self):
        keys = ['id', 'display_name', 'pref_name', 'pref_cry', 'faction']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "DUser({})".format(', '.join(kwargs))

    def __str__(self):
        return repr(self)

    def __eq__(self, other):
        return isinstance(other, DUser) and self.id == other.id

    @property
    def mention(self):
        """ Mention this user in a response. """
        return "<@" + self.id + ">"

    #  def sheets(self, session):
        #  """ Return all sheets found. """
        #  return session.query(SheetRow).filter_by(name=self.pref_name).all()

    #  def get_sheet(self, session, sheet_type, *, faction=None):
        #  """
        #  Get a sheet belonging to a certain type. See ESheetType.

        #  Returns a SheetRow subclass. None if not set.
        #  """
        #  if not faction:
            #  faction = self.faction

        #  for sheet in self.sheets(session):
            #  if sheet.type == sheet_type and sheet.faction == faction:
                #  return sheet

        #  return None


class FortUser(Base):
    """
    Track all infomration about the user in a row of the cattle sheet.

    These are what actually is in the sheet.
    """
    __tablename__ = 'hudson_fort_users'

    id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('discord_users.id'),
                     primary_key=True,)  # Discord id
    name = sqla.Column(sqla.String(LEN_NAME), unique=True)
    row = sqla.Column(sqla.Integer, unique=True)
    cry = sqla.Column(sqla.String(LEN_NAME), default='')

    # Relationships
    merits = sqla_orm.relationship('Drop',
                                   cascade='all, delete, delete-orphan',
                                   back_populates='hudson_fort_users',
                                   lazy='select')

    def __repr__(self):
        keys = ['id', 'name', 'row', 'cry']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(other, FortUser) and self.id == other.id

    def merit_summary(self):
        """ Summarize user merits. """
        total = 0
        for drop in self.merits:
            total += drop.amount

        return 'Dropped {}'.format(total)


class FortDrop(Base):
    """
    Every drop made by a user creates a fort entry here.
    User maintains a sub collection of these for easy access.
    """
    __tablename__ = 'hudson_fort_merits'

    id = sqla.Column(sqla.Integer, primary_key=True)
    amount = sqla.Column(sqla.Integer)
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('hudson_fort_systems.id'), nullable=False)
    user_id = sqla.Column(sqla.Integer, sqla.ForeignKey('hudson_fort_users.id'), nullable=False)

    # Relationships
    user = sqla_orm.relationship('FortUser', uselist=False, back_populates='hudson_fort_merits',
                                 lazy='select')
    system = sqla_orm.relationship('FortSystem', uselist=False, back_populates='hudson_fort_merits',
                                   lazy='select')

    def __repr__(self):
        keys = ['system_id', 'user_id', 'amount']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        system = ''
        if getattr(self, 'system'):
            system = "system={!r}, ".format(self.system.name)

        suser = ''
        if getattr(self, 'user'):
            suser = "user={!r}, ".format(self.user.name)

        return "id={!r}, {}{}{!r}".format(self.id, system, suser, self)

    def __eq__(self, other):
        return isinstance(other, FortDrop) and (self.user_id, self.system_id) == (
            other.user_id, other.system_id)

    def __lt__(self, other):
        return self.amount < other.amount


class FortSystem(Base):
    """
    Represent a single system for fortification.
    Object can be flushed and queried from the database.

    data: List to be unpacked: ump, trigger, status, notes):
    Data tuple is to be used to make a table, with header

    args:
        id: Set by the database, unique id.
        name: Name of the system. (string)
        fort_status: Current reported status from galmap/users. (int)
        trigger: Total trigger of merits required. (int)
        undermine: Percentage of undermining of the system. (float)
        notes: Any notes attached to the system. (string)
        sheet_col: The name of the column in the excel. (string)
        sheet_order: Order systems should be ordered. (int)
    """
    __tablename__ = 'hudson_fort_systems'

    header = ['Type', 'System', 'Missing', 'Merits (Fort%/UM%)', 'Notes']

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN_NAME), unique=True)
    fort_status = sqla.Column(sqla.Integer)
    trigger = sqla.Column(sqla.Integer)
    um_status = sqla.Column(sqla.Integer, default=0)
    undermine = sqla.Column(sqla.Float, default=0.0)
    fort_override = sqla.Column(sqla.Float, default=0.0)
    distance = sqla.Column(sqla.Float)
    notes = sqla.Column(sqla.String(LEN_NAME), default='')
    sheet_col = sqla.Column(sqla.String(5))
    sheet_order = sqla.Column(sqla.Integer)
    type = sqla.Column(sqla.String(5))

    __mapper_args__ = {
        'polymorphic_identity': EFortType.fort,
        'polymorphic_on': type
    }

    def __repr__(self):
        keys = ['name', 'fort_status', 'trigger', 'fort_override', 'um_status',
                'undermine', 'distance', 'notes', 'sheet_col', 'sheet_order']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "System({})".format(', '.join(kwargs))

    def __str__(self):
        return "id={!r}, cmdr_merits={!r}, {!r}".format(self.id, self.cmdr_merits, self)

    def __eq__(self, other):
        return isinstance(other, FortSystem) and self.name == other.name

    def __lt__(self, other):
        """ Order systems by remaining supplies needed. """
        return isinstance(other, self.__class__) and self.missing < other.missing

    @property
    def cmdr_merits(self):
        """ Total merits dropped by cmdrs """
        total = 0
        for drop in self.merits:
            total += drop.amount
        return total

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
        return self.fort_override >= 1.0 or self.current_status >= self.trigger

    # TODO: Make this useful in queries to db.
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
        status = '{:>4}/{} ({}%/{}%)'.format(self.current_status, self.trigger,
                                             self.completion, self.ump)

        return (self.type.capitalize(), self.name,
                '{:>4}'.format(self.missing), status, self.notes)

    def set_status(self, new_status):
        """
        Update the fort_status and um_status of this System based on new_status.
        Format of new_status: fort_status[:um_status]

        Raises: ValueError
        """
        for val, attr in zip(new_status.split(':'), ['fort_status', 'um_status']):
            new_val = int(val)
            if new_val < 0:
                raise cog.exc.InvalidCommandArgs('New fort/um status must be in range: [0, \u221E]')

            setattr(self, attr, int(val))

    def display(self, *, miss=None):
        """
        Return a useful short representation of System.

        Kwargs:
            missing: A trinary:
                - None, show missing only if < 1500 left
                - True, display missing
                - False, do not display missing
        """
        umd = ''
        if self.um_status > 0:
            umd = ', {} :Undermin{}:'.format(
                self.um_status, 'ed' if self.is_undermined else 'ing')
        elif self.is_undermined:
            umd = ', :Undermined:'

        msg = '**{}** {:>4}/{} :Fortif{}:{}'.format(
            self.name, self.current_status, self.trigger,
            'ied' if self.is_fortified else 'ying', umd)

        if miss or miss is not False and (self.missing and self.missing < 1500):
            msg += ' ({} left)'.format(self.missing)

        if self.notes:
            msg += ' ' + self.notes

        return msg

    def display_details(self):
        """ Return a highly detailed system display. """
        miss = ' ({} left)'.format(self.missing) if self.missing else ''
        lines = [
            ['Completion', '{}%{}'.format(self.completion, miss)],
            ['CMDR Merits', '{}/{}'.format(self.cmdr_merits, self.trigger)],
            ['Fort Status', '{}/{}'.format(self.fort_status, self.trigger)],
            ['UM Status', '{} ({:.2f}%)'.format(self.um_status, self.undermine * 100)],
            ['Notes', self.notes],
        ]

        return '**{}**\n'.format(self.name) + cog.tbl.wrap_markdown(cog.tbl.format_table(lines))


class FortPrep(FortSystem):
    """
    A prep system that must be fortified for expansion.
    """
    __mapper_args__ = {
        'polymorphic_identity': EFortType.prep,
    }

    @property
    def is_fortified(self):
        """ Prep systems never get finished. """
        return False

    def display(self, *, miss=None):
        """
        Return a useful short representation of PrepSystem.
        """
        return 'Prep: ' + super().display(miss=miss)


class FortOrder(Base):
    """
    Simply store a list of Control systems in the order they should be forted.
    """
    __tablename__ = 'fort_order'

    order = sqla.Column(sqla.Integer, unique=True)
    system_name = sqla.Column(sqla.String(LEN_NAME), primary_key=True)

    def __repr__(self):
        keys = ['order', 'system_name']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "FortOrder({})".format(', '.join(kwargs))

    def __str__(self):
        return repr(self)

    def __eq__(self, other):
        return isinstance(self, FortOrder) and isinstance(other, FortOrder) and (
            str(self) == str(other))


class UMUser(Base):
    """
    Track all infomration about the user in a row of the cattle sheet.
    """
    __tablename__ = 'hudson_um_users'

    id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('discord_users.id'),
                     primary_key=True,)  # Discord id
    name = sqla.Column(sqla.String(LEN_NAME), unique=True)
    row = sqla.Column(sqla.Integer, unique=True)
    cry = sqla.Column(sqla.String(LEN_NAME), default='')

    # Relationships
    merits = sqla_orm.relationship('UMDrop',
                                   cascade='all, delete, delete-orphan',
                                   back_populates='hudson_um_users',
                                   lazy='select')

    def __repr__(self):
        keys = ['id', 'name', 'row', 'cry']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(other, UMUser) and self.id == other.id

    def merit_summary(self):
        """ Summarize user merits. """
        held = 0
        redeemed = 0
        for hold in self.merits:
            held += hold.held
            redeemed += hold.redeemed

        return 'Holding {}, Redeemed {}'.format(held, redeemed)


class UMHold(Base):
    """
    Represents a user's held and redeemed merits within an undermining system.
    """
    __tablename__ = 'hudson_um_merits'

    id = sqla.Column(sqla.Integer, primary_key=True)
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('hudson_um_systems.id'), nullable=False)
    user_id = sqla.Column(sqla.Integer, sqla.ForeignKey('hudson_um_users.id'), nullable=False)
    held = sqla.Column(sqla.Integer)
    redeemed = sqla.Column(sqla.Integer)

    # Relationships
    user = sqla_orm.relationship('UMUser', uselist=False, back_populates='hudson_um_merits',
                                 lazy='select')
    system = sqla_orm.relationship('UMSystem', uselist=False, back_populates='hudson_um_merits',
                                   lazy='select')


    def __repr__(self):
        keys = ['system_id', 'user_id', 'held', 'redeemed']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "Hold({})".format(', '.join(kwargs))

    def __str__(self):
        system = ''
        if getattr(self, 'system', None):
            system = "system={!r}, ".format(self.system.name)

        suser = ''
        if getattr(self, 'user', None):
            suser = "user={!r}, ".format(self.user.name)

        return "id={!r}, {}{}{!r}".format(self.id, system, suser, self)

    def __eq__(self, other):
        return isinstance(other, UMHold) and (self.user_id, self.system_id) == (
            other.user_id, other.system_id)

    def __lt__(self, other):
        return self.held + self.redeemed < other.held + other.redeemed


class UMSystem(Base):
    """
    A control system we intend on undermining.
    """
    __tablename__ = 'hudson_um_systems'

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN_NAME), unique=True)
    type = sqla.Column(sqla.String(15))  # EUMType
    sheet_col = sqla.Column(sqla.String(5))
    goal = sqla.Column(sqla.Integer)
    security = sqla.Column(sqla.String(LEN_NAME))
    notes = sqla.Column(sqla.String(LEN_NAME))
    close_control = sqla.Column(sqla.String(LEN_NAME))
    priority = sqla.Column(sqla.String(LEN_NAME))
    progress_us = sqla.Column(sqla.Integer)
    progress_them = sqla.Column(sqla.Float)
    map_offset = sqla.Column(sqla.Integer, default=0)
    exp_trigger = sqla.Column(sqla.Integer, default=0)

    __mapper_args__ = {
        'polymorphic_identity': 'base_system',
        'polymorphic_on': type
    }

    # Relationships
    merits = sqla_orm.relationship('UMHold',
                                   cascade='all, delete, delete-orphan',
                                   back_populates='hudson_um_systems',
                                   lazy='select')

    @staticmethod
    def factory(kwargs):
        """ Simple factory to make undermining systems. """
        cls = kwargs.pop('cls')
        return cls(**kwargs)

    def __repr__(self):
        keys = ['name', 'type', 'sheet_col', 'goal', 'security', 'notes',
                'progress_us', 'progress_them', 'close_control', 'priority', 'map_offset']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "UMSystem({})".format(', '.join(kwargs))

    def __str__(self):
        """
        Show additional computed properties.
        """
        return "id={!r}, cmdr_merits={!r}, {!r}".format(self.id, self.cmdr_merits, self)

    def __eq__(self, other):
        return isinstance(other, UMSystem) and self.name == other.name

    @property
    def completion(self):
        """ The completion percentage formatted as a string """
        try:
            comp_cent = (self.goal - self.missing) / self.goal * 100
        except ZeroDivisionError:
            comp_cent = 0

        completion = '{:.0f}%'.format(comp_cent)

        return completion

    @property
    def cmdr_merits(self):
        """ Total merits held and redeemed by cmdrs """
        total = 0
        for hold in self.merits:
            total += hold.held + hold.redeemed
        return total

    @property
    def missing(self):
        """ The remaining supplies to fortify """
        return self.goal - max(self.cmdr_merits + self.map_offset, self.progress_us)

    @property
    def descriptor(self):
        """ Descriptive prefix for string. """
        return self.type.capitalize()

    @property
    def is_undermined(self):
        """
        Return true only if the system is undermined.
        """
        return self.missing <= 0

    def display(self):
        """
        Format a simple summary for users.
        """
        lines = [
            [self.descriptor, '{} [{} sec]'.format(self.name, self.security[0].upper())],
            [self.completion, 'Merits {} {}'.format('Missing' if self.missing > 0 else 'Leading',
                                                    str(abs(self.missing)))],
            ['Our Progress ' + str(self.progress_us),
             'Enemy Progress {:.0f}%'.format(self.progress_them * 100)],
            ['Nearest Hudson', self.close_control],
            ['Priority', self.priority],
        ]

        return cog.tbl.wrap_markdown(cog.tbl.format_table(lines))

    def set_status(self, new_status):
        """
        Update the fort_status and um_status of this System based on new_status.
        Format of new_status: fort_status[:um_status]

        Raises: ValueError
        """
        vals = new_status.split(':')
        if len(vals) == 2:
            new_them = float(vals[1]) / 100
            if new_them < 0:
                raise cog.exc.InvalidCommandArgs('New "progress them" must be a % in range: [0, \u221E]')
            self.progress_them = new_them

        new_us = int(vals[0])
        if new_us < 0:
            raise cog.exc.InvalidCommandArgs('New "progress us" must be a number merits in range: [0, \u221E]')
        self.progress_us = new_us


class UMControl(UMSystem):
    """ Undermine an enemy control system. """
    __mapper_args__ = {
        'polymorphic_identity': EUMType.control,
    }


class UMExpand(UMSystem):
    """ An expansion we want. """
    __mapper_args__ = {
        'polymorphic_identity': EUMType.expand,
    }

    @property
    def is_undermined(self):
        """
        Expansions are never finished until tick.
        """
        return False

    @property
    def completion(self):
        """ The completion percentage formatted as a string """
        try:
            comp_cent = max(self.progress_us,
                            self.cmdr_merits + self.map_offset) * 100 / self.exp_trigger
        except ZeroDivisionError:
            comp_cent = 0

        comp_cent -= self.progress_them * 100
        prefix = 'Leading by' if comp_cent >= 0 else 'Behind by'
        completion = '{} {:.0f}%'.format(prefix, abs(comp_cent))

        return completion


class UMOppose(UMExpand):
    """ We want to oppose the expansion. """
    __mapper_args__ = {
        'polymorphic_identity': EUMType.oppose,
    }

    @property
    def descriptor(self):
        """ Descriptive prefix for string. """
        suffix = 'expansion'
        if self.notes != '':
            suffix = self.notes.split()[0]
        return 'Opposing ' + suffix


class KOS(Base):
    """
    Represents a the kos list.
    """
    __tablename__ = 'kos'

    id = sqla.Column(sqla.Integer, primary_key=True)
    cmdr = sqla.Column(sqla.String(100), unique=True, nullable=False)
    faction = sqla.Column(sqla.String(100), nullable=False)
    danger = sqla.Column(sqla.Integer)
    is_friendly = sqla.Column(sqla.Boolean)

    def __repr__(self):
        keys = ['cmdr', 'faction', 'danger', 'is_friendly']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "KOS({})".format(', '.join(kwargs))

    def __str__(self):
        return "id={!r}, {!r}".format(self.id, self)

    def __eq__(self, other):
        return isinstance(other, KOS) and (self.cmdr) == (other.cmdr)

    def __hash__(self):
        return hash(self.cmdr)

    @property
    def friendly(self):
        return 'FRIENDLY' if self.is_friendly else 'KILL'


def kwargs_um_system(cells, sheet_col):
    """
    Return keyword args parsed from cell frame.

    Format !D1:E13:
        1: Title | Title
        2: Exp Trigger/Opp. Tigger | % safety margin  -> If cells blank, not expansion system.
        3: Leading by xx% OR behind by xx% (
        4: Estimated Goal (integer)
        5: CMDR Merits (Total merits)
        6: Missing Merits
        7: Security Level | Notes
        8: Closest Control (string) | priority (string)
        9: System Name (string)
        10: Our Progress (integer) | Type String (Ignore)
        11: Enemy Progress (percentage) | Type String (Ignore)
        12: Skip
        13: Map Offset (Map Value - Cmdr Merits)
    """
    try:
        main_col, sec_col = cells[0], cells[1]

        if main_col[8] == '' or 'template' in main_col[8].lower():
            raise cog.exc.SheetParsingError("Halt UMSystem parsing.")

        if main_col[0].startswith('Exp'):
            cls = UMExpand
        elif main_col[0] != '':
            cls = UMOppose
        else:
            cls = UMControl

        # Cell is not guaranteed to exist in list
        try:
            map_offset = parse_int(main_col[12])
        except IndexError:
            map_offset = 0

        return {
            'exp_trigger': parse_int(main_col[1]),
            'goal': parse_int(main_col[3]),
            'security': main_col[6].strip().replace('Sec: ', ''),
            'notes': sec_col[6].strip(),
            'close_control': main_col[7].strip(),
            'priority': sec_col[7].strip(),
            'name': main_col[8].strip(),
            'progress_us': parse_int(main_col[9]),
            'progress_them': parse_percent(main_col[10]),
            'map_offset': map_offset,
            'sheet_col': sheet_col,
            'cls': cls,
        }
    except (IndexError, TypeError):
        raise cog.exc.SheetParsingError("Halt UMSystem parsing.")


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
            raise cog.exc.SheetParsingError("Halt System parsing.")

        return {
            'undermine': parse_percent(lines[0]),
            'fort_override': parse_percent(lines[1]),
            'trigger': parse_int(lines[2]),
            'fort_status': parse_int(lines[5]),
            'um_status': parse_int(lines[6]),
            'distance': parse_float(lines[7]),
            'notes': lines[8].strip(),
            'name': lines[9].strip(),
            'sheet_col': column,
            'sheet_order': order,
        }
    except (IndexError, TypeError):
        raise cog.exc.SheetParsingError("Halt System parsing.")


def parse_int(word):
    """ Parse into int, on failure return 0 """
    try:
        return int(word)
    except ValueError:
        try:
            return int(word.replace(',', ''))
        except ValueError:
            return 0


def parse_float(word):
    """ Parse into float, on failure return 0.0 """
    try:
        return float(word)
    except ValueError:
        return 0.0


def parse_percent(word):
    """ Parse a percent into a float. """
    try:
        return float(word)
    except ValueError:
        try:
            return parse_float(word.replace('%', '')) / 100.0
        except ValueError:
            return 0.0


def empty_tables(session, *, perm=False):
    """
    Drop all tables.
    """
    classes = [FortDrop, UMHold, FortSystem, UMSystem, FortUser, UMUser, KOS]
    if perm:
        classes += [DUser]

    for cls in classes:
        for matched in session.query(cls):
            session.delete(matched)
    session.commit()


def recreate_tables():
    """
    Recreate all tables in the database, mainly for schema changes and testing.
    """
    cogdb.Session.close_all()
    Base.metadata.drop_all(cogdb.engine)
    Base.metadata.create_all(cogdb.engine)


# Relationships
# TODO: Is there a better way?
#   Now using two one way selects, feels icky.
#   I cannot enforce this key constraint as both sides are inserted independently
#   So instead of a key I make two one way joins and cast the key.
# DUser.sheets = sqla_orm.relationship("SheetRow",
                                    # primaryjoin="DUser.pref_name == foreign(SheetRow.name)",
                                    # cascade_backrefs=False)
# SheetRow.duser = sqla_orm.relationship("DUser",
                                    # primaryjoin="SheetRow.name == foreign(DUser.pref_name)",
                                    # cascade_backrefs=False)
# FIXME: Put these up
# Fortification relations
#  Drop.user = sqla_orm.relationship('SheetCattle', uselist=False, back_populates='merits',
                                  #  lazy='select')
#  SheetCattle.merits = sqla_orm.relationship('Drop',
                                           #  cascade='all, delete, delete-orphan',
                                           #  back_populates='user',
                                           #  lazy='select')
#  Drop.system = sqla_orm.relationship('System', uselist=False, back_populates='merits',
                                    #  lazy='select')
#  System.merits = sqla_orm.relationship('Drop',
                                      #  cascade='all, delete, delete-orphan',
                                      #  back_populates='system',
                                      #  lazy='select')

#  Undermining relations
#  Hold.user = sqla_orm.relationship('SheetUM', uselist=False, back_populates='merits',
                                  #  lazy='select')
#  SheetUM.merits = sqla_orm.relationship('Hold',
                                       #  cascade='all, delete, delete-orphan',
                                       #  back_populates='user',
                                       #  lazy='select')
#  Hold.system = sqla_orm.relationship('UMSystem', uselist=False, back_populates='merits',
                                    #  lazy='select')
#  SystemUM.merits = sqla_orm.relationship('Hold',
                                        #  cascade='all, delete, delete-orphan',
                                        #  back_populates='system',
                                        #  lazy='select')


if cogdb.TEST_DB:
    recreate_tables()
else:
    Base.metadata.create_all(cogdb.engine)


def main():  # pragma: no cover
    """
    This continues to exist only as a sanity test for schema and relations.
    """
    Base.metadata.drop_all(cogdb.engine)
    Base.metadata.create_all(cogdb.engine)
    session = cogdb.Session()

    dusers = (
        DUser(id=197221, pref_name='User1'),
        DUser(id=299221, pref_name='User2'),
        DUser(id=293211, pref_name='User3'),
    )
    session.add_all(dusers)
    session.commit()

    sheets = (
        FortUser(name=dusers[0].pref_name, row=15),
        FortUser(name=dusers[1].pref_name, row=16),
        FortUser(name=dusers[2].pref_name, row=17),
        UMUser(name=dusers[2].pref_name, row=22),
    )

    session.add_all(sheets)
    session.commit()

    systems = (
        FortSystem(name='Frey', sheet_col='F', sheet_order=1, fort_status=0,
                   trigger=7400, undermine=0),
        FortSystem(name='Adeo', sheet_col='G', sheet_order=2, fort_status=0,
                   trigger=5400, undermine=0),
        FortSystem(name='Sol', sheet_col='H', sheet_order=3, fort_status=0,
                   trigger=6000, undermine=0),
    )
    session.add_all(systems)
    session.commit()

    drops = (
        FortDrop(user_id=sheets[0].id, system_id=systems[0].id, amount=700),
        FortDrop(user_id=sheets[1].id, system_id=systems[0].id, amount=700),
        FortDrop(user_id=sheets[0].id, system_id=systems[2].id, amount=1400),
        FortDrop(user_id=sheets[2].id, system_id=systems[1].id, amount=2100),
        FortDrop(user_id=sheets[2].id, system_id=systems[0].id, amount=300),
    )
    session.add_all(drops)
    session.commit()

    def mprint(*args):
        """ Padded print. """
        args = [str(x) for x in args]
        print(*args)

    pad = ' ' * 3

    print('DiscordUsers----------')
    for user in session.query(DUser):
        mprint(user)
        mprint(pad, user.sheets)
        mprint(user.cattle)

    print('FortUsers----------')
    for user in session.query(FortUser):
        mprint(user)
        mprint(pad, user.duser)

    print('FortSystems----------')
    for sys in session.query(FortSystem):
        mprint(sys)
        mprint(pad, sys.merits)
        mprint(sorted(sys.merits))

    print('FortDrops----------')
    for drop in session.query(FortDrop):
        mprint(drop)
        mprint(pad, drop.user)
        mprint(pad, drop.system)


if __name__ == "__main__":  # pragma: no cover
    main()
