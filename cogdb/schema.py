"""
Define the major tables that are used by this bot.
These allow the bot to store and query the information in sheets that are parsed.
"""
import datetime
import enum

import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm
import sqlalchemy.orm.session
import sqlalchemy.ext.declarative
from sqlalchemy.sql.expression import or_, and_, not_
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method

import cog.exc
import cog.tbl
import cogdb


LEN_CMD = 25  # Max length of a subclass of cog.actions
LEN_NAME = 100
LEN_SHEET_COL = 5
LEN_CARRIER = 7
EVENT_CARRIER = """
CREATE EVENT IF NOT EXISTS clean_carriers
ON SCHEDULE
    EVERY 1 DAY
COMMENT "If no updates in 4 days drop"
DO
    DELETE FROM {}.carriers_ids
    WHERE
        (DATEDIFF(NOW(), carriers_ids.updated_at) > 4)
            and
        (carriers_ids.override = 0)
"""
Base = sqlalchemy.ext.declarative.declarative_base()


class DiscordUser(Base):
    """
    Table to store discord users and their permanent preferences.

    These are what the user would prefer be put into sheets.
    It is also a central tie in for relationships.
    """
    __tablename__ = 'discord_users'

    id = sqla.Column(sqla.BigInteger, primary_key=True)  # Discord id
    display_name = sqla.Column(sqla.String(LEN_NAME))
    pref_name = sqla.Column(sqla.String(LEN_NAME), unique=True, nullable=False)  # pref_name == display_name until change
    pref_cry = sqla.Column(sqla.String(LEN_NAME), default='')

    # Relationships
    fort_user = sqla.orm.relationship(
        'FortUser', uselist=False, viewonly=True,
        primaryjoin='foreign(DiscordUser.pref_name) == FortUser.name'
    )
    fort_merits = sqla_orm.relationship(
        'FortDrop', lazy='select', uselist=True, viewonly=True,
        primaryjoin='and_(foreign(DiscordUser.pref_name) == remote(FortUser.name), foreign(FortUser.id) == FortDrop.user_id)'
    )
    um_user = sqla.orm.relationship(
        'UMUser', uselist=False, viewonly=True,
        primaryjoin='foreign(DiscordUser.pref_name) == UMUser.name'
    )
    um_merits = sqla_orm.relationship(
        'UMHold', lazy='select', uselist=True, viewonly=True,
        primaryjoin='and_(foreign(DiscordUser.pref_name) == remote(UMUser.name), foreign(UMUser.id) == UMHold.user_id)'
    )

    def __repr__(self):
        keys = ['id', 'display_name', 'pref_name', 'pref_cry']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(other, DiscordUser) and self.id == other.id

    def __hash__(self):
        return hash(self.id)

    @property
    def mention(self):
        """ Mention this user in a response. """
        return "<@{}>".format(self.id)

    @property
    def total_merits(self):
        """ The total merits a user has done this cycle. """
        total = 0

        try:
            total += self.fort_user.dropped
        except AttributeError:
            pass
        try:
            total += self.um_user.held + self.um_user.redeemed
        except AttributeError:
            pass

        return total


class FortUser(Base):
    """
    Track all infomration about the user in a row of the cattle sheet.

    These are what actually is in the sheet.
    """
    __tablename__ = 'hudson_fort_users'

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN_NAME), unique=True)  # Undeclared FK to discord_users
    row = sqla.Column(sqla.Integer, unique=True)
    cry = sqla.Column(sqla.String(LEN_NAME), default='')

    # Relationships
    discord_user = sqla.orm.relationship(
        'DiscordUser', uselist=False,
        primaryjoin='foreign(FortUser.name) == DiscordUser.pref_name'
    )
    merits = sqla_orm.relationship('FortDrop', uselist=True,
                                   cascade='all, delete, delete-orphan',
                                   back_populates='user',
                                   lazy='select')

    @hybrid_property
    def dropped(self):
        """ Total merits dropped by cmdrs """
        total = 0
        for drop in self.merits:
            total += drop.amount

        return total

    @dropped.expression
    def dropped(self):
        """ Total merits dropped by cmdrs """
        return sqla.select([sqla.func.sum(FortDrop.amount)]).\
            where(FortDrop.user_id == self.id).\
            label('dropped')

    def __repr__(self):
        keys = ['id', 'name', 'row', 'cry']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        return "dropped={!r}, {!r}".format(self.dropped, self)

    def __eq__(self, other):
        return isinstance(other, FortUser) and self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def merit_summary(self):
        """ Summarize user merits. """
        return 'Dropped {}'.format(self.dropped)


class EFortType(enum.Enum):
    """ Type of fort system. """
    fort = 1
    prep = 2


class FortSystem(Base):
    """
    Represent a single system for fortification in the sheet.
    Object can be flushed and queried from the database.

    Carefully examine all methods to understanda, it centralizes a lot of logic.
    When representing the system use display methods and see header tuple.
    """
    __tablename__ = 'hudson_fort_systems'

    header = ['Type', 'System', 'Missing', 'Merits (Fort%/UM%)', 'Notes']

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN_NAME), unique=True)
    type = sqla.Column(sqla.Enum(EFortType), default=EFortType.fort)
    fort_status = sqla.Column(sqla.Integer, default=0)
    trigger = sqla.Column(sqla.Integer, default=1)
    fort_override = sqla.Column(sqla.Float, default=0.0)
    um_status = sqla.Column(sqla.Integer, default=0)
    undermine = sqla.Column(sqla.Float, default=0.0)
    distance = sqla.Column(sqla.Float, default=0.0)
    notes = sqla.Column(sqla.String(LEN_NAME), default='')
    sheet_col = sqla.Column(sqla.String(LEN_SHEET_COL), default='')
    sheet_order = sqla.Column(sqla.Integer)
    manual_order = sqla.Column(sqla.Integer, nullable=True)

    __mapper_args__ = {
        'polymorphic_identity': EFortType.fort,
        'polymorphic_on': type
    }

    # Relationships
    merits = sqla_orm.relationship('FortDrop', uselist=True,
                                   cascade='all, delete, delete-orphan',
                                   back_populates='system',
                                   lazy='select')

    def __repr__(self):
        keys = ['id', 'name', 'fort_status', 'trigger', 'fort_override', 'um_status',
                'undermine', 'distance', 'notes', 'sheet_col', 'sheet_order']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        return "cmdr_merits={!r}, {!r}".format(self.cmdr_merits, self)

    def __eq__(self, other):
        return isinstance(other, FortSystem) and self.name == other.name

    def __lt__(self, other):
        """ Order systems by remaining supplies needed. """
        return isinstance(other, self.__class__) and self.missing < other.missing

    def __hash__(self):
        return hash(self.name)

    @hybrid_property
    def cmdr_merits(self):
        """ Total merits dropped by cmdrs """
        total = 0
        for drop in self.merits:
            total += drop.amount
        return total

    @cmdr_merits.expression
    def cmdr_merits(self):
        """ Total merits dropped by cmdrs """
        return sqla.select([sqla.func.sum(FortDrop.amount)]).\
            where(FortDrop.system_id == self.id).\
            label('cmdr_merits')

    @property
    def ump(self):
        """ Return the undermine percentage, stored as decimal. """
        return '{:.1f}'.format(self.undermine * 100)

    @property
    def current_status(self):
        """ Simply return max fort status reported. """
        return max(self.fort_status, self.cmdr_merits)

    @hybrid_property
    def skip(self):
        """ The system should be skipped. """
        notes = self.notes.lower()
        return 'leave' in notes or 'skip' in notes

    @skip.expression
    def skip(cls):
        """ The system should be skipped. """
        return or_(cls.notes.ilike("%leave%"), cls.notes.ilike("%skip%"))

    @hybrid_property
    def is_medium(self):
        """ The system should be skipped. """
        return 's/m' in self.notes.lower()

    @is_medium.expression
    def is_medium(cls):
        """ The system should be skipped. """
        return cls.notes.ilike("%s/m%")

    @property
    def is_fortified(self):
        """ The remaining supplies to fortify """
        return self.fort_override >= 1.0 or self.current_status >= self.trigger

    @hybrid_property
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
        type = str(self.type).split('.')[-1].capitalize()

        return (type, self.name, '{:>4}'.format(self.missing), status, self.notes)

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


class FortDrop(Base):
    """
    Every drop made by a user creates a fort entry here.
    A drop represents the value at the intersection of a FortUser and a FortSystem.
    """
    __tablename__ = 'hudson_fort_merits'

    id = sqla.Column(sqla.Integer, primary_key=True)
    amount = sqla.Column(sqla.Integer)
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('hudson_fort_systems.id'), nullable=False)
    user_id = sqla.Column(sqla.Integer, sqla.ForeignKey('hudson_fort_users.id'), nullable=False)

    # Relationships
    user = sqla_orm.relationship('FortUser', uselist=False, back_populates='merits',
                                 lazy='select')
    system = sqla_orm.relationship('FortSystem', uselist=False, back_populates='merits',
                                   lazy='select')

    def __repr__(self):
        keys = ['id', 'system_id', 'user_id', 'amount']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        system = ''
        if getattr(self, 'system'):
            system = "system={!r}, ".format(self.system.name)

        suser = ''
        if getattr(self, 'user'):
            suser = "user={!r}, ".format(self.user.name)

        return "{}{}{!r}".format(system, suser, self)

    def __eq__(self, other):
        return isinstance(other, FortDrop) and (self.system_id, self.user_id) == (
            other.system_id, other.user_id)

    def __lt__(self, other):
        return self.amount < other.amount

    def __hash__(self):
        return hash("{}_{}".format(self.system_id, self.user_id))


class FortOrder(Base):
    """
    Simply store a list of Control systems in the order they should be forted.
    """
    __tablename__ = 'hudson_fort_order'

    order = sqla.Column(sqla.Integer, primary_key=True)
    system_name = sqla.Column(sqla.String(LEN_NAME), unique=True)

    # Relationships
    system = sqla.orm.relationship(
        'FortSystem', uselist=False,
        primaryjoin="foreign(FortOrder.system_name) == FortSystem.name"
    )

    def __repr__(self):
        keys = ['order', 'system_name']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(other, FortOrder) and self.system_name == other.system_name

    def __hash__(self):
        return hash(self.system_name)


class UMUser(Base):
    """
    Track all infomration about the user in a row of the cattle sheet.
    """
    __tablename__ = 'hudson_um_users'

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN_NAME), unique=True)  # Undeclared FK to discord_users
    row = sqla.Column(sqla.Integer, unique=True)
    cry = sqla.Column(sqla.String(LEN_NAME), default='')

    # Relationships
    discord_user = sqla.orm.relationship(
        'DiscordUser', uselist=False,
        primaryjoin='foreign(UMUser.name) == DiscordUser.pref_name'
    )
    merits = sqla_orm.relationship('UMHold', uselist=True,
                                   cascade='all, delete, delete-orphan',
                                   back_populates='user',
                                   lazy='select')

    @hybrid_property
    def held(self):
        """ Total merits held by this cmdr. """
        total = 0
        for hold in self.merits:
            total += hold.held

        return total

    @held.expression
    def held(self):
        """ Total merits held by this cmdr. """
        return sqla.select([sqla.func.sum(UMHold.held)]).\
            where(UMHold.user_id == self.id).\
            label('held')

    @hybrid_property
    def redeemed(self):
        """ Total merits redeemed by this cmdr. """
        total = 0
        for hold in self.merits:
            total += hold.redeemed

        return total

    @redeemed.expression
    def redeemed(self):
        """ Total merits redeemed by this cmdr. """
        return sqla.select([sqla.func.sum(UMHold.redeemed)]).\
            where(UMHold.user_id == self.id).\
            label('redeemed')

    def __repr__(self):
        keys = ['id', 'name', 'row', 'cry']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        return "held={!r}, redeemed={!r}, {!r}".format(self.held, self.redeemed, self)

    def __eq__(self, other):
        return isinstance(other, UMUser) and self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def merit_summary(self):
        """ Summarize user merits. """
        return 'Holding {}, Redeemed {}'.format(self.held, self.redeemed)


class EUMType(enum.Enum):
    """ Type of undermine system. """
    control = 1
    expand = 2
    oppose = 3


class UMSystem(Base):
    """
    A control system we intend on undermining.
    """
    __tablename__ = 'hudson_um_systems'

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN_NAME), unique=True)
    type = sqla.Column(sqla.Enum(EUMType))
    sheet_col = sqla.Column(sqla.String(LEN_SHEET_COL))
    goal = sqla.Column(sqla.Integer)
    security = sqla.Column(sqla.String(LEN_NAME), default='')
    notes = sqla.Column(sqla.String(LEN_NAME), default='')
    close_control = sqla.Column(sqla.String(LEN_NAME), default='')
    priority = sqla.Column(sqla.String(LEN_NAME))
    progress_us = sqla.Column(sqla.Integer)
    progress_them = sqla.Column(sqla.Float)
    map_offset = sqla.Column(sqla.Integer, default=0)
    exp_trigger = sqla.Column(sqla.Integer, default=0)

    __mapper_args__ = {
        'polymorphic_identity': EUMType.control,
        'polymorphic_on': type,
    }

    # Relationships
    merits = sqla_orm.relationship('UMHold', uselist=True,
                                   cascade='all, delete, delete-orphan',
                                   back_populates='system',
                                   lazy='select')

    @staticmethod
    def factory(kwargs):
        """ Simple factory to make undermining systems. """
        cls = kwargs.pop('cls')
        return cls(**kwargs)

    def __repr__(self):
        keys = ['id', 'name', 'sheet_col', 'goal', 'security', 'notes',
                'progress_us', 'progress_them', 'close_control', 'priority', 'map_offset']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        """
        Show additional computed properties.
        """
        return "cmdr_merits={!r}, {!r}".format(self.cmdr_merits, self)

    def __eq__(self, other):
        return isinstance(other, UMSystem) and self.name == other.name

    def __hash__(self):
        return hash(self.name)

    @property
    def completion(self):
        """ The completion percentage formatted as a string """
        try:
            comp_cent = (self.goal - self.missing) / self.goal * 100
        except ZeroDivisionError:
            comp_cent = 0

        completion = '{:.0f}%'.format(comp_cent)

        return completion

    @hybrid_property
    def cmdr_merits(self):
        """ Total merits held and redeemed by cmdrs """
        total = 0
        for hold in self.merits:
            total += hold.held + hold.redeemed
        return total

    @cmdr_merits.expression
    def cmdr_merits(self):
        """ Total merits dropped by cmdrs """
        return sqla.select([sqla.func.sum(UMHold.held + UMHold.redeemed)]).\
            where(UMHold.system_id == self.id).\
            label('cmdr_merits')

    @property
    def missing(self):
        """ The remaining supplies to fortify """
        return self.goal - max(self.cmdr_merits + self.map_offset, self.progress_us)

    @property
    def descriptor(self):
        """ Descriptive prefix for string. """
        return str(self.type).split('.')[-1].capitalize()

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
            ['Power', self.notes],
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
    user = sqla_orm.relationship('UMUser', uselist=False, back_populates='merits',
                                 lazy='select')
    system = sqla_orm.relationship('UMSystem', uselist=False, back_populates='merits',
                                   lazy='select')

    def __repr__(self):
        keys = ['id', 'system_id', 'user_id', 'held', 'redeemed']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        system = ''
        if getattr(self, 'system', None):
            system = "system={!r}, ".format(self.system.name)

        suser = ''
        if getattr(self, 'user', None):
            suser = "user={!r}, ".format(self.user.name)

        return "{}{}{!r}".format(system, suser, self)

    def __eq__(self, other):
        return isinstance(other, UMHold) and (self.system_id, self.user_id) == (
            other.system_id, other.user_id)

    def __lt__(self, other):
        return self.held + self.redeemed < other.held + other.redeemed

    def __hash__(self):
        return hash("{}_{}".format(self.system_id, self.user_id))


class KOS(Base):
    """
    Represents a the kos list.
    """
    __tablename__ = 'kos'

    id = sqla.Column(sqla.Integer, primary_key=True)
    cmdr = sqla.Column(sqla.String(LEN_NAME), unique=True, nullable=False)
    faction = sqla.Column(sqla.String(LEN_NAME), nullable=False)
    danger = sqla.Column(sqla.Integer)
    is_friendly = sqla.Column(sqla.Boolean)

    def __repr__(self):
        keys = ['id', 'cmdr', 'faction', 'danger', 'is_friendly']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(other, KOS) and (self.cmdr) == (other.cmdr)

    def __hash__(self):
        return hash(self.cmdr)

    @property
    def friendly(self):
        return 'FRIENDLY' if self.is_friendly else 'KILL'


class AdminPerm(Base):
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
        return isinstance(other, AdminPerm) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class ChannelPerm(Base):
    """
    A channel permission to restrict cmd to listed channels.
    """
    __tablename__ = 'perms_channels'

    cmd = sqla.Column(sqla.String(LEN_CMD), primary_key=True)
    server_id = sqla.Column(sqla.BigInteger, primary_key=True)
    channel_id = sqla.Column(sqla.BigInteger, primary_key=True)

    def __repr__(self):
        keys = ['cmd', 'server_id', 'channel_id']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(other, ChannelPerm) and hash(self) == hash(other)

    def __hash__(self):
        return hash("{}_{}_{}".format(self.cmd, self.server_id, self.channel_id))


class RolePerm(Base):
    """
    A role permission to restrict cmd to listed roles.
    """
    __tablename__ = 'perms_roles'

    cmd = sqla.Column(sqla.String(LEN_CMD), primary_key=True)
    server_id = sqla.Column(sqla.BigInteger, primary_key=True)
    role_id = sqla.Column(sqla.BigInteger, primary_key=True)

    def __repr__(self):
        keys = ['cmd', 'server_id', 'role_id']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(other, RolePerm) and hash(self) == hash(other)

    def __hash__(self):
        return hash("{}_{}_{}".format(self.cmd, self.server_id, self.role_id))


class TrackSystem(Base):
    """
    Track a system for carriers.
    """
    __tablename__ = 'carriers_systems'

    system = sqla.Column(sqla.String(LEN_NAME), primary_key=True)
    distance = sqla.Column(sqla.Integer)

    def __repr__(self):
        keys = ['system', 'distance']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        return "Tracking systems <= {}ly from {}".format(self.distance, self.system)

    def __eq__(self, other):
        return isinstance(other, TrackSystem) and hash(self) == hash(other)

    def __hash__(self):
        return hash("{}".format(self.system))


class TrackSystemCached(Base):
    """
    Computed systems that are the total coverage of TrackSystem directives.
    This set of system names is recomputed on every addition or removal.
    """
    __tablename__ = 'carriers_systems_cached'

    system = sqla.Column(sqla.String(LEN_NAME), primary_key=True)

    def __repr__(self):
        keys = ['system']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(other, TrackSystemCached) and hash(self) == hash(other)

    def __hash__(self):
        return hash("{}".format(self.system))


class TrackByID(Base):
    """
    Track where a carrier is by storing id and last known system.
    """
    __tablename__ = 'carriers_ids'

    header = ["ID", "Squad", "System"]

    id = sqla.Column(sqla.String(LEN_CARRIER), primary_key=True)
    squad = sqla.Column(sqla.String(LEN_NAME), default="")
    system = sqla.Column(sqla.String(LEN_NAME), default="")
    override = sqla.Column(sqla.Boolean, default=False)
    updated_at = sqla.Column(sqla.DateTime, default=datetime.datetime.now(datetime.timezone.utc))  # All dates UTC
    # This flag indicates user requested this ID ALWAYS be tracked, regardless of location.

    def __repr__(self):
        keys = ['id', 'squad', 'system', 'override', 'updated_at']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        """ A pretty one line to give all information. """
        return "{id} [{squad}] seen in **{system}** at {date}.".format(
            id=self.id, squad=self.squad if self.squad else "No Group",
            system=self.system if self.system else "No Info", date=self.updated_at
        )

    def __eq__(self, other):
        return isinstance(other, TrackByID) and hash(self) == hash(other)

    def __hash__(self):
        return hash("{}".format(self.id))

    def table_line(self):
        """ Returns a line for table formatting. """
        return ("{}{}".format(self.id, " (O)" if self.override else ""), self.squad, self.system)


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
            cls = UMSystem

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
        classes += [DiscordUser]

    for cls in classes:
        for matched in session.query(cls):
            session.delete(matched)
    session.commit()


def recreate_tables():
    """
    Recreate all tables in the database, mainly for schema changes and testing.
    """
    exclude = []
    if not cogdb.TEST_DB:
        exclude = [DiscordUser.__tablename__, AdminPerm.__tablename__]
    sqlalchemy.orm.session.close_all_sessions()

    meta = sqlalchemy.MetaData(bind=cogdb.engine)
    meta.reflect()
    for tbl in reversed(meta.sorted_tables):
        try:
            if not (str(tbl) in exclude):
                tbl.drop()
        except sqla.exc.OperationalError:
            pass
    Base.metadata.create_all(cogdb.engine)

    with cogdb.engine.connect() as con:
        con.execute(sqla.sql.text(EVENT_CARRIER.format(cogdb.CUR_DB).strip()))


if cogdb.TEST_DB:
    recreate_tables()
else:
    Base.metadata.create_all(cogdb.engine)


def main():  # pragma: no cover
    """
    This continues to exist only as a sanity test for schema and relations.
    """
    recreate_tables()
    session = cogdb.Session()

    try:
        dusers = (
            DiscordUser(id=1, pref_name='User1'),
            DiscordUser(id=2, pref_name='User2'),
            DiscordUser(id=3, pref_name='User3'),
        )
        session.add_all(dusers)
        session.flush()
    except sqlalchemy.exc.IntegrityError:
        session.rollback()

    sheets = (
        FortUser(id=dusers[0].id, name=dusers[0].pref_name, row=15),
        FortUser(id=dusers[1].id, name=dusers[1].pref_name, row=16),
        FortUser(id=dusers[2].id, name=dusers[2].pref_name, row=17),
    )

    session.add_all(sheets)
    session.flush()

    systems = (
        FortSystem(name='Frey', sheet_col='F', sheet_order=1, fort_status=0,
                   trigger=7400, undermine=0),
        FortSystem(name='Adeo', sheet_col='G', sheet_order=2, fort_status=0,
                   trigger=5400, undermine=0),
        FortSystem(name='Sol', sheet_col='H', sheet_order=3, fort_status=0,
                   trigger=6000, undermine=0),
        FortSystem(name='Othime', sheet_col='I', sheet_order=4, fort_status=0,
                   trigger=6000, undermine=0, notes="S/M Priority, Skip"),
        FortSystem(name='Rana', sheet_col='J', sheet_order=5, fort_status=0,
                   trigger=6000, undermine=1.2, notes="Attacked"),
    )
    session.add_all(systems)
    session.flush()

    drops = (
        FortDrop(user_id=sheets[0].id, system_id=systems[0].id, amount=700),
        FortDrop(user_id=sheets[1].id, system_id=systems[0].id, amount=700),
        FortDrop(user_id=sheets[0].id, system_id=systems[2].id, amount=1400),
        FortDrop(user_id=sheets[2].id, system_id=systems[1].id, amount=2100),
        FortDrop(user_id=sheets[2].id, system_id=systems[0].id, amount=300),
    )
    session.add_all(drops)
    session.commit()

    orders = (
        FortOrder(order=1, system_name='Sol'),
        FortOrder(order=2, system_name='Othime'),
    )
    session.add_all(orders)
    session.commit()

    def mprint(*args):
        """ Padded print. """
        args = [str(x) for x in args]
        print(*args)

    pad = ' ' * 3

    print('DiscordUsers----------')
    for user in session.query(DiscordUser).filter(DiscordUser.pref_name.like("User%")).limit(10):
        mprint(user)
        mprint(pad, user.fort_user)
        mprint(pad, user.fort_merits)

    print('FortUsers----------')
    for user in session.query(FortUser):
        mprint(user)
        mprint(pad, user.discord_user)

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

    print('FortOrders----------')
    for order in session.query(FortOrder):
        mprint(order)
        mprint(pad, order.system)

    print(dusers[2].fort_merits)
    print(dusers[2].um_merits)

    #  print(session.query(FortSystem).filter(FortSystem.cmdr_merits > 100).all())
    print(session.query(FortUser).filter(FortUser.dropped > 100).all())


if __name__ == "__main__":  # pragma: no cover
    main()
