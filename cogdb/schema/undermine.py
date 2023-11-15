"""

"""
import enum

import sqlalchemy as sqla
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.sql.expression import or_

from cogdb.schema.common import Base, LEN
import cog.exc


class EUMSheet(enum.Enum):
    """
    The type of sheet a record relates to.
        main - The main undermining sheet.
        snipe - The undermining sheet for snipes of other powers.
    """
    main = 'main'
    snipe = 'snipe'


class EUMType(enum.Enum):
    """ Type of undermine system. """
    control = 1
    expand = 2
    oppose = 3


class UMUser(Base):
    """
    Track all infomration about the user in a row of the cattle sheet.
    """
    __tablename__ = 'hudson_um_users'

    id = sqla.Column(sqla.Integer, primary_key=True)
    sheet_src = sqla.Column(sqla.Enum(EUMSheet), default=EUMSheet.main)
    name = sqla.Column(sqla.String(LEN['name']), index=True)  # Undeclared FK to discord_users
    row = sqla.Column(sqla.Integer)
    cry = sqla.Column(sqla.String(LEN['name']), default='')

    __table_args__ = (
        sqla.UniqueConstraint('sheet_src', 'row', name='umuser_sheet_row_constraint'),
    )

    # Relationships
    discord_user = relationship(
        'DiscordUser', uselist=False,
        primaryjoin='foreign(UMUser.name) == DiscordUser.pref_name'
    )
    merits = relationship(
        'UMHold', uselist=True, cascade='all, delete, delete-orphan',
        back_populates='user', lazy='select'
    )

    @hybrid_property
    def held(self):
        """ Total merits held by this cmdr. """
        total = 0
        for hold in self.merits:
            total += hold.held

        return total

    @held.expression
    def held(cls):
        """ Total merits held by this cmdr. """
        return sqla.func.cast(
            sqla.func.ifnull(
                sqla.select(sqla.func.sum(UMHold.held)).
                where(sqla.and_(UMHold.user_id == cls.id,
                                UMHold.sheet_src != EUMSheet.snipe)).
                label('held'),
                0
            ),
            sqla.Integer
        )

    @hybrid_property
    def redeemed(self):
        """ Total merits redeemed by this cmdr. """
        total = 0
        for hold in self.merits:
            total += hold.redeemed

        return total

    @redeemed.expression
    def redeemed(cls):
        """ Total merits redeemed by this cmdr. """
        return sqla.func.cast(
            sqla.func.ifnull(
                sqla.select(sqla.func.sum(UMHold.redeemed)).
                where(sqla.and_(UMHold.user_id == cls.id,
                                UMHold.sheet_src != EUMSheet.snipe)).
                label('redeemed'),
                0
            ),
            sqla.Integer
        )

    def __repr__(self):
        keys = ['id', 'name', 'row', 'cry']
        kwargs = [f'{key}={getattr(self, key)!r}' for key in keys]
        sheet_src = 'EUMSheet.main' if self.sheet_src == EUMSheet.main else 'EUMSheet.snipe'
        kwargs.insert(1, f"sheet_src={sheet_src}")

        return f'{self.__class__.__name__}({", ".join(kwargs)})'

    def __str__(self):
        return f"held={self.held!r}, redeemed={self.redeemed!r}, {self!r}"

    def __eq__(self, other):
        return isinstance(other, UMUser) and self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def merit_summary(self):
        """ Summarize user merits. """
        return f'Holding {self.held}, Redeemed {self.redeemed}'


class UMSystem(Base):
    """
    A control system we intend on undermining.
    """
    __tablename__ = 'hudson_um_systems'

    id = sqla.Column(sqla.Integer, primary_key=True)
    sheet_src = sqla.Column(sqla.Enum(EUMSheet), default=EUMSheet.main)
    name = sqla.Column(sqla.String(LEN['name']), index=True)
    type = sqla.Column(sqla.Enum(EUMType), default=EUMType.control)
    sheet_col = sqla.Column(sqla.String(LEN['sheet_col']))
    goal = sqla.Column(sqla.Integer, default=0)
    security = sqla.Column(sqla.String(LEN['name']), default='')
    notes = sqla.Column(sqla.String(LEN['name']), default='')
    close_control = sqla.Column(sqla.String(LEN['name']), default='')
    priority = sqla.Column(sqla.String(LEN['name']), default="Normal")
    progress_us = sqla.Column(sqla.Integer, default=0)
    progress_them = sqla.Column(sqla.Float, default=0.0)
    map_offset = sqla.Column(sqla.Integer, default=0)
    exp_trigger = sqla.Column(sqla.Integer, default=0)

    __table_args__ = (
        sqla.UniqueConstraint('sheet_src', 'sheet_col', name='umsystem_sheet_row_constraint'),
    )
    __mapper_args__ = {
        'polymorphic_identity': EUMType.control,
        'polymorphic_on': type,
    }

    # Relationships
    merits = relationship('UMHold', uselist=True,
                                   cascade='all, delete, delete-orphan',
                                   back_populates='system',
                                   lazy='select')

    @staticmethod
    def factory(kwargs):
        """ Simple factory to make undermining systems. """
        cls = kwargs.pop('cls')
        return cls(**kwargs)

    def __repr__(self):
        keys = [
            'id', 'name', 'sheet_col', 'goal', 'security', 'notes',
            'progress_us', 'progress_them', 'close_control', 'priority', 'map_offset'
        ]
        kwargs = [f'{key}={getattr(self, key)!r}' for key in keys]
        sheet_src = "EUMSheet.main" if self.sheet_src == EUMSheet.main else "EUMSheet.snipe"
        kwargs.insert(1, f"sheet_src={sheet_src}")

        return f'{self.__class__.__name__}({", ".join(kwargs)})'

    def __str__(self):
        """
        Show additional computed properties.
        """
        return f"cmdr_merits={self.cmdr_merits!r}, {self!r}"

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

        completion = f'{comp_cent:.0f}%'

        return completion

    @hybrid_property
    def held_merits(self):
        """ Total merits held by cmdrs."""
        total = 0
        for hold in self.merits:
            total += hold.held
        return total

    @held_merits.expression
    def held_merits(cls):
        """ Total merits held by cmdrs."""
        return sqla.func.cast(
            sqla.func.ifnull(
                sqla.select(sqla.func.sum(UMHold.held)).
                where(UMHold.system_id == cls.id).
                label('cmdr_merits'),
                0
            ),
            sqla.Integer
        )

    @hybrid_property
    def cmdr_merits(self):
        """ Total merits held and redeemed by cmdrs """
        total = 0
        for hold in self.merits:
            total += hold.held + hold.redeemed
        return total

    @cmdr_merits.expression
    def cmdr_merits(cls):
        """ Total merits held or redeemd by cmdrs """
        return sqla.func.cast(
            sqla.func.ifnull(
                sqla.select(sqla.func.sum(UMHold.held + UMHold.redeemed)).
                where(UMHold.system_id == cls.id).
                label('cmdr_merits'),
                0
            ),
            sqla.Integer
        )

    @hybrid_property
    def missing(self):
        """ The remaining merites targetted to undermine. """
        return self.goal - max(self.cmdr_merits + self.map_offset, self.progress_us)

    @missing.expression
    def missing(cls):
        """ The remaining merites targetted to undermine. """
        return cls.goal - sqla.func.greatest(cls.cmdr_merits + cls.map_offset, cls.progress_us)

    @hybrid_property
    def is_skipped(self):
        """ The system should be skipped. """
        priority = self.priority.lower()
        return 'leave' in priority or 'skip' in priority

    @is_skipped.expression
    def is_skipped(cls):
        """ The system should be skipped. """
        return or_(cls.priority.ilike("%leave%"), cls.priority.ilike("%skip%"))

    @property
    def descriptor(self):
        """ Descriptive prefix for string. """
        return str(self.type).split('.', maxsplit=1)[-1].capitalize()

    @hybrid_property
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
            [self.descriptor, f'{self.name} [{self.security[0].upper()} sec]'],
            [self.completion, f"Merits {'Missing' if self.missing > 0 else 'Leading'} {abs(self.missing)}"],
            ['Our Progress ' + str(self.progress_us), f'Enemy Progress {self.progress_them * 100:.0f}%'],
            ['Nearest Hudson', self.close_control],
            ['Priority', self.priority],
            ['Power', self.notes],
        ]

        return cog.tbl.format_table(lines)[0]

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

    @hybrid_property
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
        completion = f'{prefix} {abs(comp_cent):.0f}%'

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
    sheet_src = sqla.Column(sqla.Enum(EUMSheet), default=EUMSheet.main)
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('hudson_um_systems.id'), nullable=False)
    user_id = sqla.Column(sqla.Integer, sqla.ForeignKey('hudson_um_users.id'), nullable=False)
    held = sqla.Column(sqla.Integer, default=0, nullable=False)
    redeemed = sqla.Column(sqla.Integer, default=0, nullable=False)

    __table_args__ = (
        sqla.UniqueConstraint('sheet_src', 'system_id', 'user_id', name='umhold_sheet_row_constraint'),
    )

    # Relationships
    user = relationship(
        'UMUser', uselist=False, back_populates='merits', lazy='select'
    )
    system = relationship(
        'UMSystem', uselist=False, back_populates='merits', lazy='select'
    )

    def __repr__(self):
        keys = ['id', 'system_id', 'user_id', 'held', 'redeemed']
        kwargs = [f'{key}={getattr(self, key)!r}' for key in keys]
        sheet_src = "EUMSheet.main" if self.sheet_src == EUMSheet.main else "EUMSheet.snipe"
        kwargs.insert(1, f"sheet_src={sheet_src}")

        return f'{self.__class__.__name__}({", ".join(kwargs)})'

    def __str__(self):
        system = ''
        if getattr(self, 'system', None):
            system = f"system={self.system.name!r}, "

        suser = ''
        if getattr(self, 'user', None):
            suser = f"user={self.user.name!r}, "

        return f"{system}{suser}{self!r}"

    def __eq__(self, other):
        return isinstance(other, UMHold) and (self.system_id, self.user_id) == (
            other.system_id, other.user_id)

    def __lt__(self, other):
        return self.held + self.redeemed < other.held + other.redeemed

    def __hash__(self):
        return hash(f"{self.system_id}_{self.user_id}")
