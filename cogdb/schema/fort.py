"""
Track all fortification information on the fort sheet during a given cycle.
Information on the systems, users and the drops made by users in systems are stored for querying and analysis.
FortOrder allows manual override by leadership over the order of the systems in the sheet.
"""
import enum

import sqlalchemy as sqla
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.sql.expression import or_

from cogdb.schema.common import Base, LEN
import cog.tbl
import cog.util
from cog.util import ReprMixin


class EFortType(enum.Enum):
    """
    The type of FortSystem available.
        fort - A standard fortification system.
        prep - A system being prepared for expansion.
    """
    fort = 1
    prep = 2


class FortUser(ReprMixin, Base):
    """
    Represents the contributions of a member of the discord on the sheets.
    Note that the name of a user is expected to be unique and not necessarily
    identical to the user's discord name.
    """
    __tablename__ = 'hudson_fort_users'
    _repr_keys = ['id', 'name', 'row', 'cry']

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN['name']), index=True)  # Undeclared FK to discord_users
    row = sqla.Column(sqla.Integer, unique=True)
    cry = sqla.Column(sqla.String(LEN['name']), default='')

    # Relationships
    discord_user = relationship(
        'DiscordUser', uselist=False,
        primaryjoin='foreign(FortUser.name) == DiscordUser.pref_name'
    )
    merits = relationship(
        'FortDrop', uselist=True, cascade='all, delete, delete-orphan',
        back_populates='user', lazy='select'
    )

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
        return sqla.func.cast(
            sqla.func.ifnull(
                sqla.select(sqla.func.sum(FortDrop.amount)).
                where(FortDrop.user_id == self.id).
                label('dropped'),
                0
            ),
            sqla.Integer
        )

    def __str__(self):
        return f"dropped={self.dropped!r}, {self!r}".format(self.dropped, self)

    def __eq__(self, other):
        return isinstance(other, FortUser) and self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def merit_summary(self):
        """ Summarize user merits. """
        return f'Dropped {self.dropped}'


class FortSystem(ReprMixin, Base):
    """
    Represent a single standard fort system of the fortification sheet.
    Provides convenience methods to query or display information about the current state of fortification of a system.

    A few details:
        A FortSystemis complete once the trigger is reached, further forts wasted.
        A FortSystem stores the sheet_order to determine the order the systems should be forted.
        A FortSystem stores the sheet_col that allows for updating the column later if requested by command.
    """
    __tablename__ = 'hudson_fort_systems'
    _repr_keys = [
        'id', 'name', 'fort_status', 'trigger', 'fort_override', 'um_status',
        'undermine', 'distance', 'notes', 'sheet_col', 'sheet_order'
    ]

    header = ['Type', 'System', 'Missing', 'Merits (Fort%/UM%)', 'Notes']

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN['name']), index=True)
    type = sqla.Column(sqla.Enum(EFortType), default=EFortType.fort)
    fort_status = sqla.Column(sqla.Integer, default=0)
    trigger = sqla.Column(sqla.Integer, default=1)
    fort_override = sqla.Column(sqla.Float, default=0.0)
    um_status = sqla.Column(sqla.Integer, default=0)
    undermine = sqla.Column(sqla.Float, default=0.0)
    distance = sqla.Column(sqla.Float, default=0.0)
    notes = sqla.Column(sqla.String(LEN['name']), default='')
    sheet_col = sqla.Column(sqla.String(LEN['sheet_col']), default='', unique=True)
    sheet_order = sqla.Column(sqla.Integer)
    manual_order = sqla.Column(sqla.Integer, nullable=True)

    __mapper_args__ = {
        'polymorphic_identity': EFortType.fort,
        'polymorphic_on': type
    }

    # Relationships
    merits = relationship(
        'FortDrop', uselist=True, cascade='all, delete, delete-orphan',
        back_populates='system', lazy='select'
    )

    def __str__(self):
        return f"cmdr_merits={self.cmdr_merits!r}, {self!r}"

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
    def cmdr_merits(cls):
        """ Total merits dropped by cmdrs """
        return sqla.func.cast(
            sqla.func.ifnull(
                sqla.select(sqla.func.sum(FortDrop.amount)).
                where(FortDrop.system_id == cls.id).
                label('cmdr_merits'),
                0
            ),
            sqla.Integer
        )

    @hybrid_property
    def ump(self):
        """ Return the undermine percentage, stored as decimal. """
        return f'{self.undermine * 100:.1f}'

    @ump.expression
    def ump(cls):
        """ Return the undermine percentage, stored as decimal. """
        return sqla.func.round(cls.undermine * 100, 1)

    @hybrid_property
    def current_status(self):
        """ Simply return max fort status reported. """
        return max(self.fort_status, self.cmdr_merits)

    @current_status.expression
    def current_status(cls):
        """ Simply return max fort status reported. """
        return sqla.func.greatest(cls.fort_status, cls.cmdr_merits)

    @hybrid_property
    def missing(self):
        """ The remaining supplies to fortify. """
        return max(0, self.trigger - self.current_status)

    @missing.expression
    def missing(cls):
        """ The remaining supplies to fortify. """
        return sqla.func.greatest(0, cls.trigger - cls.current_status)

    @hybrid_property
    def is_priority(self):
        """ The system should be priority. """
        notes = self.notes.lower()
        return 'priority' in notes

    @is_priority.expression
    def is_priority(cls):
        """ The system should be priority. """
        return cls.notes.ilike("%priority%")

    @hybrid_property
    def is_prep(self):
        """ The system should be priority. """
        return self.type == EFortType.prep

    @hybrid_property
    def is_skipped(self):
        """ The system should be skipped. """
        notes = self.notes.lower()
        return 'leave' in notes or 'skip' in notes

    @is_skipped.expression
    def is_skipped(cls):
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

    @hybrid_property
    def is_fortified(self):
        """ Check if the system is fortified. """
        return self.fort_override >= 1.0 or self.current_status >= self.trigger

    @is_fortified.expression
    def is_fortified(cls):
        """ Check if the system is fortified. Expression. """
        return sqla.or_(cls.fort_override >= 1.0, cls.current_status >= cls.trigger)

    @hybrid_property
    def is_undermined(self):
        """ Check if the system is undermined. """
        return self.undermine >= 1.00

    @hybrid_property
    def is_deferred(self):
        """ Check if the system is deferred. """
        return self.missing > 0 and self.missing <= cog.util.CONF.constants.defer_missing

    @is_deferred.expression
    def is_deferred(cls):
        """ Check if the system is deferred. """
        return sqla.and_(cls.missing > 0, cls.missing <= cog.util.CONF.constants.defer_missing)

    @property
    def completion(self):
        """ The fort completion percentage. """
        try:
            comp_cent = self.current_status / self.trigger * 100
        except ZeroDivisionError:
            comp_cent = 0

        return f'{comp_cent:.1f}'

    @property
    def table_row(self):
        """
        Return a tuple of important data to be formatted for table output.
        Each element should be mapped to separate column.
        See header.
        """
        status = f'{self.current_status:>4}/{self.trigger} ({self.completion}%/{self.ump}%)'
        sys_type = str(self.type).split('.', maxsplit=1)[-1].capitalize()

        return (sys_type, self.name, f'{self.missing:>4}', status, self.notes)

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
            um_suffix = 'ed' if self.is_undermined else 'ing'
            umd = f', {self.um_status} :Undermin{um_suffix}:'
        elif self.is_undermined:
            umd = ', :Undermined:'

        fort_suffix = 'ied' if self.is_fortified else 'ying'
        msg = f'**{self.name}** {self.current_status:>4}/{self.trigger} :Fortif{fort_suffix}:{umd}'

        if miss or miss is not False and (self.missing and self.missing < 1500):
            msg += f' ({self.missing} left)'

        if self.notes:
            msg += ' ' + self.notes

        msg += f' - {self.distance}Ly'

        return msg

    def display_details(self):
        """ Return a highly detailed system display. """
        miss = f" ({self.missing} left)" if self.missing else ''
        lines = [
            ['Completion', f'{self.completion}%{miss}'],
            ['CMDR Merits', f'{self.cmdr_merits}/{self.trigger}'],
            ['Fort Status', f'{self.fort_status}/{self.trigger}'],
            ['UM Status', f'{self.um_status} ({self.undermine * 100:.2f}%)'],
            ['Notes', self.notes],
        ]

        return cog.tbl.format_table(lines, prefix=f'**{self.name}**\n')[0]


class FortPrep(FortSystem):
    """
    Represent a single preparation system of the fortification sheet.
    Systems are prepared a cycle before they are set for expansion by the faction.
    Provides convenience methods to query or display information about the current state of preparation of a system.

    A few details:
        A FortPrep is never complete, the winner has the higher of forts vs undermining at tick.
        A FortPrep stores the sheet_order to determine the order the systems should be forted.
        A FortPrep stores the sheet_col that allows for updating the column later if requested by command.
    """
    __mapper_args__ = {
        'polymorphic_identity': EFortType.prep,
    }

    def display(self, *, miss=None):
        """
        Return a useful short representation of PrepSystem.
        """
        return 'Prep: ' + super().display(miss=miss)


class FortDrop(ReprMixin, Base):
    """
    A FortDrop represents the contributions of a single FortUser towards a FortSystem or FortPrep.
    A contribution generally comes in the form of delivering fortification cargo to a system.
    """
    __tablename__ = 'hudson_fort_merits'
    _repr_keys = ['id', 'system_id', 'user_id', 'amount']

    id = sqla.Column(sqla.Integer, primary_key=True)
    amount = sqla.Column(sqla.Integer, default=0, nullable=False)
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('hudson_fort_systems.id'), nullable=False)
    user_id = sqla.Column(sqla.Integer, sqla.ForeignKey('hudson_fort_users.id'), nullable=False)

    # Relationships
    user = relationship(
        'FortUser', uselist=False, back_populates='merits', lazy='select'
    )
    system = relationship(
        'FortSystem', uselist=False, back_populates='merits', lazy='select'
    )

    def __str__(self):
        system = ''
        if getattr(self, 'system'):
            system = f"system={self.system.name!r}, "

        suser = ''
        if getattr(self, 'user'):
            suser = f"user={self.user.name!r}, "

        return f"{system}{suser}{self!r}"

    def __eq__(self, other):
        return isinstance(other, FortDrop) and (self.system_id, self.user_id) == (
            other.system_id, other.user_id)

    def __lt__(self, other):
        return self.amount < other.amount

    def __hash__(self):
        return hash(f"{self.system_id}_{self.user_id}")


class FortOrder(ReprMixin, Base):
    """
    Represent the manual order that should override FortSystem.sheet_order.
    When there are no FortOrder objects left default back to FortSystem.sheet_order.
    When there is even one FortOrder object remaining, display ONLY the FortSystems or FortPreps matching.
    """
    __tablename__ = 'hudson_fort_order'
    _repr_keys = ['order', 'system_name']

    order = sqla.Column(sqla.Integer, primary_key=True)
    system_name = sqla.Column(sqla.String(LEN['name']), unique=True)

    # Relationships
    system = relationship(
        'FortSystem', uselist=False, viewonly=True,
        primaryjoin="foreign(FortOrder.system_name) == FortSystem.name"
    )

    def __eq__(self, other):
        return isinstance(other, FortOrder) and self.system_name == other.system_name

    def __hash__(self):
        return hash(self.system_name)
