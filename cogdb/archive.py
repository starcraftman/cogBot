"""
Archive previous cycles fort and um information.
Data imported should be frozen and will not be subject to changes or updates.
These classes are a mirror to cogdb.schema except:
    - They have an additional cycle field and constraints to support this
    - They have an archive classmethod that will create from an existing class the archive.

To be used for analysis, projections and achievements.
"""
import sqlalchemy as sqla
import sqlalchemy.ext.declarative
import sqlalchemy.orm.session
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.sql.expression import or_

import cog.util
from cog.util import ReprMixin
import cogdb
from cogdb.schema import (
    LEN, Base, DiscordUser, EUMSheet, EUMType, EFortType, FortUser,
)


class ArchiveMixin():
    """
    Mixin to provide ability to archive an analogous
    """
    @classmethod
    def archive(cls, other, *, cycle=None):
        """
        Archive an existing class.

        The class you mix this into should have it's own id as a primary (that will not be copied) and
        in addition a cycle attribute to be set independent of the other object.

        Args:
            cls: The class itself.
            other: The original object to archive, should be related to the class mixed in.
            cycle: Optionally designate the cycle of the archival object. If not provided, default current cycle.

        Returns: The cls object fully created.
        """
        kwargs = {x: getattr(other, x) for x in cls._repr_keys if x not in ['id', 'cycle']}
        kwargs['cycle'] = cog.util.current_cycle() if not cycle else cycle
        return cls(**kwargs)


class AFortUser(ArchiveMixin, ReprMixin, Base):
    """
    Archival copy of cogdb.schema.FortUser augmented with a cycle field.
    """
    __tablename__ = 'archive_hudson_fort_users'
    __table_args__ = (
        UniqueConstraint('cycle', 'name', name='fortuser_cycle_name_unique'),
        UniqueConstraint('cycle', 'row', name='fortuser_cycle_row_unique'),
    )
    _repr_keys = ['id', 'cycle', 'name', 'row', 'cry']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cycle = sqla.Column(sqla.Integer, default=cog.util.current_cycle)
    name = sqla.Column(sqla.String(LEN['name']))  # Undeclared FK to discord_users
    row = sqla.Column(sqla.Integer)
    cry = sqla.Column(sqla.String(LEN['name']), default='')

    # Relationships
    discord_user = sqla.orm.relationship(
        'DiscordUser', uselist=False,
        primaryjoin='foreign(AFortUser.name) == DiscordUser.pref_name'
    )

    def __eq__(self, other):
        return isinstance(other, AFortUser) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f'{self.cycle}_{self.name}')

    def merit_summary(self):
        """ Summarize user merits. """
        return f'Dropped {self.dropped}'


class AFortSystem(ArchiveMixin, ReprMixin, Base):
    """
    Archival copy of cogdb.schema.FortSystem augmented with a cycle field.
    """
    __tablename__ = 'archive_hudson_fort_systems'
    __table_args__ = (
        UniqueConstraint('cycle', 'name', name='fortsystem_cycle_name_unique'),
        UniqueConstraint('cycle', 'sheet_col', name='fortsystem_cycle_sheetcol_unique'),
    )
    _repr_keys = [
        'id', 'cycle', 'name', 'fort_status', 'trigger', 'fort_override', 'um_status',
        'undermine', 'distance', 'notes', 'sheet_col', 'sheet_order'
    ]

    header = ['Type', 'System', 'Missing', 'Merits (Fort%/UM%)', 'Notes']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cycle = sqla.Column(sqla.Integer, default=cog.util.current_cycle)
    name = sqla.Column(sqla.String(LEN['name']))
    type = sqla.Column(sqla.Enum(EFortType), default=EFortType.fort)
    fort_status = sqla.Column(sqla.Integer, default=0)
    trigger = sqla.Column(sqla.Integer, default=1)
    fort_override = sqla.Column(sqla.Float, default=0.0)
    um_status = sqla.Column(sqla.Integer, default=0)
    undermine = sqla.Column(sqla.Float, default=0.0)
    distance = sqla.Column(sqla.Float, default=0.0)
    notes = sqla.Column(sqla.String(LEN['name']), default='')
    sheet_col = sqla.Column(sqla.String(LEN['sheet_col']), default='')
    sheet_order = sqla.Column(sqla.Integer)
    manual_order = sqla.Column(sqla.Integer, nullable=True)

    __mapper_args__ = {
        'polymorphic_identity': EFortType.fort,
        'polymorphic_on': type
    }

    def __eq__(self, other):
        return isinstance(other, AFortSystem) and hash(self) == hash(other)

    def __lt__(self, other):
        """ Order systems by remaining supplies needed. """
        return isinstance(other, self.__class__) and self.missing < other.missing

    def __hash__(self):
        return hash(f'{self.cycle}_{self.name}')

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


class AFortPrep(AFortSystem):
    """
    Archival copy of cogdb.schema.FortPrep augmented with a cycle field.
    """
    __mapper_args__ = {
        'polymorphic_identity': EFortType.prep,
    }

    def display(self, *, miss=None):
        """
        Return a useful short representation of PrepSystem.
        """
        return 'Prep: ' + super().display(miss=miss)


class AFortDrop(ArchiveMixin, ReprMixin, Base):
    """
    Archival copy of cogdb.schema.FortDrop augmented with a cycle field.
    """
    __tablename__ = 'archive_hudson_fort_merits'
    __table_args__ = (
        UniqueConstraint('cycle', 'system_id', 'user_id', name='fortdrop_cycle_systemid_userid_constraint'),
    )
    _repr_keys = ['id', 'system_id', 'user_id', 'cycle', 'amount']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    system_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('archive_hudson_fort_systems.id'), nullable=False)
    user_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('archive_hudson_fort_users.id'), nullable=False)
    cycle = sqla.Column(sqla.Integer, default=cog.util.current_cycle)
    amount = sqla.Column(sqla.Integer, default=0, nullable=False)

    def __eq__(self, other):
        return isinstance(other, AFortDrop) and hash(self) == hash(other)

    def __lt__(self, other):
        return self.amount < other.amount

    def __hash__(self):
        return hash(f"{self.cycle}_{self.system_id}_{self.user_id}")


class UMSheetSrcMixin():
    """
    Simple mixin for formatting self.sheet_src.
    """
    @property
    def sheet_src_str(self):
        text = 'None'
        if self.sheet_src == EUMSheet.main:
            text = 'EUMSheet.main'
        elif self.sheet_src == EUMSheet.snipe:
            text = 'EUMSheet.snipe'

        return text


class AUMUser(ArchiveMixin, UMSheetSrcMixin, Base):
    """
    Archival copy of cogdb.schema.UMUser augmented with a cycle field.
    """
    __tablename__ = 'archive_hudson_um_users'
    __table_args__ = (
        sqla.UniqueConstraint('cycle', 'sheet_src', 'name', name='umuser_cycle_sheet_name_constraint'),
        sqla.UniqueConstraint('cycle', 'sheet_src', 'row', name='umuser_cycle_sheet_row_constraint'),
    )
    _repr_keys = ['id', 'cycle', 'name', 'row', 'cry']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cycle = sqla.Column(sqla.Integer, default=cog.util.current_cycle)
    sheet_src = sqla.Column(sqla.Enum(EUMSheet), default=EUMSheet.main)
    name = sqla.Column(sqla.String(LEN['name']))  # Undeclared FK to discord_users
    row = sqla.Column(sqla.Integer)
    cry = sqla.Column(sqla.String(LEN['name']), default='')

    # Relationships
    discord_user = sqla.orm.relationship(
        'DiscordUser', uselist=False,
        primaryjoin='foreign(AUMUser.name) == DiscordUser.pref_name'
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
                sqla.select(sqla.func.sum(AUMHold.held)).
                where(sqla.and_(AUMHold.user_id == cls.id,
                                AUMHold.sheet_src != EUMSheet.snipe)).
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
                sqla.select(sqla.func.sum(AUMHold.redeemed)).
                where(sqla.and_(AUMHold.user_id == cls.id,
                                AUMHold.sheet_src != EUMSheet.snipe)).
                label('redeemed'),
                0
            ),
            sqla.Integer
        )

    def __repr__(self):
        kwargs = [f'{key}={getattr(self, key)!r}' for key in self._repr_keys]
        kwargs.insert(1, f"sheet_src={self.sheet_src_str}")

        return f'{self.__class__.__name__}({", ".join(kwargs)})'

    def __eq__(self, other):
        return isinstance(other, AUMUser) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f'{self.cycle}_{self.sheet_src_str}_{self.name}')


class AUMSystem(ArchiveMixin, UMSheetSrcMixin, Base):
    """
    Archival copy of cogdb.schema.UMSystem augmented with a cycle field.
    """
    __tablename__ = 'archive_hudson_um_systems'
    __table_args__ = (
        UniqueConstraint('cycle', 'sheet_src', 'name', name='umsystem_cycle_name_unique'),
        UniqueConstraint('cycle', 'sheet_src', 'sheet_col', name='umsystem_sheetcol_cycle_constraint'),
    )
    _repr_keys = [
        'id', 'cycle', 'name', 'sheet_col', 'goal', 'security', 'notes',
        'progress_us', 'progress_them', 'close_control', 'priority', 'map_offset'
    ]

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    cycle = sqla.Column(sqla.Integer, default=cog.util.current_cycle)
    sheet_src = sqla.Column(sqla.Enum(EUMSheet), default=EUMSheet.main)
    name = sqla.Column(sqla.String(LEN['name']))
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

    __mapper_args__ = {
        'polymorphic_identity': EUMType.control,
        'polymorphic_on': type,
    }

    @staticmethod
    def factory(kwargs):
        """ Simple factory to make undermining systems. """
        cls = kwargs.pop('cls')
        return cls(**kwargs)

    def __repr__(self):
        kwargs = [f'{key}={getattr(self, key)!r}' for key in self._repr_keys]
        kwargs.insert(1, f"sheet_src={self.sheet_src_str}")

        return f'{self.__class__.__name__}({", ".join(kwargs)})'

    def __eq__(self, other):
        return isinstance(other, AUMSystem) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f'{self.cycle}_{self.sheet_src_str}_{self.name}')

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
                sqla.select(sqla.func.sum(AUMHold.held)).
                where(AUMHold.system_id == cls.id).
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
                sqla.select(sqla.func.sum(AUMHold.held + AUMHold.redeemed)).
                where(AUMHold.system_id == cls.id).
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


class AUMExpand(AUMSystem):
    """
    Archival copy of cogdb.schema.UMExpand augmented with a cycle field.
    """
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


class AUMOppose(AUMExpand):
    """
    Archival copy of cogdb.schema.UMOppose augmented with a cycle field.
    """
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


class AUMHold(ArchiveMixin, UMSheetSrcMixin, Base):
    """
    Archival copy of cogdb.schema.UMHold augmented with a cycle field.
    """
    __tablename__ = 'archive_hudson_um_merits'
    __table_args__ = (
        sqla.UniqueConstraint('cycle', 'sheet_src', 'system_id', 'user_id', name='umhold_cycle_sheet_systemid_userid_constraint'),
    )
    _repr_keys = ['id', 'system_id', 'user_id', 'cycle', 'held', 'redeemed']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    sheet_src = sqla.Column(sqla.Enum(EUMSheet), default=EUMSheet.main)
    system_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('archive_hudson_um_systems.id'), nullable=False)
    user_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('archive_hudson_um_users.id'), nullable=False)
    cycle = sqla.Column(sqla.Integer, default=cog.util.current_cycle)
    held = sqla.Column(sqla.Integer, default=0, nullable=False)
    redeemed = sqla.Column(sqla.Integer, default=0, nullable=False)

    def __repr__(self):
        kwargs = [f'{key}={getattr(self, key)!r}' for key in self._repr_keys]
        kwargs.insert(1, f"sheet_src={self.sheet_src_str}")

        return f'{self.__class__.__name__}({", ".join(kwargs)})'

    def __eq__(self, other):
        return isinstance(other, AUMHold) and hash(self) == hash(other)

    def __lt__(self, other):
        return self.cycle < other.cycle and self.held + self.redeemed < other.held + other.redeemed

    def __hash__(self):
        return hash(f"{self.cycle}_{self.sheet_src}_{self.system_id}_{self.user_id}")


def empty_tables(session, *, perm=False):
    """
    Drop all archival tables.

    Args:
        session: A session onto the database.
        perm: When True, delete DiscordUser objects as well, otherwise no.
    """
    classes = [AFortDrop, AUMHold, AFortSystem, AUMSystem, AFortUser, AUMUser]
    if perm:
        classes += [DiscordUser]

    for cls in classes:
        try:
            session.query(cls).delete()
        except sqla.exc.ProgrammingError:  # Table was deleted or some other problem, attempt to recreate
            pass
    session.commit()


def recreate_tables():
    """
    Recreate all archival tables.
    The tables will first be dropped then recreated.
    """
    exclude = []
    if not cogdb.TEST_DB:
        exclude = [DiscordUser.__tablename__]
    sqlalchemy.orm.session.close_all_sessions()

    meta = sqlalchemy.MetaData(bind=cogdb.engine)
    meta.reflect()
    for tbl in reversed(meta.sorted_tables):
        try:
            if not str(tbl) in exclude:
                tbl.drop()
        except sqla.exc.OperationalError:
            pass
    Base.metadata.create_all(cogdb.engine)


if cogdb.TEST_DB:
    recreate_tables()
else:
    Base.metadata.create_all(cogdb.engine)


def main():
    """
    Main entry for testing archive tables, simple demo of idea.
    """
    recreate_tables()
    with cogdb.session_scope(cogdb.Session) as session:
        session.add(AFortUser(
            id=1,
            cycle=1,
            name='user1',
            row=10,
        ))
        session.commit()

        fortu = FortUser(id=22, name='user2', row=10)
        archive = AFortUser.archive(fortu, cycle=22)
        session.add(archive)
        session.commit()
        print(archive)


if __name__ == "__main__":
    main()
