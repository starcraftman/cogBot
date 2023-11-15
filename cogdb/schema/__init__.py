"""
Define the major tables that are used by this bot.
These allow the bot to store and query the information in sheets that are parsed.
"""
import datetime
import enum
import time

import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm
import sqlalchemy.orm.session
import sqlalchemy.ext.declarative

import cogdb
from cogdb.schema.common import Base, LEN
from cogdb.schema.discord_user import DiscordUser
from cogdb.schema.fort import EFortType, FortOrder, FortDrop, FortUser, FortSystem, FortPrep
from cogdb.schema.global_config import Global
from cogdb.schema.kos import KOS
from cogdb.schema.permissions import AdminPerm, ChannelPerm, RolePerm
from cogdb.schema.tracking import EVENT_CARRIER, TRACK_SYSTEM_SEP, TrackByID, TrackSystem, TrackSystemCached
from cogdb.schema.undermine import EUMType, EUMSheet, UMHold, UMUser, UMSystem, UMExpand, UMOppose
from cogdb.schema.vote import EVoteType, Vote

import cog.exc
import cog.tbl
import cog.util
from cog.util import ReprMixin, TimestampMixin


class Consolidation(ReprMixin, Base):
    """
    Track the consolidation vote changes over time.
    """
    __tablename__ = 'consolidation_tracker'
    _repr_keys = ['id', 'cycle', 'amount', 'cons_total', 'prep_total', 'updated_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    amount = sqla.Column(sqla.Integer, default=0)
    cons_total = sqla.Column(sqla.Integer, default=0)
    prep_total = sqla.Column(sqla.Integer, default=0)
    updated_at = sqla.Column(sqla.DateTime, default=datetime.datetime.utcnow, unique=True)  # All dates UTC

    def __str__(self):
        """ A pretty one line to give all information. """
        return f"Consolidation {self.amount}% at {self.updated_at}."

    def __eq__(self, other):
        return isinstance(other, Consolidation) and hash(self) == hash(other)

    def __hash__(self):
        return hash(self.id)

    @sqla_orm.validates('cons_total', 'prep_total')
    def validate_totals(self, key, value):
        """ Validation function for cons_total and prep_total. """
        try:
            if value < 0:
                raise cog.exc.ValidationFail(f"Bounds check failed for: {key} with value {value}")
        except TypeError:
            pass

        return value

    @sqla_orm.validates('amount')
    def validate_amount(self, key, value):
        """ Validation function for amount. """
        try:
            if value < 0 or value > 100:
                raise cog.exc.ValidationFail(f"Bounds check failed for: {key} with value {value}")
        except TypeError:
            pass

        return value


class ESheetType(enum.Enum):
    """ Type of sheet the transaction modified. """
    fort = 1
    um = 2
    snipe = 3


class SheetRecord(ReprMixin, TimestampMixin, Base):
    """
    For every command modifying the local database and sheet, record a transaction.
    """
    __tablename__ = 'history_sheet_transactions'
    _repr_keys = ['id', 'discord_id', 'channel_id', 'sheet_src', 'cycle', 'command',
                  'flushed_sheet', 'created_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    discord_id = sqla.Column(sqla.BigInteger, nullable=False)
    channel_id = sqla.Column(sqla.BigInteger, nullable=False)
    sheet_src = sqla.Column(sqla.Enum(ESheetType), default=ESheetType.fort)
    cycle = sqla.Column(sqla.Integer, default=cog.util.current_cycle)
    command = sqla.Column(sqla.String(LEN['command']), default="")
    flushed_sheet = sqla.Column(sqla.Boolean, default=False)
    created_at = sqla.Column(sqla.Integer, default=time.time)

    # Relationships
    user = sqla_orm.relationship('DiscordUser', uselist=False, viewonly=True, lazy='joined',
                                 primaryjoin='foreign(SheetRecord.discord_id) == DiscordUser.id')

    def __eq__(self, other):
        return (isinstance(self, SheetRecord) and isinstance(other, SheetRecord)
                and self.id == other.id)

    def __hash__(self):
        return hash(self.id)


def kwargs_um_system(cells, sheet_col, *, sheet_src=EUMSheet.main):
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

    Args:
        cells: The cells to parse and use for kwargs initialization.
        sheet_col: The column of the sheet these cells came from.

    Kwargs:
        sheet_src: The sheet src, by default main.

    Raises:
        SheetParsingError - An error occurred during parsing of the cells.
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
            'sheet_src': sheet_src,
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
    except (IndexError, TypeError) as exc:
        raise cog.exc.SheetParsingError("Halt UMSystem parsing.") from exc


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
    except (IndexError, TypeError) as exc:
        raise cog.exc.SheetParsingError("Halt System parsing.") from exc


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
    classes = [SheetRecord, FortDrop, UMHold, FortSystem, UMSystem, FortUser, UMUser, KOS,
               KOS, TrackSystem, TrackSystemCached, TrackByID,
               AdminPerm, ChannelPerm, RolePerm]
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
            if not str(tbl) in exclude:
                tbl.drop()
        except sqla.exc.OperationalError:
            pass
    Base.metadata.create_all(cogdb.engine)

    with cogdb.engine.connect() as con:
        con.execute(sqla.sql.text(EVENT_CARRIER.format(cogdb.CUR_DB).strip()))


def run_schema_queries(session):  # pragma: no cover
    """
    Run a simple of tests.
    This section can be used to experiment with relations and changes.
    """
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
        FortPrep(name='Rhea', sheet_col='L', sheet_order=6, fort_status=0,
                 trigger=8000, notes="To prep"),
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
    res = session.query(FortSystem.name, FortSystem.cmdr_merits).filter(FortSystem.cmdr_merits > 1000).all()
    print(res)


if cogdb.TEST_DB:
    recreate_tables()
else:
    Base.metadata.create_all(cogdb.engine)


def main():  # pragma: no cover
    """
    This continues to exist only as a sanity test for schema and relations.
    """
    recreate_tables()
    with cogdb.session_scope(cogdb.Session) as session:
        run_schema_queries(session)
    recreate_tables()


if __name__ == "__main__":  # pragma: no cover
    main()
