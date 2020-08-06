"""
Module should handle logic related to querying/manipulating tables from a high level.
"""
from __future__ import absolute_import, print_function
import logging
import os
import sys
import tempfile

import sqlalchemy.exc as sqla_exc
import sqlalchemy.orm.exc as sqla_oexc

import cog.exc
import cog.sheets
from cog.util import substr_match
import cogdb
from cogdb.schema import (DUser, System, PrepSystem, SystemUM, SheetRow, SheetCattle, SheetUM,
                          Drop, Hold, EFaction, ESheetType, kwargs_fort_system, kwargs_um_system,
                          Admin, ChannelPerm, RolePerm, FortOrder, KOS)
from cogdb.side import HUDSON_CONTROLS, WINTERS_CONTROLS


DEFER_MISSING = 750


def fuzzy_find(needle, stack, obj_attr='zzzz', ignore_case=True):
    """
    Searches for needle in whole stack and gathers matches. Returns match if only 1.

    Raise separate exceptions for NoMatch and MoreThanOneMatch.
    """
    matches = []
    for obj in stack:
        try:
            if substr_match(needle, getattr(obj, obj_attr, obj), ignore_case=ignore_case):
                matches.append(obj)
        except cog.exc.NoMatch:
            pass

    num_matches = len(matches)
    if num_matches == 1:
        return matches[0]

    if num_matches == 0:
        cls = stack[0].__class__.__name__ if getattr(stack[0], '__class__') else 'string'
        raise cog.exc.NoMatch(needle, cls)

    raise cog.exc.MoreThanOneMatch(needle, matches, obj_attr)


def dump_db():  # pragma: no cover
    """
    Purely debug function, shunts db contents into file for examination.
    """
    session = cogdb.Session()
    fname = os.path.join(tempfile.gettempdir(), 'dbdump_' + os.environ.get('COG_TOKEN', 'dev'))
    print("Dumping db contents to:", fname)
    with open(fname, 'w') as fout:
        for cls in [DUser, SheetRow, System, SystemUM, Drop, Hold,
                    FortOrder, Admin, RolePerm, ChannelPerm]:
            fout.write('---- ' + str(cls) + ' ----\n')
            fout.writelines([str(obj) + "\n" for obj in session.query(cls)])


def get_duser(session, discord_id):
    """
    Return the DUser that has the same discord_id.

    Raises:
        NoMatch - No possible match found.
    """
    try:
        return session.query(DUser).filter_by(id=discord_id).one()
    except sqla_oexc.NoResultFound:
        raise cog.exc.NoMatch(discord_id, 'DUser')


def ensure_duser(session, member):
    """
    Ensure a member has an entry in the dusers table. A DUser is required by all users.

    Returns: The DUser
    """
    try:
        duser = get_duser(session, member.id)
        duser.display_name = member.display_name
    except cog.exc.NoMatch:
        duser = add_duser(session, member)

    return duser


def add_duser(session, member, *, faction=EFaction.hudson):
    """
    Add a discord user to the database.
    """
    name = member.display_name
    new_duser = DUser(id=member.id, display_name=name,
                      pref_name=name, faction=faction)
    session.add(new_duser)
    session.commit()

    return new_duser


def check_pref_name(session, duser, new_name):
    """
    Check that new name is not taken by another DUser or present as a stray in SheetRows.

    Raises:
        InvalidCommandArgs - DUser.pref_name taken by another DUser.
    """
    others = session.query(DUser).filter(DUser.id != duser.id, DUser.pref_name == new_name).all()
    if others:
        raise cog.exc.InvalidCommandArgs(
            "Sheet name {}, taken by {}.\n\nPlease choose another.".format(
                new_name, others[0].display_name))

    # Note: Unlikely needed, should be caught above. However, no fixed relationship guaranteeing.
    # for sheet in session.query(SheetRow).filter(SheetRow.name == new_name).all():
        # if sheet.duser(session).id != duser.id:
            # raise cog.exc.InvalidCommandArgs("Sheet name {}, taken by {}.\n\nPlease choose another.".format(new_name, sheet.duser(session).display_name))


def next_sheet_row(session, *, cls, faction, start_row):
    """
    Find the next available row to add a SheetRow for.

    Must scan all users, gaps may exist in sheet.
    """
    try:
        users = session.query(cls).filter_by(faction=faction).order_by(cls.row).all()
        last_user = users[0]
        next_row = users[-1].row + 1
        for user in users[1:]:
            if last_user.row + 1 != user.row:
                next_row = last_user.row + 1
                break
            last_user = user
    except IndexError:
        next_row = start_row

    return next_row


def add_sheet(session, name, **kwargs):
    """
    Simply add user past last user in sheet.

    Kwargs:
        cry: The cry to use.
        faction: By default Hudson. Any of EFaction.
        type: By default cattle sheet. Any of SheetRow subclasses.
        start_row: Starting row if none inserted.
    """
    faction = kwargs.get('faction', EFaction.hudson)
    cls = getattr(sys.modules[__name__], kwargs.get('type', ESheetType.cattle))
    cry = kwargs.get('cry', '')

    next_row = next_sheet_row(session, cls=cls, faction=faction,
                              start_row=kwargs['start_row'])
    sheet = cls(name=name, cry=cry, row=next_row, faction=faction)
    session.add(sheet)
    session.commit()

    return sheet


def fort_get_medium_systems(session):
    """
    Return unfortified systems designated for small/medium ships.
    """
    mediums = session.query(System).all()
    unforted = [med for med in mediums if "S/M" in med.notes and not med.is_fortified and not
                med.skip and not med.missing < DEFER_MISSING]
    return unforted


def fort_get_systems(session, mediums=True):
    """
    Return a list of all Systems. PrepSystems are not included.

    args:
        mediums: If false, exclude all systems designated for j
                 Determined by "S/M" being in notes.
    """
    query = session.query(System).filter(System.type != 'prep')
    if not mediums:
        med_names = [med.name for med in fort_get_medium_systems(session)]
        query = query.filter(System.name.notin_(med_names))

    return query.all()


def fort_get_preps(session):
    """
    Return a list of all PrepSystems.
    """
    return session.query(PrepSystem).all()


def fort_find_current_index(session):
    """
    Scan Systems from the beginning to find next unfortified target that is not Othime.

    Raises:
        NoMoreTargets - No more targets left OR a serious problem with data.
    """
    for ind, system in enumerate(fort_get_systems(session)):
        if system.is_fortified or system.skip or system.missing < DEFER_MISSING:
            continue

        return ind

    raise cog.exc.NoMoreTargets('No more fort targets at this time.')


def fort_find_system(session, system_name, search_all=True):
    """
    Return the System with System.name that matches.
    If search_all True, search all systems.
    If search_all False, search from current target forward.

    Raises:
        NoMatch - No possible match found.
        MoreThanOneMatch - Too many matches possible, ask user to resubmit.
    """
    try:
        return session.query(System).filter_by(name=system_name).one()
    except (sqla_oexc.NoResultFound, sqla_oexc.MultipleResultsFound):
        index = 0 if search_all else fort_find_current_index(session)
        systems = fort_get_systems(session)[index:] + fort_get_preps(session)
        return fuzzy_find(system_name, systems, 'name')


def fort_get_systems_by_state(session):
    """
    Return a dictionary that lists the systems states below:

        left: Has neither been fortified nor undermined.
        fortified: Has been fortified and not undermined.
        undermined: Has been undermined and not fortified.
        cancelled: Has been both fortified and undermined.
    """
    log = logging.getLogger('cogdb.query')
    states = {
        'cancelled': [],
        'fortified': [],
        'left': [],
        'undermined': [],
        'skipped': [],
    }

    for system in fort_get_systems(session):
        log.info('STATE - %s', system)
        if system.is_fortified and system.is_undermined:
            states['cancelled'].append(system)
        if system.is_undermined:
            states['undermined'].append(system)
        if system.is_fortified:
            states['fortified'].append(system)
        if not system.is_fortified and not system.skip:
            states['left'].append(system)
        if system.skip:
            states['skipped'].append(system)

    return states


def fort_get_targets(session):
    """
    Returns a list of Systems that should be fortified.

    - First System is not Othime and is unfortified.
    - Second System if prsent is Othime, only when not fortified.
    - All Systems after are prep targets.
    """
    targets = fort_order_get(session)
    if targets:
        return targets[:1]

    current = fort_find_current_index(session)
    systems = fort_get_systems(session)
    targets = [systems[current]]

    mediums = fort_get_medium_systems(session)
    if mediums and mediums[0].name != targets[0].name:
        targets.append(mediums[0])

    targets += fort_get_preps(session)

    return targets


def fort_get_next_targets(session, count=1):
    """
    Return next 'count' fort targets.
    """
    systems = fort_order_get(session)
    start = 1
    if not systems:
        systems = fort_get_systems(session)
        start = fort_find_current_index(session) + 1

    targets = []
    for system in systems[start:]:
        if system.is_fortified or system.skip or system.missing < DEFER_MISSING:
            continue

        targets.append(system)
        count = count - 1

        if count == 0:
            break

    return targets


def fort_get_deferred_targets(session):
    """
    Return all deferred targets under deferal amount.
    """
    return [system for system in fort_get_systems(session)
            if system.missing < DEFER_MISSING and not system.is_fortified]


def fort_add_drop(session, *, user, system, amount):
    """
    Add a Drop for 'amount' to the database where Drop intersects at:
        System.name and SUser.name
    If fort exists, increment its value. Else add it to database.

    Kwargs: system, user, amount

    Returns: The Drop object.

    Raises:
        InvalidCommandArgs: User requested an amount outside bounds [-800, 800]
    """
    if amount not in range(-800, 801):
        raise cog.exc.InvalidCommandArgs('Drop amount must be in range [-800, 800]')

    try:
        drop = session.query(Drop).filter_by(user_id=user.id, system_id=system.id).one()
    except sqla_oexc.NoResultFound:
        drop = Drop(user_id=user.id, system_id=system.id, amount=0)
        session.add(drop)

    log = logging.getLogger('cogdb.query')
    log.info('ADD_DROP - Before: Drop %s, System %s', drop, system)
    drop.amount = max(0, drop.amount + amount)
    system.fort_status = system.fort_status + amount
    session.commit()
    log.info('ADD_DROP - After: Drop %s, System %s', drop, system)

    return drop


def fort_order_get(_):
    """
    Get the order of systems to fort.

    If any systems have been completed, remove them from the list.

    Returns: [] if no systems set, else a list of System objects.
    """
    systems = []
    dsession = cogdb.Session()  # Isolate deletions, feels a bit off though
    for fort_order in dsession.query(FortOrder).order_by(FortOrder.order):
        system = dsession.query(System).filter_by(name=fort_order.system_name).one()
        if system.is_fortified or system.missing < DEFER_MISSING:
            dsession.delete(fort_order)
        else:
            systems += [system]

    dsession.commit()
    return systems


def fort_order_set(session, system_names):
    """
    Simply set the systems in the order desired.

    Ensure systems are actually valid before.
    """
    try:
        for ind, system_name in enumerate(system_names):
            if not isinstance(system_name, System):
                system_name = fort_find_system(session, system_name).name
            session.add(FortOrder(order=ind, system_name=system_name))
        session.commit()
    except (sqla_exc.IntegrityError, sqla_oexc.FlushError):
        session.rollback()
        raise cog.exc.InvalidCommandArgs("Duplicate system specified, check your command!")
    except cog.exc.NoMatch:
        session.rollback()
        raise cog.exc.InvalidCommandArgs("System '{}' not found in fort systems.".format(system_name))


def fort_order_drop(session, systems):
    """
    Drop the given system_names from the override table.
    """
    for system_name in systems:
        try:
            if isinstance(system_name, System):
                system_name = system_name.name
            session.delete(session.query(FortOrder).filter_by(system_name=system_name).one())
        except sqla_oexc.NoResultFound:
            pass

    session.commit()


class FortScanner():
    """
    Scanner for the Hudson fort sheet.

    args:
        asheet: The AsyncGSheet that connects to the sheet.
    """
    SYSTEM_RANGE = '{}10:{}10'  # Fill in max column

    def __init__(self, asheet, user_args=None, db_classes=None):
        self.asheet = asheet
        self.users_args = user_args
        self.db_classes = db_classes if db_classes else [Drop, System, SheetCattle]
        self.users_args = user_args if user_args else [SheetCattle, EFaction.hudson]

        self._cells = None
        self._cells_column = None
        self.system_col = None
        self.user_col = 'B'
        self.user_row = 11

    @property
    def cells_row_major(self):
        """ Cells of the sheet by row major dimension. """
        return self._cells

    @property
    def cells_column_major(self):
        """ Cells of the sheet by column major dimension. """
        if not self._cells_column:
            self._cells_column = cog.util.transpose_table(self._cells)

        return self._cells_column

    async def update_cells(self):
        """ Fetch all cells from the sheet. """
        self._cells = await self.asheet.whole_sheet()
        self._cells_column = None

    def drop_entries(self, session):
        """
        Before scan, drop the matching entries in the table.
        """
        for cls in self.db_classes:
            for matched in session.query(cls):
                session.delete(matched)

    def scan(self):
        """
        Scan the entire sheet for all data.
        Update the cells before parsing.
        """
        self.system_col = self.find_system_column()

        systems = self.fort_systems() + self.prep_systems()
        users = self.users(first_id=1)
        merits = self.merits(systems, users)

        session = cogdb.Session()
        self.drop_entries(session)
        session.commit()
        session.add_all(systems + users)
        session.commit()
        session.add_all(merits)
        session.commit()

        return True

    def users(self, *, row_cnt=None, first_id=1):
        """
        Scan the users in the sheet and return sheet user objects.

        Args:
            row_cnt: The starting row for users, zero indexed. Default is user_row.
            first_id: The id to start for the users.
        """
        found = []
        cls, faction = self.user_args
        print(row_cnt, str(cls), str(faction))

        if not row_cnt:
            row_cnt = self.user_row - 1

        users = [x[row_cnt:self.asheet.last_row] for x in self.cells_column_major[:2]]
        for cry, name in list(zip(*users)):
            row_cnt += 1
            if name.strip() == '':
                continue

            sheet_user = cls(id=first_id, cry=cry, name=name, faction=faction, row=row_cnt)
            first_id += 1
            if sheet_user in found:
                rows = [other.row for other in found if other == sheet_user] + [row_cnt]
                raise cog.exc.NameCollisionError("Fort", sheet_user.name, rows)

            found += [sheet_user]

        return found

    def fort_systems(self):
        """
        Scan and parse the system information into System objects.

        Returns:
            A list of System objects to be put in database.
        """
        found, order, cell_column = [], 1, cog.sheets.Column(self.system_col)
        ind = cog.sheets.column_to_index(self.system_col, zero_index=True)

        for col in self.cells_column_major[ind:]:
            kwargs = kwargs_fort_system(col[0:10], order, str(cell_column))
            kwargs['id'] = order
            found += [System(**kwargs)]

            order += 1
            cell_column.fwd()

        return found

    def prep_systems(self):
        """
        Scan the Prep systems if any into the System db.

        Preps exist in range [D, system_col)
        """
        found, order, cell_column = [], 0, cog.sheets.Column('C')
        first_prep = cog.sheets.column_to_index(str(cell_column))
        first_system = cog.sheets.column_to_index(self.system_col) - 1

        for col in self.cells_column_major[first_prep:first_system]:
            order = order + 1
            cell_column.fwd()
            col = col[0:10]

            if col[-1].strip() == "TBA":
                continue

            kwargs = kwargs_fort_system(col, order, str(cell_column))
            kwargs['id'] = 1000 + order

            found += [PrepSystem(**kwargs)]

        return found

    def merits(self, systems, users):
        """
        Scan the fortification area of the sheet and return Drop objects representing
        merits each user has dropped in System.

        Args:
            systems: The list of Systems in the order entered in the sheet.
            users: The list of Users in order the order entered in the sheet.
        """
        found, cnt = [], 1

        for system in systems:
            ind = cog.sheets.column_to_index(system.sheet_col, zero_index=True)
            merit_cells = self.cells_column_major[ind][10:]

            print(len(merit_cells), len(users))
            for user in users:
                try:
                    amount = int(merit_cells.pop(0).strip())
                    found.append(Drop(id=cnt, user_id=user.id, system_id=system.id,
                                      amount=cogdb.schema.parse_int(amount)))
                    cnt += 1

                except ValueError:
                    pass

        return found

    def find_system_column(self):
        """
        Find the first column that has a system cell in it.
        Determined based on TBA columns.

        Raises:
            SheetParsingError when fails to locate the system column.
        """
        row = self.cells_row_major[9]
        col_count = cog.sheets.Column()

        next_not_tba = False
        for cell in row:
            if next_not_tba and cell.strip() != 'TBA':
                return str(col_count)

            if cell == 'TBA':
                next_not_tba = True

            col_count.next()

        raise cog.exc.SheetParsingError("Unable to determine system column.")

    async def send_batch(self, dicts):
        """
        Seend a batch update made up from premade range/value dicts.
        """
        logging.getLogger("cogdb.query").info("Sending update to Fort Sheet.\n%s", str(dicts))
        await self.asheet.batch_update(dicts)

    @staticmethod
    def update_sheet_user_dict(row, cry, name):
        """
        Create an update user dict. See AsyncGSheet.batch_update

        Returns: A list of update dicts to pass to batch_update.
        """
        cell_range = 'A{row}:B{row}'.format(row=row)
        return [{'range': cell_range, 'values': [[cry, name]]}]

    @staticmethod
    def update_drop_dict(system_col, user_row, amount):
        """
        Create an update drop dict. See AsyncGSheet.batch_update

        Returns: A list of update dicts to pass to batch_update.
        """
        cell_range = '{col}{row}:{col}{row}'.format(col=system_col, row=user_row)
        return [{'range': cell_range, 'values': [[amount]]}]

    @staticmethod
    def update_system_dict(col, fort_status, um_status):
        """
        Create an update system dict. See AsyncGSheet.batch_update

        Returns: A list of update dicts to pass to batch_update.
        """
        cell_range = '{col}6:{col}7'.format(col=col)
        return [{'range': cell_range, 'values': [[fort_status, um_status]]}]


class UMScanner(FortScanner):
    """
    Scanner for the Hudson undermine sheet.

    args:
        asheet: The AsyncGSheet that connects to the sheet.
    """
    def __init__(self, asheet):
        super().__init__(asheet, [SheetUM, EFaction.hudson], [Hold, SystemUM, SheetUM])

        # These are fixed based on current format
        self.system_col = 'D'
        self.user_col = 'B'
        self.user_row = 14

    def scan(self):
        """
        Main function, scan the sheet into the database.
        """
        systems = self.systems()
        users = self.users(first_id=1001)
        merits = self.merits(systems, users)

        session = cogdb.Session()
        self.drop_entries(session)
        session.commit()
        session.add_all(systems + users)
        session.commit()
        session.add_all(merits)
        session.commit()

        return True

    def systems(self):
        """
        Scan all the systems in the sheet.
        A UM System takes up two adjacent columns.

        Returns:
            A list of UMSystems to insert into db.
        """
        cell_column = cog.sheets.Column(self.system_col)
        found, cnt, sys_ind = [], 1, 3

        while True:
            sys_cells = [x[:13] for x in self.cells_column_major[sys_ind:sys_ind + 2]]

            if not sys_cells[0][8] or 'Template' in sys_cells[0][8]:
                break

            kwargs = kwargs_um_system(sys_cells, str(cell_column))
            kwargs['id'] = cnt
            cnt += 1
            cls = kwargs.pop('cls')
            found += [(cls(**kwargs))]

            sys_ind += 2
            cell_column.offset(2)

        return found

    def merits(self, systems, users):
        """
        Parse the held and redeemed merits that fall under the same column as System.

        Args:
            systems: The SystemUMs parsed from sheet.
            users: The SheetRows parsed from sheet.

        Returns:
            A list of Hold objects to put in db.
        """
        found, cnt = [], 1

        for system in systems:
            sys_ind = cog.sheets.column_to_index(system.sheet_col, zero_index=True)
            sys_cells = [x[13:self.asheet.last_row] for x
                         in self.cells_column_major[sys_ind:sys_ind + 2]]

            for user_ind, row in enumerate(zip(*sys_cells)):
                held, redeemed = row
                if held.strip() == '' and redeemed.strip() == '':
                    continue

                held = cogdb.schema.parse_int(held)
                redeemed = cogdb.schema.parse_int(redeemed)
                hold = Hold(id=cnt, user_id=users[user_ind].id, system_id=system.id,
                            held=held, redeemed=redeemed)
                found += [hold]

                cnt += 1

        return found

    @staticmethod
    def update_hold_dict(system_col, user_row, held, redeemed):
        """
        Create an update hold dict. See AsyncGSheet.batch_update

        Returns: A list of update dicts to pass to batch_update.
        """
        col2 = cog.sheets.Column(system_col).next()
        cell_range = '!{col1}{row1}:{col2}{row2}'.format(col1=system_col, col2=col2,
                                                    row1=user_row, row2=user_row + 1)
        return [{'range': cell_range, 'values': [[held, redeemed]]}]

    @staticmethod
    def update_system_dict(col, progress_us, progress_them, map_offset):
        """
        Create an update system dict. See AsyncGSheet.batch_update

        Returns: A list of update dicts to pass to batch_update.
        """
        cell_range = '{col}10:{col}13'.format(col=col)
        values = [[progress_us, progress_them, 'Hold Merits', map_offset]]
        return [{'range': cell_range, 'values': values}]


class KOSScanner(FortScanner):
    """
    Scanner for the Hudson KOS sheet.

    args:
        asheet: The AsyncGSheet that connects to the sheet.
    """
    def __init__(self, asheet):
        super().__init__(asheet, None, [KOS])

    def scan(self):
        """
        Main function, scan the sheet into the database.

        Raises:
            SheetParsingError - Duplicate CMDRs detected.
        """
        entries = self.kos_entries()

        dupe_entries = entries[:]
        for ent in set(entries):
            dupe_entries.remove(ent)
        if dupe_entries:
            cmdrs = ["CMDR {} duplicated in sheet".format(x.cmdr) for x in dupe_entries]
            raise cog.exc.SheetParsingError("Duplicate CMDRs in KOS sheet.\n\n" + '\n'.join(cmdrs))

        session = cogdb.Session()
        self.drop_entries(session)
        session.commit()
        session.add_all(entries)
        session.commit()

        return True

    def kos_entries(self):
        """
        Process all the entries in the sheet into KOS objects for db.

        Returns:
            A list of KOS objects for db.
        """
        found = []

        for cnt, row in enumerate(self.cells_row_major[1:]):
            try:
                danger = int(row[2])
            except ValueError:
                danger = 0
            is_friendly = row[3][0] in ('f', 'F')
            found += [cogdb.schema.KOS(id=cnt, cmdr=row[0], faction=row[1], danger=danger,
                                       is_friendly=is_friendly)]

            cnt += 1

        return found

    @staticmethod
    def kos_report_dict(row, reported_by, cmdr, reason):
        """
        Create an update system dict. See AsyncGSheet.batch_update

        Returns: A list of update dicts to pass to batch_update.
        """
        values = [[reported_by, cmdr, reason]]
        cell_range = '!A{row}:C{row}'.format(row=row)
        return [{'range': cell_range, 'values': values}]


def um_find_system(session, system_name):
    """
    Find the SystemUM with system_name
    """
    try:
        return session.query(SystemUM).filter_by(name=system_name).one()
    except (sqla_oexc.NoResultFound, sqla_oexc.MultipleResultsFound):
        systems = session.query(SystemUM).\
            filter(SystemUM.name.ilike('%{}%'.format(system_name))).\
            all()

        if len(systems) > 1:
            raise cog.exc.MoreThanOneMatch(system_name, systems, SystemUM)

        if len(systems) == 0:
            raise cog.exc.NoMatch(system_name, SystemUM)

        return systems[0]




def um_get_systems(session, exclude_finished=True):
    """
    Return a list of all current undermining targets.

    kwargs:
        finished: Return just the finished targets.
    """
    query = session.query(SystemUM)
    if exclude_finished:
        query = query.filter(SystemUM.is_undermined is False)

    return query.all()


def um_reset_held(session, user):
    """
    Reset all held merits to 0.
    """
    holds = session.query(Hold).filter_by(user_id=user.id).all()
    for hold in holds:
        hold.held = 0

    session.commit()
    return holds


def um_redeem_merits(session, user):
    """
    Redeem all held merits for user.
    """
    total = 0
    holds = session.query(Hold).filter_by(user_id=user.id).all()
    for hold in holds:
        total += hold.held
        hold.redeemed = hold.redeemed + hold.held
        hold.held = 0

    session.commit()
    return (holds, total)


def um_add_hold(session, **kwargs):
    """
    Add or update the user's Hold, that is their UM merits held or redeemed.
        System.name and SUser.name
    If Hold exists, increment the held value. Otherwise add it to database.

    Returns: The Hold object.

    Raises:
        InvalidCommandArgs: Hold cannot be negative.
    """
    system = kwargs['system']
    user = kwargs['user']
    held = kwargs['held']

    if held < 0:
        raise cog.exc.InvalidCommandArgs('Hold amount must be in range [0, \u221E]')

    try:
        hold = session.query(Hold).filter_by(user_id=user.id,
                                             system_id=system.id).one()
    except sqla_oexc.NoResultFound:
        hold = Hold(user_id=user.id, system_id=system.id, held=0, redeemed=0)
        session.add(hold)

    hold.held = held
    session.commit()

    return hold


def um_all_held_merits(session):
    """
    Return a list of lists that show all users with merits still held.

    List of the form:
    [
        [CMDR, system_name_1, system_name_2, ...],
        [cmdrname, merits_system_1, merits_system_2, ...],
        [cmdrname, merits_system_1, merits_system_2, ...],
    ]
    """
    c_dict = {}
    for merit in session.query(Hold).filter(Hold.held > 0).order_by(Hold.system_id).all():
        try:
            c_dict[merit.user.name][merit.system.name] = merit
        except KeyError:
            c_dict[merit.user.name] = {merit.system.name: merit}

    systems = session.query(SystemUM).order_by(SystemUM.id).all()
    system_names = [sys.name for sys in systems]
    rows = []
    for cmdr in c_dict:
        row = [cmdr]
        for system_name in system_names:
            try:
                row += [c_dict[cmdr][system_name].held]
            except KeyError:
                row += [0]

        rows += [row]

    return [['CMDR'] + system_names] + rows


def get_admin(session, member):
    """
    If the member is an admin, return the Admin.
    Otherwise, raise NoMatch.
    """
    try:
        return session.query(Admin).filter_by(id=member.id).one()
    except sqla_oexc.NoResultFound:
        raise cog.exc.NoMatch(member.display_name, 'Admin')


def add_admin(session, member):
    """
    Add a new admin.
    """
    try:
        session.add(Admin(id=member.id))
        session.commit()
    except (sqla_exc.IntegrityError, sqla_oexc.FlushError):
        raise cog.exc.InvalidCommandArgs("Member {} is already an admin.".format(member.display_name))


def add_channel_perm(session, cmd, server_name, channel_name):
    try:
        session.add(ChannelPerm(cmd=cmd, server=server_name, channel=channel_name))
        session.commit()
    except (sqla_exc.IntegrityError, sqla_oexc.FlushError):
        raise cog.exc.InvalidCommandArgs("Channel permission already exists.")


def add_role_perm(session, cmd, server_name, role_name):
    try:
        session.add(RolePerm(cmd=cmd, server=server_name, role=role_name))
        session.commit()
    except (sqla_exc.IntegrityError, sqla_oexc.FlushError):
        raise cog.exc.InvalidCommandArgs("Role permission already exists.")


def remove_channel_perm(session, cmd, server_name, channel_name):
    try:
        session.delete(session.query(ChannelPerm).
                       filter_by(cmd=cmd, server=server_name, channel=channel_name).one())
        session.commit()
    except sqla_oexc.NoResultFound:
        raise cog.exc.InvalidCommandArgs("Channel permission does not exist.")


def remove_role_perm(session, cmd, server_name, role_name):
    try:
        session.delete(session.query(RolePerm).
                       filter_by(cmd=cmd, server=server_name, role=role_name).one())
        session.commit()
    except sqla_oexc.NoResultFound:
        raise cog.exc.InvalidCommandArgs("Role permission does not exist.")


def check_perms(msg, args):
    """
    Check if a user is authorized to issue this command.
    Checks will be made against channel and user roles.

    Raises InvalidPerms if any permission issue.
    """
    session = cogdb.Session()
    check_channel_perms(session, args.cmd, msg.channel.guild.name, msg.channel.name)
    check_role_perms(session, args.cmd, msg.channel.guild.name, msg.author.roles)


def check_channel_perms(session, cmd, server_name, channel_name):
    """
    A user is allowed to issue a command if:
        a) no restrictions for the cmd
        b) the channel is whitelisted in the restricted channels

    Raises InvalidPerms if fails permission check.
    """
    channels = [perm.channel for perm in session.query(ChannelPerm).
                filter_by(cmd=cmd, server=server_name)]
    if channels and channel_name not in channels:
        raise cog.exc.InvalidPerms("The '{}' command is not permitted on this channel.".format(
            cmd.lower()))


def check_role_perms(session, cmd, server_name, member_roles):
    """
    A user is allowed to issue a command if:
        a) no roles set for the cmd
        b) he matches ANY of the set roles

    Raises InvalidPerms if fails permission check.
    """
    perm_roles = {perm.role for perm in session.query(RolePerm).
                  filter_by(cmd=cmd, server=server_name)}
    member_roles = {role.name for role in member_roles}
    if perm_roles and len(member_roles - perm_roles) == len(member_roles):
        raise cog.exc.InvalidPerms("You do not have the roles for the command.")


def complete_control_name(partial, include_winters=False):
    """
    Provide name completion of Federal controls without db query.
    """
    systems = HUDSON_CONTROLS[:]
    if include_winters:
        systems += WINTERS_CONTROLS

    return fuzzy_find(partial, systems)


def kos_search_cmdr(session, term):
    """
    Search for a kos entry for cmdr.
    """
    term = '%' + str(term) + '%'
    return session.query(KOS).filter(KOS.cmdr.ilike(term)).all()


async def test_fortscanner():
    import cog.sheets
    paths = cog.util.get_config('paths')
    cog.sheets.AGCM = cog.sheets.init_agcm(paths['json'], paths['token'])

    sheet = cog.util.get_config('scanners', 'hudson_cattle')
    asheet = cog.sheets.AsyncGSheet(sheet['id'], sheet['page'])
    await asheet.init_sheet()

    fscan = AFortScanner(asheet)
    await fscan.update_cells()
    fscan.system_col = fscan.find_system_column()
    print(fscan.system_col)

    #  users = fscan.users()
    #  f_sys = fscan.fort_systems()
    #  p_sys = fscan.prep_systems()
    #  merits = fscan.merits(f_sys + p_sys, users)
    fscan.scan()


async def test_umscanner():
    import cog.sheets
    paths = cog.util.get_config('paths')
    cog.sheets.AGCM = cog.sheets.init_agcm(paths['json'], paths['token'])

    sheet = cog.util.get_config('scanners', 'hudson_undermine')
    asheet = cog.sheets.AsyncGSheet(sheet['id'], sheet['page'])
    await asheet.init_sheet()

    fscan = AUMScanner(asheet)
    await fscan.update_cells()

    systems = fscan.systems()
    print(systems)
    users = fscan.users()
    print(users)
    merits = fscan.merits(systems, users)
    print(merits)


async def test_kosscanner():
    import cog.sheets
    paths = cog.util.get_config('paths')
    cog.sheets.AGCM = cog.sheets.init_agcm(paths['json'], paths['token'])

    sheet = cog.util.get_config('scanners', 'hudson_kos')
    asheet = cog.sheets.AsyncGSheet(sheet['id'], sheet['page'])
    await asheet.init_sheet()

    fscan = AKOSScanner(asheet)
    await fscan.update_cells()

    fscan.scan()


def main():
    import asyncio
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.get_event_loop().set_debug(True)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_kosscanner())


if __name__ == "__main__":
    main()
