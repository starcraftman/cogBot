"""
Module should handle logic related to querying/manipulating tables from a high level.
"""
from __future__ import absolute_import, print_function
import sys

import sqlalchemy.orm.exc as sqa_exc

import cog.exc
import cog.sheets
import cogdb
from cogdb.schema import (DUser, System, PrepSystem, SystemUM, SheetRow, SheetCattle, SheetUM,
                          Drop, Hold, EFaction, ESheetType, kwargs_fort_system, kwargs_um_system)


def subseq_match(needle, line, ignore_case=True):
    """
    True iff the subsequence needle present in line.
    """
    n_index, l_index, matches = 0, 0, 0

    if ignore_case:
        needle = needle.lower()
        line = line.lower()

    while n_index != len(needle):
        while l_index != len(line):
            if needle[n_index] == line[l_index]:
                matches += 1
                l_index += 1
                break

            # Stop searching if match no longer possible
            if len(needle[n_index:]) > len(line[l_index + 1:]):
                raise cog.exc.NoMatch(needle)

            l_index += 1
        n_index += 1

    return matches == len(needle)


def substr_match(needle, line, ignore_case=True):
    """
    True iff the substr is present in string. Ignore spaces and optionally case.
    """
    needle = needle.replace(' ', '')
    line = line.replace(' ', '')

    if ignore_case:
        needle = needle.lower()
        line = line.lower()

    return needle in line


def fuzzy_find(needle, stack, obj_attr='zzzz', ignore_case=True):
    """
    Searches for needle in whole stack and gathers matches. Returns match if only 1.

    Raise separate exceptions for NoMatch and MoreThanOneMatch.
    """
    matches = []
    for obj in stack:
        try:
            if substr_match(needle, getattr(obj, obj_attr, obj), ignore_case):
                matches.append(obj)
        except cog.exc.NoMatch:
            pass

    num_matches = len(matches)
    if num_matches == 1:
        return matches[0]
    elif num_matches == 0:
        cls = stack[0].__class__.__name__ if getattr(stack[0], '__class__') else 'string'
        raise cog.exc.NoMatch(needle, cls)
    else:
        raise cog.exc.MoreThanOneMatch(needle, matches, obj_attr)


def dump_db():  # pragma: no cover
    """
    Purely debug function, prints locally database.
    """
    session = cogdb.Session()
    print('Printing filled databases')
    classes = [DUser, SheetRow, System, SystemUM, Drop, Hold]
    for cls in classes:
        print('---- ' + str(cls) + ' ----')
        for obj in session.query(cls):
            print(obj)


def get_duser(session, discord_id):
    """
    Return the DUser that has the same discord_id.

    Raises:
        NoMatch - No possible match found.
    """
    try:
        return session.query(DUser).filter_by(id=discord_id).one()
    except sqa_exc.NoResultFound:
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

    try:
        next_row = session.query(cls).filter_by(faction=faction).all()[-1].row + 1
    except IndexError:
        next_row = kwargs['start_row']
    sheet = cls(name=name, cry=cry, row=next_row, faction=faction)
    session.add(sheet)
    session.commit()

    return sheet


def fort_get_othime(session):
    """
    Return the System Othime.
    """
    return session.query(System).filter_by(name='Othime').one()


def fort_get_systems(session, not_othime=False):
    """
    Return a list of all Systems. PrepSystems are not included.

    args:
        not_othime: If true, remove Othime from Systems results.
    """
    query = session.query(System).filter(System.type != 'prep')
    if not_othime:
        query = query.filter(System.name != 'Othime')

    return query.all()


def fort_get_preps(session):
    """
    Return a list of all PrepSystems.
    """
    return session.query(PrepSystem).all()


def fort_find_current_index(session):
    """
    Scan Systems from the beginning to find next unfortified target that is not Othime.
    """
    for ind, system in enumerate(fort_get_systems(session, not_othime=True)):
        if system.is_fortified or system.skip:
            continue

        return ind


def fort_find_system(session, system_name, search_all=False):
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
    except (sqa_exc.NoResultFound, sqa_exc.MultipleResultsFound):
        index = 0 if search_all else fort_find_current_index(session)
        systems = fort_get_preps(session) + fort_get_systems(session)[index:]
        return fuzzy_find(system_name, systems, 'name')


def fort_get_systems_by_state(session):
    """
    Return a dictionary that lists the systems states below:

        left: Has neither been fortified nor undermined.
        fortified: Has been fortified and not undermined.
        undermined: Has been undermined and not fortified.
        cancelled: Has been both fortified and undermined.
    """
    states = {
        'cancelled': [],
        'fortified': [],
        'left': [],
        'undermined': [],
        'skipped': [],
    }

    for system in fort_get_systems(session):
        if system.is_fortified and system.is_undermined:
            states['cancelled'].append(system)
        elif system.is_undermined:
            states['undermined'].append(system)
        elif system.is_fortified:
            states['fortified'].append(system)
        else:
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
    current = fort_find_current_index(session)
    systems = fort_get_systems(session, not_othime=True)
    othime = fort_get_othime(session)

    targets = [systems[current]]
    if not othime.is_fortified:
        targets.append(othime)
    targets += fort_get_preps(session)

    return targets


def fort_get_next_targets(session, count=1):
    """
    Return next 'count' fort targets.
    """
    current = fort_find_current_index(session)
    targets = []
    systems = fort_get_systems(session, not_othime=True)

    start = current + 1
    for system in systems[start:]:
        if system.is_fortified or system.skip:
            continue

        targets.append(system)
        count = count - 1

        if count == 0:
            break

    return targets


def fort_add_drop(session, **kwargs):
    """
    Add a Drop for 'amount' to the database where Drop intersects at:
        System.name and SUser.name
    If fort exists, increment its value. Else add it to database.

    Kwargs: system, user, amount

    Returns: The Drop object.
    """
    system = kwargs['system']
    user = kwargs['user']
    amount = kwargs['amount']

    try:
        drop = session.query(Drop).filter_by(user_id=user.id, system_id=system.id).one()
        drop.amount = drop.amount + amount
    except sqa_exc.NoResultFound:
        drop = Drop(user_id=user.id, system_id=system.id, amount=amount)
        session.add(drop)

    system.fort_status = system.fort_status + amount
    session.commit()

    return drop


class SheetScanner(object):
    """
    Scan a sheet to populate the database with information
    Also provide methods to update the sheet with new data

    Process whole sheets at a time with gsheet.whole_sheet()

    Important Note:
        Calls to modify the sheet should be asynchronous.
        Register them as futures and allow them to finish without waiting on.
    """
    def __init__(self, gsheet, user_args):
        self.gsheet = gsheet
        self.__cells = None
        self.user_col = None
        self.user_row = None
        self.users_args = user_args

    @property
    def cells(self):
        """
        Access a cached version of the cells.
        """
        if not self.__cells:
            self.__cells = self.gsheet.whole_sheet()

        return self.__cells

    def scan(self, session):
        """
        Main function, scan the sheet into the database.
        """
        systems = self.systems()
        users = self.users(*self.users_args)
        session.add_all(systems + users)
        session.commit()

        session.add_all(self.merits(systems, users))
        session.commit()

        self.__cells = None

    def users(self, cls, faction):
        """
        Scan the users in the sheet and return SUser objects.

        Args:
            cls: The subclass of SheetRow
            faction: The faction owning the sheet.
        """
        row = self.user_row - 1
        user_column = cog.sheets.column_to_index(self.user_col)
        cry_column = user_column - 1

        found = []
        for user in self.cells[user_column][row:]:
            row += 1

            if user == '':  # Users sometimes miss an entry
                continue

            try:
                cry = self.cells[cry_column][row - 1]
            except IndexError:
                cry = ''

            found.append(cls(name=user, faction=faction, row=row, cry=cry))

        return found

    def systems(self):
        """
        Scan and parse the system information.
        """
        raise NotImplementedError

    def merits(self, systems, users):
        """
        Scan and parse the merit information by system and user.
        """
        raise NotImplementedError

    async def update_sheet_user(self, user):
        """
        Update the user cry and name on the given row.
        """
        col1 = cog.sheets.Column(self.user_col).prev()
        cell_range = '!{col1}{row}:{col2}{row}'.format(row=user.row, col1=col1,
                                                       col2=self.user_col)
        self.gsheet.update(cell_range, [[user.cry, user.name]])


class FortScanner(SheetScanner):
    """
    Scanner for the Hudson fort sheet.
    """
    def __init__(self, gsheet):
        super(FortScanner, self).__init__(gsheet, (SheetCattle, EFaction.hudson))
        self.system_col = self.find_system_column()
        self.user_col, self.user_row = self.find_user_row()

    def systems(self):
        return self.fort_systems() + self.prep_systems()

    def fort_systems(self):
        """
        Scan and parse the system information into System objects.
        """
        found = []
        cell_column = cog.sheets.Column(self.system_col)
        first_system = cog.sheets.column_to_index(str(cell_column))
        order = 1

        try:
            for col in self.cells[first_system:]:
                kwargs = kwargs_fort_system(col, order, str(cell_column))
                found.append(System(**kwargs))
                order = order + 1
                cell_column.next()
        except cog.exc.SheetParsingError:
            pass

        return found

    def prep_systems(self):
        """
        Scan the Prep systems if any into the System db.

        Preps exist in range [D, system_col)
        """
        found = []
        cell_column = cog.sheets.Column('D')
        first_prep = cog.sheets.column_to_index(str(cell_column))
        first_system = cog.sheets.column_to_index(self.system_col)
        order = 1

        try:
            for col in self.cells[first_prep:first_system]:
                kwargs = kwargs_fort_system(col, order, str(cell_column))
                order = order + 1
                cell_column.next()

                if kwargs['name'] == 'TBA':
                    continue

                found.append(PrepSystem(**kwargs))
        except cog.exc.SheetParsingError:
            pass

        return found

    def merits(self, systems, users):
        """
        Scan the fortification area of the sheet and return Drop objects representing
        merits each user has dropped in System.

        Args:
            systems: The list of Systems in the order entered in the sheet.
            users: The list of Users in order the order entered in the sheet.
        """
        found = []
        col_offset = cog.sheets.column_to_index(systems[0].sheet_col) - 1

        for system in systems:
            try:
                for user in users:
                    col_ind = col_offset + system.sheet_order
                    amount = self.cells[col_ind][user.row - 1]

                    if amount == '':  # Some rows just placeholders if empty
                        continue

                    found.append(Drop(user_id=user.id, system_id=system.id,
                                      amount=amount))
            except IndexError:
                pass  # No more amounts in column

        return found

    def find_user_row(self):
        """
        Returns: First row and column that has users in it.

        Raises: SheetParsingError when fails to locate expected anchor in cells.
        """
        cell_anchor = 'CMDR Name'
        col_count = cog.sheets.Column('A')

        for column in self.cells:
            col_count.next()
            if cell_anchor not in column:
                continue

            col_count.prev()  # Gone past by one
            for row_count, row in enumerate(column):
                if row == cell_anchor:
                    return (str(col_count), row_count + 2)

        raise cog.exc.SheetParsingError

    def find_system_column(self):
        """
        Find the first column that has a system cell in it.
        Determined based on cell's background color.

        Raises: SheetParsingError when fails to locate expected anchor in cells.
        """
        column = cog.sheets.Column()
        # System's always use this background color.
        system_colors = {'red': 0.42745098, 'blue': 0.92156863, 'green': 0.61960787}

        fmt_cells = self.gsheet.get_with_formatting('!A10:J10')
        for val in fmt_cells['sheets'][0]['data'][0]['rowData'][0]['values']:
            if val['effectiveFormat']['backgroundColor'] == system_colors:
                return str(column)

            column.next()

        raise cog.exc.SheetParsingError

    async def update_drop(self, drop):
        """
        Update a drop to the sheet.
        """
        cell_range = '!{col}{row}:{col}{row}'.format(col=drop.system.sheet_col,
                                                     row=drop.user.row)
        self.gsheet.update(cell_range, [[drop.amount]])

    async def update_system(self, system):
        """
        Update the system column of the sheet.
        """
        cell_range = '!{col}{start}:{col}{end}'.format(col=system.sheet_col,
                                                       start=6, end=7)
        self.gsheet.update(cell_range, [[system.fort_status, system.um_status]], dim='COLUMNS')


class UMScanner(SheetScanner):
    """
    Scanner for the Hudson undermine sheet.
    """
    def __init__(self, gsheet):
        super(UMScanner, self).__init__(gsheet, (SheetUM, EFaction.hudson))
        # These are fixed based on current format
        self.system_col = 'D'
        self.user_col = 'B'
        self.user_row = 14

    def systems(self):
        """
        Scan all the systems in the sheet.
        A UM System takes up two adjacent columns.
        """
        cell_column = cog.sheets.Column(self.system_col)

        found = []
        try:
            while True:
                col = cog.sheets.column_to_index(str(cell_column))
                kwargs = kwargs_um_system(self.cells[col:col + 2], str(cell_column))
                cls = kwargs.pop('cls')
                found.append(cls(**kwargs))
                cell_column.offset(2)
        except cog.exc.SheetParsingError:
            pass

        return found

    def held_merits(self, systems, users, holds):
        """
        Parse the held merits that fall under the same column as System.

        Args:
            systems: The SystemUMs parsed from sheet.
            users: The SheetRows parsed from sheet.
            holds: The partially finished Holds.
        """
        for system in systems:
            col_ind = cog.sheets.column_to_index(system.sheet_col)

            try:
                for user in users:
                    held = self.cells[col_ind][user.row - 1]
                    if held == '':
                        continue

                    key = '{}_{}'.format(system.id, user.id)
                    hold = holds.get(key, Hold(user_id=user.id, system_id=system.id,
                                               held=0, redeemed=0))
                    hold.held += cogdb.schema.parse_int(held)
                    holds[key] = hold
            except IndexError:
                pass  # No more in column

        return holds

    def redeemed_merits(self, systems, users, holds):
        """
        Parse the redeemed merits that fall under the same column as System.

        Args:
            systems: The SystemUMs parsed from sheet.
            users: The SheetRows parsed from sheet.
            holds: The partially finished Holds.
        """
        for system in systems:
            col_ind = cog.sheets.column_to_index(system.sheet_col) + 1

            try:
                for user in users:
                    redeemed = self.cells[col_ind][user.row - 1]
                    if redeemed == '':
                        continue

                    key = '{}_{}'.format(system.id, user.id)
                    hold = holds.get(key, Hold(user_id=user.id, system_id=system.id,
                                               held=0, redeemed=0))
                    hold.redeemed += cogdb.schema.parse_int(redeemed)
                    holds[key] = hold
            except IndexError:
                pass  # No more in column

        return holds

    def merits(self, systems, users):
        """
        Scan the merits in the held/redeemed area.

        Args:
            systems: The list of Systems in the order entered in the sheet.
            users: The list of Users in order the order entered in the sheet.
        """
        holds = {}
        self.held_merits(systems, users, holds)
        self.redeemed_merits(systems, users, holds)
        return holds.values()

    # Calls to modify the sheet All asynchronous, register them as futures and move on.
    async def update_hold(self, hold):
        """
        Update a hold on the sheet.
        """
        col2 = cog.sheets.Column(hold.system.sheet_col).next()
        cell_range = '!{col1}{row1}:{col2}{row2}'.format(col1=hold.system.sheet_col, col2=col2,
                                                         row1=hold.user.row,
                                                         row2=hold.user.row + 1)
        self.gsheet.update(cell_range, [[hold.held, hold.redeemed]])

    async def update_system(self, system):
        """
        Update the system column of the sheet.
        """
        cell_range = '!{col}{start}:{col}{end}'.format(col=system.sheet_col,
                                                       start=10, end=13)
        self.gsheet.update(cell_range, [[system.progress_us, system.progress_them,
                                         'Hold Merits', system.map_offset]], dim='COLUMNS')


def um_find_system(session, system_name):
    """
    Find the SystemUM with system_name
    """
    try:
        return session.query(SystemUM).filter_by(name=system_name).one()
    except (sqa_exc.NoResultFound, sqa_exc.MultipleResultsFound):
        systems = session.query(SystemUM).all()
        return fuzzy_find(system_name, systems, 'name')


def um_get_systems(session, exclude_finished=True):
    """
    Return a list of all current undermining targets.

    kwargs:
        finished: Return just the finished targets.
    """
    systems = session.query(SystemUM).all()
    if exclude_finished:
        systems = [system for system in systems if not system.is_undermined]

    return systems


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
    """
    system = kwargs['system']
    user = kwargs['user']
    held = kwargs['held']

    try:
        hold = session.query(Hold).filter_by(user_id=user.id,
                                             system_id=system.id).one()
        hold.held = held
    except sqa_exc.NoResultFound:
        hold = Hold(user_id=user.id, system_id=system.id, held=held, redeemed=0)
        session.add(hold)
    session.commit()

    return hold
