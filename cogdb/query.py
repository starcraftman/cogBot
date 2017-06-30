"""
Module should handle logic related to querying/manipulating tables from a high level.
"""
from __future__ import absolute_import, print_function

import sqlalchemy.orm.exc as sqa_exc

import cog.exc
import cog.sheets
import cogdb
from cogdb.schema import Fort, DUser, SUser, System, system_result_dict


def get_othime(session):
    """
    Return the System Othime.
    """
    return session.query(System).filter_by(name='Othime').one()


def get_systems_not_othime(session):
    """
    Return a list of all Systems except Othime.
    """
    return session.query(System).filter(System.name != 'Othime').all()


def get_all_systems(session):
    """
    Return a list of all Systems.
    """
    return session.query(System).all()


def get_all_sheet_users(session):
    """
    Return a list of all SUsers.
    """
    return session.query(SUser).all()


def find_current_target(session):
    """
    Scan Systems from the beginning to find next unfortified target that is not Othime.
    """
    for ind, system in enumerate(get_systems_not_othime(session)):
        if system.is_fortified or system.skip:
            continue

        return ind


def get_fort_targets(session, current):
    """
    Returns a list of Systems that should be fortified.
    First System is not Othime and is unfortified.
    Second System if prsent is Othime, only when not fortified.
    """
    systems = get_systems_not_othime(session)
    othime = get_othime(session)

    targets = [systems[current]]
    if not othime.is_fortified:
        targets.append(othime)

    return targets


def get_next_fort_targets(session, current, count=5):
    """
    Return next 'count' fort targets.
    """
    targets = []
    systems = get_systems_not_othime(session)

    start = current + 1
    for system in systems[start:]:
        if system.is_fortified or system.skip:
            continue

        targets.append(system)
        count = count - 1

        if count == 0:
            break

    return targets


def get_all_systems_by_state(session):
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

    for system in get_all_systems(session):
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


def get_discord_user_by_id(session, did):
    """
    Return the DUser that has the same discord_id.

    Raises:
        NoMatch - No possible match found.
    """
    try:
        return session.query(DUser).filter_by(discord_id=did).one()
    except sqa_exc.NoResultFound:
        raise cog.exc.NoMatch(did, 'DUser')


def get_sheet_user_by_name(session, name):
    """
    Return the SUser with SUser.name that matches.

    Raises:
        NoMatch - No possible match found.
        MoreThanOneMatch - Too many matches possible, ask user to resubmit.
    """
    try:
        return session.query(SUser).filter_by(name=name).one()
    except (sqa_exc.NoResultFound, sqa_exc.MultipleResultsFound):
        users = get_all_sheet_users(session)
        return fuzzy_find(name, users, 'name')


def get_system_by_name(session, system_name, search_all=False):
    """
    Return the System with System.name that matches.

    Raises:
        NoMatch - No possible match found.
        MoreThanOneMatch - Too many matches possible, ask user to resubmit.
    """
    try:
        return session.query(System).filter_by(name=system_name).one()
    except (sqa_exc.NoResultFound, sqa_exc.MultipleResultsFound):
        index = 0 if search_all else find_current_target(session)

        systems = get_all_systems(session)[index:]
        return fuzzy_find(system_name, systems, 'name')


def check_sheet_user(session, duser, create_hook=None):
    """
    Try to find a user's name in sheet.
    If not create and add user to the sheet with hook.

    Returns: The SUser
    """
    try:
        suser = get_sheet_user_by_name(session, duser.pref_name)
    except cog.exc.NoMatch:
        suser = add_sheet_user(session, name=duser.pref_name)
        session.commit()

        if create_hook:
            create_hook(suser)

    duser.set_cattle(suser)
    session.commit()

    return suser


def check_discord_user(session, member):
    """
    Ensure a member has an entry in the dusers table.

    Returns: The DUser
    """
    try:
        duser = get_discord_user_by_id(session, member.id)
    except cog.exc.NoMatch:
        duser = add_discord_user(session, member)
        session.commit()

    return duser


def add_discord_user(session, member, capacity=0, pref_name=None):
    """
    Add a discord user to the database.
    """
    if not pref_name:
        pref_name = member.display_name
    new_duser = DUser(discord_id=member.id, display_name=member.display_name,
                      capacity=capacity, pref_name=pref_name)
    session.add(new_duser)
    session.commit()

    return new_duser


def add_sheet_user(session, name):
    """
    Simply add user past last user in sheet.
    """
    next_row = get_all_sheet_users(session)[-1].row + 1
    new_user = SUser(name=name, row=next_row)
    session.add(new_user)
    session.commit()

    return new_user


def add_fort(session, **kwargs):
    """
    Add a fort for 'amount' to the database where fort intersects at:
        System.name and SUser.name
    If fort exists, increment its value. Else add it to database.

    Kwargs: system, user, amount

    Returns: The Fort object.
    """
    system = kwargs['system']
    suser = kwargs['suser']
    amount = kwargs['amount']

    try:
        fort = session.query(Fort).filter_by(user_id=suser.id,
                                             system_id=system.id).one()
        fort.amount = fort.amount + amount
    except sqa_exc.NoResultFound:
        fort = Fort(user_id=suser.id, system_id=system.id, amount=amount)
        session.add(fort)
    system.fort_status = system.fort_status + amount
    system.cmdr_merits = system.cmdr_merits + amount
    session.commit()

    return fort


class SheetScanner(object):
    """
    Scan a sheet's cells for useful information.

    Whole sheets can be fetched by simply getting far beyond expected column end.
        i.e. sheet.get('!A:EA', dim='COLUMNS')
    """
    def __init__(self, gsheet):
        self.gsheet = gsheet
        self.__cells = None
        self.system_col = self.find_system_column()
        self.user_col, self.user_row = self.find_user_row()

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
        Update db with scanned information from sheet.
        """
        systems = self.systems()
        users = self.users()
        session.add_all(systems + users)
        session.commit()

        session.add_all(self.forts(systems, users))
        session.commit()

    def systems(self):
        """
        Scan the systems in the fortification sheet and return System objects that can be inserted.
        """
        found = []
        cell_column = cog.sheets.Column(self.system_col)
        first_system_col = cog.sheets.column_to_index(self.system_col)
        order = 1

        try:
            for col in self.cells[first_system_col:]:
                kwargs = system_result_dict(col, order, str(cell_column))
                found.append(System(**kwargs))
                order = order + 1
                cell_column.next()
        except cog.exc.SheetParsingError:
            pass

        return found

    def users(self):
        """
        Scan the users in the fortification sheet and return User objects that can be inserted.
        """
        found = []
        row = self.user_row - 1
        user_column = cog.sheets.column_to_index(self.user_col)
        cry_column = user_column - 1

        for user in self.cells[user_column][row:]:
            row += 1

            if user == '':  # Users sometimes miss an entry
                continue

            try:
                cry = self.cells[cry_column][row - 1]
            except IndexError:
                cry = ''

            found.append(SUser(name=user, row=row, cry=cry))

        return found

    def forts(self, systems, susers):
        """
        Scan the fortification area of the sheet and return Fort objects representing
        fortification of each system.

        Args:
            systems: The list of Systems in the order entered in the sheet.
            users: The list of Users in order the order entered in the sheet.
        """
        found = []
        col_offset = cog.sheets.column_to_index(systems[0].sheet_col) - 1

        for system in systems:
            try:
                for suser in susers:
                    col_ind = col_offset + system.sheet_order
                    amount = self.cells[col_ind][suser.row - 1]

                    if amount == '':  # Some rows just placeholders if empty
                        continue

                    found.append(Fort(user_id=suser.id, system_id=system.id,
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

    # Calls to modify the sheet All asynchronous, register them as futures and move on.
    async def update_fort(self, fort):
        """
        Add a fort to the sheet.
        """
        cell_range = '!{col}{row}:{col}{row}'.format(col=fort.system.sheet_col,
                                                     row=fort.suser.row)
        self.gsheet.update(cell_range, [[fort.amount]])

    async def update_system(self, system, max_fort=True):
        """
        Update the system column of the sheet.
        """
        cell_range = '!{col}{start}:{col}{end}'.format(col=system.sheet_col,
                                                       start=6, end=9)
        fort_status = system.current_status if max_fort else system.fort_status
        self.gsheet.update(cell_range, [[fort_status, system.um_status,
                                         system.distance, system.notes]], dim='COLUMNS')

    async def update_sheet_user(self, suser):
        """
        Update the user cry and name on the given row.
        """
        col1 = cog.sheets.Column(self.user_col).prev()
        cell_range = '!{col1}{row}:{col2}{row}'.format(row=suser.row, col1=col1,
                                                       col2=self.user_col)
        self.gsheet.update(cell_range, [[suser.cry, suser.name]])


class UMScanner(object):
    """
    Scanner for the undermine sheet.
    """
    def __init__(self, gsheet):
        self.gsheet = gsheet
        self.__cells = None
        self.user_col = 'C'
        self.user_row = 14

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
        Update db with scanned information from sheet.
        """
        systems = self.columns()
        users = self.rows()
        session.add_all(systems + users)
        session.commit()

        session.add_all(self.joined_data(systems, users))
        session.commit()

    def columns(self):
        """
        Scan the system column information.
        """
        pass

    def rows(self):
        """
        Scan the users in the sheet and return SUser objects.
        """
        found = []
        row = self.user_row - 1
        user_column = cog.sheets.column_to_index(self.user_col)
        cry_column = user_column - 1

        for user in self.cells[user_column][row:]:
            row += 1

            if user == '':  # Users sometimes miss an entry
                continue

            try:
                cry = self.cells[cry_column][row - 1]
            except IndexError:
                cry = ''

            found.append(SUser(name=user, row=row, cry=cry))

        return found

    def joined_data(self, rows, columns):
        pass


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


def fuzzy_find(needle, stack, obj_attr='zzzz', ignore_case=True):
    """
    Searches for needle in whole stack and gathers matches. Returns match if only 1.

    Raise separate exceptions for NoMatch and MoreThanOneMatch.
    """
    matches = []
    for obj in stack:
        try:
            if subseq_match(needle, getattr(obj, obj_attr, obj), ignore_case):
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


def dump_db():
    """
    Purely debug function, prints locally database.
    """
    session = cogdb.Session()
    print('Printing filled databases')
    classes = [cogdb.schema.Command, cogdb.schema.DUser,
               cogdb.schema.SUser, cogdb.schema.Fort, cogdb.schema.System]
    for cls in classes:
        print('---- ' + str(cls) + ' ----')
        for obj in session.query(cls):
            print(obj)
