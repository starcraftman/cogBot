"""
Module should handle logic related to querying/manipulating tables from a high level.
"""
from __future__ import absolute_import, print_function

import sqlalchemy.orm.exc as sqa_exc

import cog.exc
import cog.sheets
import cogdb
from cogdb.schema import Fort, SUser, System, system_result_dict


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


def get_all_users(session):
    """
    Return a list of all Users.
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

    return states


def get_sheet_user_by_name(session, sheet_name):
    """
    Return the User with User.sheet_name that matches.

    Raises:
        NoMatch - No possible match found.
        MoreThanOneMatch - Too many matches possible, ask user to resubmit.
    """
    try:
        return session.query(SUser).filter_by(sheet_name=sheet_name).one()
    except (sqa_exc.NoResultFound, sqa_exc.MultipleResultsFound):
        users = get_all_users(session)
        return fuzzy_find(sheet_name, users, 'sheet_name')


def get_system_by_name(session, system_name):
    """
    Return the System with System.name that matches.

    Raises:
        NoMatch - No possible match found.
        MoreThanOneMatch - Too many matches possible, ask user to resubmit.
    """
    try:
        return session.query(System).filter_by(name=system_name).one()
    except (sqa_exc.NoResultFound, sqa_exc.MultipleResultsFound):
        index = find_current_target(session)
        systems = get_all_systems(session)[index:]
        return fuzzy_find(system_name, systems, 'name')


def add_user(session, callback, sheet_name):
    """
    Simply add user past last user in sheet.
    """
    next_row = get_all_users(session)[-1].sheet_row + 1
    new_user = SUser(sheet_name=sheet_name, sheet_row=next_row)
    session.add(new_user)
    session.commit()

    callback(new_user)

    return new_user


def add_fort(session, callback, **kwargs):
    """
    Add a fort for 'amount' to the database where fort intersects at:
        System.name and User.sheet_name
    If fort exists, increment its value. Else add it to database.

    Kwargs: system, user, amount

    Returns: The Fort object.
    """
    system = kwargs['system']
    user = kwargs['user']
    amount = kwargs['amount']

    try:
        fort = session.query(Fort).filter_by(user_id=user.id, system_id=system.id).one()
        fort.amount = fort.amount + amount
    except sqa_exc.NoResultFound:
        fort = Fort(user_id=user.id, system_id=system.id, amount=amount)
        session.add(fort)
    system.fort_status = system.fort_status + amount
    system.cmdr_merits = system.cmdr_merits + amount
    session.commit()

    callback(fort)

    return fort


class SheetScanner(object):
    """
    Scan a sheet's cells for useful information.

    Whole sheets can be fetched by simply getting far beyond expected column end.
        i.e. sheet.get('!A:EA', dim='COLUMNS')
    """
    def __init__(self, cells, system_col='A', user_col='A', user_row=1):
        self.cells = cells
        self.system_col = system_col
        self.user_col = user_col
        self.user_row = user_row

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
        except cog.exc.IncorrectData:
            pass

        return found

    def users(self):
        """
        Scan the users in the fortification sheet and return User objects that can be inserted.

        Ensure Users and Systems have been flushed to link ids.
        """
        found = []
        row = self.user_row - 1
        user_column = cog.sheets.column_to_index(self.user_col)

        for user in self.cells[user_column][row:]:
            row += 1

            if user == '':  # Users sometimes miss an entry
                continue

            found.append(SUser(sheet_name=user, sheet_row=row))

        return found

    def forts(self, systems, users):
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
                for user in users:
                    col_ind = col_offset + system.sheet_order
                    amount = self.cells[col_ind][user.sheet_row - 1]

                    if amount == '':  # Some rows just placeholders if empty
                        continue

                    found.append(Fort(user_id=user.id, system_id=system.id, amount=amount))
            except IndexError:
                pass  # No more amounts in column

        return found


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
                raise cog.exc.NoMatch

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
        raise cog.exc.NoMatch
    else:
        raise cog.exc.MoreThanOneMatch(needle, matches)


def first_system_column(fmt_cells):
    """
    Find the first column that has a system cell in it.

    Determined based on cell's background color.
    """
    column = cog.sheets.Column()
    # System's always use this background color.
    system_colors = {'red': 0.42745098, 'blue': 0.92156863, 'green': 0.61960787}

    for val in fmt_cells['sheets'][0]['data'][0]['rowData'][0]['values']:
        if val['effectiveFormat']['backgroundColor'] == system_colors:
            return str(column)

        column.next()

    raise cog.exc.SheetParsingError


def first_user_row(cells):
    """
    cells: List of columns in sheet, each column is a list of rows.

    Returns: First row and column that has users in it.

    Raises: SheetParsingError when fails to locate expected anchor in cells.
    """
    cell_anchor = 'CMDR Name'
    col_count = cog.sheets.Column('A')

    for column in cells:
        col_count.next()
        if cell_anchor not in column:
            continue

        col_count.prev()  # Gone past by one
        for row_count, row in enumerate(column):
            if row == cell_anchor:
                return (str(col_count), row_count + 2)

    raise cog.exc.SheetParsingError


def dump_db():
    """
    Purely debug function, prints locally database.
    """
    session = cogdb.Session()
    print('Printing filled databases')
    for system in session.query(System):
        print(system)

    for user in session.query(SUser):
        print(user)

    for fort in session.query(Fort):
        print(fort)
