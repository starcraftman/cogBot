"""
Module should handle logic related to querying/manipulating tables from a high level.
"""
from __future__ import absolute_import, print_function

import sqlalchemy.orm.exc as sqa_exc

import cogdb
from cogdb.schema import Fort, System, User
import cog.exc
import cog.sheets


# TODO: Similarly, when updating sheet rely on batch_update eventually taken from a queue.
# TODO: ForTable -> make functional and pass data or reorganize, doesn't work for me.


class FortTable(object):
    """
    Represents the fort sheet, answers simple questions.
    """
    def __init__(self, sheet):
        """
        Query on creation any data needed.
        """
        self.index = 0
        self.sheet = sheet
        self.session = cogdb.Session()
        self.set_target()

    @property
    def othime(self):
        return self.session.query(System).filter_by(name='Othime').one()

    @property
    def systems(self):
        return self.session.query(System).filter(System.name != 'Othime').all()

    @property
    def users(self):
        return self.session.query(User).all()

    def set_target(self):
        """
        Scan list from the beginning to find next unfortified target.
        """
        for ind, system in enumerate(self.systems):
            if system.is_fortified or system.skip:
                continue

            self.index = ind
            break

    def targets(self):
        """
        Print out the current system to forify.
        """
        targets = [self.systems[self.index]]
        if not self.othime.is_fortified:
            targets.append(self.othime)

        return targets

    def next_targets(self, count=5):
        """
        Return next 5 regular fort targets.
        """
        targets = []

        start = self.index + 1
        for system in self.systems[start:]:
            if system.is_fortified or system.skip:
                continue

            targets.append(system)

            count = count - 1
            if count == 0:
                break

        return targets

    def totals(self):
        """
        Print running total of fortified, undermined systems.
        """
        undermined = 0
        fortified = 0

        for system in self.systems + [self.othime]:
            if system.is_fortified:
                fortified += 1
            if system.is_undermined:
                undermined += 1

        return 'Fortified {}/{tot}, Undermined: {}/{tot}'.format(fortified, undermined,
                                                                 tot=len(self.systems) + 1)

    def find_user(self, name):
        """
        Find and return matching User, if not found returns None.
        """
        try:
            return self.session.query(User).filter_by(sheet_name=name).one()
        except (sqa_exc.NoResultFound, sqa_exc.MultipleResultsFound):
            return None

    def add_user(self, name):
        """
        Simply add user past last entry.
        """
        next_row = self.users[-1].sheet_row + 1
        new_user = User(sheet_name=name, sheet_row=next_row)
        self.session.add(new_user)
        self.session.commit()

        # For now, update immediately and wait.
        self.sheet.update('!B{row}:B{row}'.format(row=new_user.sheet_row), [[new_user.sheet_name]])

        return new_user

    def add_fort(self, system_name, sheet_name, amount):
        try:
            system = self.session.query(System).filter_by(name=system_name).one()
            user = self.session.query(User).filter_by(sheet_name=sheet_name).one()
        except (sqa_exc.NoResultFound, sqa_exc.MultipleResultsFound):
            raise cog.exc.InvalidCommandArgs('Invalid drop command. User or system invalid.')

        try:
            fort = self.session.query(Fort).filter_by(user_id=user.id, system_id=system.id).one()
            fort.amount = fort.amount + amount
        except sqa_exc.NoResultFound:
            fort = Fort(user_id=user.id, system_id=system.id, amount=amount)
            self.session.add(fort)
        system.fort_status = system.fort_status + amount
        system.cmdr_merits = system.cmdr_merits + amount

        self.session.commit()

        range1 = '!{col}{row}:{col}{row}'.format(col=system.sheet_col,
                                                 row=user.sheet_row)
        range2 = '!{col}{row}:{col}{row}'.format(col=system.sheet_col,
                                                 row=6)
        self.sheet.update(range1, [[fort.amount]])
        self.sheet.update(range2, [[max(system.fort_status, system.cmdr_merits)]])

        return system


class SheetScanner(object):
    """
    Scan a sheet's cells for useful information.

    Whole sheets can be fetched by simply getting far beyond expected column end.
        i.e. sheet.get('!A:EA', dim='COLUMNS')
    """
    def __init__(self, cells, col_start='A', row_start=1):
        self.cells = cells
        self.col_start = col_start
        self.row_start = row_start

    def systems(self):
        """
        Scan the systems in the fortification sheet and return System objects that can be inserted.
        """
        found = []
        cell_column = cog.sheets.Column(self.col_start)
        start_index = cog.sheets.column_to_index(self.col_start)
        order = 1

        try:
            for col in self.cells[start_index:]:
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
        row = self.row_start - 1

        for user in self.cells[1][row:]:
            row += 1

            if user == '':  # Users sometimes miss an entry
                continue

            found.append(User(sheet_name=user, sheet_row=row))

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
            break

        column.next()

    return str(column)


def init_db():
    """
    Scan sheet and fill database if empty.
    """
    session = cogdb.Session()

    if not session.query(cogdb.schema.System).all():
        sheet_id = cog.share.get_config('hudson', 'cattle', 'id')
        secrets = cog.share.get_config('secrets', 'sheets')
        sheet = cog.sheets.GSheet(sheet_id, cog.share.rel_to_abs(secrets['json']),
                                  cog.share.rel_to_abs(secrets['token']))

        col_start = first_system_column(sheet.get_with_formatting('!A10:J10'))
        scanner = SheetScanner(sheet.get('!A:EA', dim='COLUMNS'), col_start, 11)
        systems = scanner.systems()
        users = scanner.users()
        session.add_all(systems + users)
        session.commit()

        forts = scanner.forts(systems, users)
        session.add_all(forts)
        session.commit()


def dump_db():
    """
    Purely debug function, prints locally database.
    """
    session = cogdb.Session()
    print('Printing filled databases')
    for system in session.query(System):
        print(system)

    for user in session.query(User):
        print(user)

    for fort in session.query(Fort):
        print(fort)


def main():
    """
    Main function, does simple fort table test.
    """
    sheet_id = cog.share.get_config('hudson', 'cattle', 'id')
    secrets = cog.share.get_config('secrets', 'sheets')
    sheet = cog.sheets.GSheet(sheet_id, cog.share.rel_to_abs(secrets['json']),
                              cog.share.rel_to_abs(secrets['token']))

    col_start = first_system_column(sheet.get_with_formatting('!A10:J10'))
    scanner = SheetScanner(sheet.get('!A:EA', dim='COLUMNS'), col_start, 11)
    print(scanner.col_start)
    print(scanner.systems()[0].name)

    # print(json.dumps(values, indent=4, sort_keys=True))
    # init_db()
    # table = FortTable(sheet)
    # print(table.targets())
    # print(table.next_targets())

    # Drop tables easily
    # session.query(Fort).delete()
    # session.query(User).delete()
    # session.query(System).delete()
    # session.commit()

    # session = cogdb.Session()
    # print('Printing filled databases')
    # for system in session.query(System):
        # print(system)

    # for user in session.query(User):
        # print(user)

    # for fort in session.query(Fort):
        # print(fort)


if __name__ == "__main__":
    main()
