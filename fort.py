"""
Module should handle logic related to querying/manipulating tables from a high level.
"""
from __future__ import absolute_import, print_function

import cdb
import share
import sheets
import tbl


# TODO: Concern, too many sheet.gets. Consolidate to get whole sheet, and parse ?
# TODO: Similarly, when updating sheet rely on batch_update eventually taken from a queue.
# TODO: FortTable.drop(user, system, aount)
# TODO: FortTable.add_user(username)


class FortTable(object):
    """
    Represents the fort sheet, answers simple questions.
    """
    def __init__(self, session):
        """
        Query on creation any data needed.
        """
        self.othime = session.query(cdb.HSystem).\
                filter_by(name='Othime').one()
        self.systems = session.query(cdb.HSystem).\
                filter(cdb.HSystem.name != 'Othime').all()
        self.index = 0
        self.set_target()

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

        for system in self.systems[self.index+1:]:
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


class SheetScanner(object):
    def __init__(self, sheet, row_start=1, col_start=1):
        self.num_results = 15
        self.sheet = sheet
        self.row_start = row_start
        self.col_start = col_start

    def systems(self):
        """
        Scan the systems in the fortification sheet and return HSystem objects that can be inserted.
        """
        found = []
        data_column = sheets.Column(self.col_start)
        order = 1
        more_systems = True

        while more_systems:
            begin = str(data_column)
            end = data_column.offset(self.num_results)
            data_column.next()
            result = self.sheet.get('!{}1:{}10'.format(begin, end), dim='COLUMNS')

            try:
                result_column = sheets.Column(begin)
                for data in result:
                    kwargs = sheets.system_result_dict(data, order, str(result_column))
                    found.append(cdb.HSystem(**kwargs))

                    result_column.next()
                    order = order + 1
            except sheets.IncompleteData:
                more_systems = False

        return found

    def users(self):
        """
        Scan the users in the fortification sheet and return User objects that can be inserted.

        Ensure Users and HSystems have been flushed to link ids.
        """
        found = []
        row = self.row_start
        more_users = True

        while more_users:
            sname_row = row - 1
            data_range = '!B{}:B{}'.format(row, row + self.num_results)
            row = row + self.num_results + 1
            result = self.sheet.get(data_range, dim='COLUMNS')

            try:
                for sname in result[0]:
                    sname_row += 1
                    if sname == '':  # Users sometimes miss an entry
                        continue

                    found.append(cdb.User(sheet_name=sname, sheet_row=sname_row))
            except IndexError:
                more_users = False

        return found

    def forts(self, systems, users):
        """
        Scan the fortification area of the sheet and return Fort objects representing
        fortification of each system.

        Args:
            systems: The list of HSystems in the order entered in the sheet.
            users: The list of Users in order the order entered in the sheet.
        """
        found = []
        data_range = '!{}{}:{}{}'.format(self.col_start, self.row_start,
                                         systems[-1].sheet_col,
                                         users[-1].sheet_row)
        result = self.sheet.get(data_range, dim='COLUMNS')
        system_ind = -1
        for col_data in result:
            system_ind += 1
            system = systems[system_ind]

            user_ind = -1
            for amount in col_data:
                user_ind += 1

                if amount == '':  # Some rows just placeholders if empty
                    continue

                user = users[user_ind]
                found.append(cdb.Fort(user_id=user.id, system_id=system.id, amount=amount))

        return found


def main():
    """
    Main function, does simple fort table test.
    """
    table = FortTable(share.get_db_session())
    print(table.targets())
    print(table.next_targets())

    # Drop tables easily
    # session.query(cdb.Fort).delete()
    # session.query(cdb.User).delete()
    # session.query(cdb.HSystem).delete()
    # session.commit()

    # print('Printing filled databases')
    # for system in systems:
        # print(system)

    # for user in users:
        # print(user)

    # for fort in forts:
        # print(fort)

if __name__ == "__main__":
    main()
