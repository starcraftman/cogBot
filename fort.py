"""
Module should handle logic related to querying/manipulating tables from a high level.
"""
from __future__ import absolute_import, print_function

import cdb
import share
import sheets
import tbl


class FortTable(object):
    """
    Represents the fort sheet
        -> Goal: Minimize unecessary operations to data on server.
    # """
    def __init__(self, othime, systems, users, forts):
        """
        Pass in systems and users objects parsed from table.

        NB: Remove othime from systems and pass in separately.
        """
        self.index = 0
        self.othime = othime
        self.forts = forts
        self.systems = systems
        self.users = users

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

    def targets_long(self):
        """
        Print out the current objectives to fortify and their status.
        """
        lines = [cdb.HSystem.header, self.systems[self.index].data_tuple]
        if not self.othime.is_fortified:
            lines += [self.othime.data_tuple]

        return tbl.format_table(lines, sep='|', header=True)

    def next_systems(self, count=5):
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

    def next_systems_long(self, num=5):
        """
        Return next 5 regular fort targets.
        """
        tuples = [system.data_tuple for system in self.next_systems(num)]
        return tbl.format_table([cdb.HSystem.header] + tuples,
                                sep='|', header=True)

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


class InitialScan(object):
    def __init__(self, start_row, start_col, sheet):
        self.fetch_amount = 15
        self.sheet = sheet
        self.start_row = start_row
        self.start_col = start_col

    def systems(self):
        """
        Scan for systems in the fort table and insert into database.
        """
        found = []
        data_column = sheets.Column(self.start_col)
        more_systems = True

        while more_systems:
            begin = str(data_column)
            end = data_column.offset(self.fetch_amount)
            data_column.next()
            result = self.sheet.get('!{}1:{}10'.format(begin, end), dim='COLUMNS')

            try:
                result_column = sheets.Column(begin)
                for ind, data in enumerate(result):
                    kwargs = sheets.system_result_dict(data, ind, str(result_column))
                    found.append(cdb.HSystem(**kwargs))
                    result_column.next()
            except sheets.IncompleteData:
                more_systems = False

        return found

    def users(self):
        found = []
        row = self.start_row
        more_users = True

        while more_users:
            begin = row
            data_range = '!B{}:B{}'.format(begin, begin + self.fetch_amount)
            row = row + self.fetch_amount + 1
            result = self.sheet.get(data_range, dim='COLUMNS')

            try:
                for uname in result[0]:
                    found.append(cdb.User(sheet_name=uname, sheet_row=begin))
                    begin += 1

                if len(result[0]) != self.fetch_amount + 1:
                    more_users = False
            except IndexError:
                more_users = False

        return found


    def forts(self, systems, users):
        """
        Given the existing systems and users, parse forts.
        """
        found = []
        data_range = '!{}{}:{}{}'.format(self.start_col, self.start_row,
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
    import sqlalchemy as sqa
    import sqlalchemy.orm as sqa_orm
    engine = sqa.create_engine('sqlite:///:memory:', echo=False)
    cdb.Base.metadata.create_all(engine)
    session = sqa_orm.sessionmaker(bind=engine)()

    sheet_id = share.get_config('hudson', 'cattle', 'id')
    secrets = share.get_config('secrets', 'sheets')
    sheet = sheets.GSheet(sheet_id, secrets['json'], secrets['token'])

    scanner = InitialScan(11, 'F', sheet)
    systems = scanner.systems()
    session.add_all(systems)
    session.commit()

    users = scanner.users()
    session.add_all(users)
    session.commit()

    forts = scanner.forts(systems, users)
    session.add_all(forts)
    session.commit()

    # print('Printing filled database, first 10 elements.')
    # for system in systems[0:10]:
        # print(system)

    # for user in users[0:10]:
        # print(user)

    # for fort in forts[0:10]:
        # print(fort)
    othime = session.query(cdb.HSystem).filter_by(name='Othime').one()
    not_othime = session.query(cdb.HSystem).filter(cdb.HSystem.name != 'Othime').all()

    table = FortTable(othime, not_othime, users, forts)
    table.set_target()
    print(table.targets())
    print(table.targets_long())
    print(table.next_systems_long())

if __name__ == "__main__":
    main()
