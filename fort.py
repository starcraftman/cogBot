"""
Module should handle logic related to querying/manipulating tables from a high level.
"""
from __future__ import absolute_import, print_function

import share
import sheets
import cdb


# TODO: Don't iterate Systems every query
# FIXME: FortTable broken, still porting to new orm objects.
# class FortTable(object):
    # """
    # Represents the fort sheet
        # -> Goal: Minimize unecessary operations to data on server.
    # """
    # def __init__(self, data):
        # """
        # Parse data from table and initialize.
        # """
        # self.data = data

    # def current(self):
        # """
        # Print out the current system to forify.
        # """
        # target = None

        # # Seek targets in systems list
        # for datum in self.data:
            # system = FortSystem(datum)

            # if not target and system.name != 'Othime' and \
                    # not system.is_fortified and not system.skip:
                # target = system

            # if target:
                # break

        # return target.name

    # def current_long(self):
        # """
        # Print out the current objectives to fortify and their status.
        # """
        # othime = None
        # target = None

        # # Seek targets in systems list
        # for datum in self.data:
            # system = FortSystem(datum)

            # if system.name == 'Othime':
                # othime = system

            # if not target and system.name != 'Othime' and \
                    # not system.is_fortified and not system.skip:
                # target = system

            # if othime and target:
                # break

        # lines = [HSystem.header, target.data_tuple]
        # if not othime.is_fortified:
            # lines += [othime.data_tuple]
        # return tbl.format_table(lines, sep='|', header=True)

    # def next_systems(self, num=None):
        # """
        # Return next 5 regular fort targets.
        # """
        # targets = []
        # if not num:
            # num = 5

        # for datum in self.data:
            # system = FortSystem(datum)

            # if system.name != 'Othime' and not system.is_fortified and not system.skip:
                # targets.append(system.name)

            # if len(targets) == num + 1:
                # break

        # return '\n'.join(targets[1:])

    # def next_systems_long(self, num=None):
        # """
        # Return next 5 regular fort targets.
        # """
        # targets = []
        # if not num:
            # num = 5

        # for datum in self.data:
            # system = FortSystem(datum)

            # if system.name != 'Othime' and not system.is_fortified and not system.skip:
                # targets.append(system.data_tuple)

            # if len(targets) == num + 1:
                # break

        # return tbl.format_table([HSystem.header] + targets[1:], sep='|', header=True)

    # def totals(self):
        # """
        # Print running total of fortified, undermined systems.
        # """
        # undermined = 0
        # fortified = 0

        # for datum in self.data:
            # system = FortSystem(datum)

            # if system.is_fortified:
                # fortified += 1
            # if system.is_undermined:
                # undermined += 1

        # return 'Fortified {}/{tot}, Undermined: {}/{tot}'.format(fortified, undermined,
                                                                 # tot=len(self.data))



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

    range_fetch = 10
    data_column = sheets.Column('F')
    more_systems = True
    while more_systems:
        begin, end = str(data_column), data_column.offset(range_fetch)
        data_column.next()
        data_range = '!{}1:{}10'.format(begin, end)
        result = sheet.get(data_range, dim='COLUMNS')

        try:
            result_column = sheets.Column(begin)
            for ind, data in enumerate(result):
                kwargs = sheets.system_result_dict(data, ind, str(result_column))
                system = cdb.HSystem(**kwargs)
                session.add(system)
                result_column.next()
        except sheets.IncompleteData:
            more_systems = False

    session.commit()

    more_users = True
    row = 11
    while more_users:
        begin = row
        end = row + range_fetch
        row += range_fetch + 1

        data_range = '!B{}:B{}'.format(begin, end)
        result = sheet.get(data_range, dim='COLUMNS')

        try:
            data_row = begin
            for uname in result[0]:
                user = cdb.User(discord_name=uname, sheet_name=uname, sheet_row=data_row)
                session.add(user)
                data_row += 1

            if len(result[0]) != range_fetch + 1:
                more_users = False
        except IndexError:
            more_users = False

    session.commit()

    users = session.query(cdb.User).all()
    systems = session.query(cdb.HSystem).all()

    # Proccess all forts
    data_range = '!{}{}:{}{}'.format('F', 11, systems[-1].sheet_col, users[-1].sheet_row)
    result = sheet.get(data_range, dim='COLUMNS')
    system_ind = 0
    for col_data in result:
        system = systems[system_ind]
        system_ind += 1

        user_ind = 0
        for row_data in col_data:
            if row_data == '':
                continue

            user = users[user_ind]
            user_ind += 1

            fort = cdb.Fort(user_id=user.id, system_id=system.id, amount=row_data)
            session.add(fort)

    session.commit()

    print('Printing filled database, first 10 elements.')
    for system in session.query(cdb.HSystem)[:10]:
        print(system)

    for user in session.query(cdb.User)[:10]:
        print(user)

    for fort in session.query(cdb.Fort)[:10]:
        print(fort)

    # table = FortTable(result)
    # print(table.current())
    # print(table.next_systems_long())

if __name__ == "__main__":
    main()
