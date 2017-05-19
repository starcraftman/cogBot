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
    import pprint
    engine = sqa.create_engine('sqlite:///:memory:', echo=False)
    cdb.Base.metadata.create_all(engine)
    Session = sqa_orm.sessionmaker(bind=engine)

    sheet_id = share.get_config('hudson', 'cattle', 'id')
    secrets = share.get_config('secrets', 'sheets')
    sheet = sheets.GSheet(sheet_id, secrets['json'], secrets['token'])
    result = sheet.get('!F1:BJ10', dim='COLUMNS')

    offset = sheets.col_to_int('F')
    for ind, data in enumerate(result):
        print(cdb.HSystem(**sheets.system_result_dict(data, ind, offset)).__repr__())

    # table = FortTable(result)
    # print(table.current())
    # print(table.next_systems_long())

if __name__ == "__main__":
    main()
