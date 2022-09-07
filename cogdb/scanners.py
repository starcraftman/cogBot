"""
All sheet scanners are stored here for now

Sheet scanners make heavy use of cog.sheets.AsyncGSheet
"""
import asyncio
import concurrent.futures as cfut
import datetime
import logging
import re
import sys
from copy import deepcopy

import sqlalchemy.exc as sqla_exc

import cog.exc
import cog.sheets
import cog.util
import cogdb
from cogdb.schema import (FortSystem, FortPrep, FortDrop, FortUser,
                          UMSystem, UMUser, UMHold, KOS, EUMSheet, Consolidation,
                          kwargs_fort_system, kwargs_um_system)


SNIPE_FIRST_ID = 10001
SCANNERS = {}


class FortScanner():
    """
    Scanner for the Hudson fort sheet.

    args:
        asheet: The AsyncGSheet that connects to the sheet.
        user_args: The arguements to use for users parsing.
        db_classes: The database classes that should be purged on replacement.
    """
    def __init__(self, asheet, db_classes=None):
        self.asheet = asheet
        self.db_classes = db_classes if db_classes else [FortDrop, FortSystem, FortUser]
        self.lock = cog.util.RWLockWrite()

        self.cells_row_major = None
        self.__cells_col_major = None
        self.system_col = None
        self.user_col = 'B'
        self.user_row = 11

    def __repr__(self):
        keys = ['asheet', 'db_classes', 'lock',
                'system_col', 'user_col', 'user_row']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "FortScanner({})".format(', '.join(kwargs))

    def __str__(self):
        return repr(self)

    def __getstate__(self):  # pragma: no cover
        """ Do not pickle asheet or lock. """
        state = self.__dict__.copy()
        state['asheet'] = None
        state['lock'] = None

        return state

    def __setstate__(self, state):  # pragma: no cover
        """ Return from pickling, stub asheet. """
        state['asheet'] = None
        state['lock'] = cog.util.RWLockWrite()
        self.__dict__.update(state)

    @property
    def cells_col_major(self):
        """
        Provide a view of cells with column as major dimension.
        Transpose is carried out on first request post update and cached until next update.
        """
        if not self.__cells_col_major:
            self.__cells_col_major = cog.util.transpose_table(self.cells_row_major)

        return self.__cells_col_major

    async def update_cells(self):
        """ Fetch all cells from the sheet. """
        self.cells_row_major = await self.asheet.whole_sheet()
        self.__cells_col_major = None

    def scheduler_run(self):
        """
        Use this when scheduler needs to call parse_sheet.
        Will inject a fresh connection.
        """
        with cogdb.session_scope(cogdb.Session) as session:
            self.parse_sheet(session)

    def parse_sheet(self, session):
        """
        Parse the updated sheet and return information to directly pass to scan.

        Returns:
            [systems, users, drops]
        """
        self.update_system_column()
        systems = self.fort_systems() + self.prep_systems()
        users = self.users()
        drops = self.drops(systems, users)

        self.flush_to_db(session, (users, systems, drops))

    def drop_db_entries(self, session):
        """
        Drop the objects in the database that this scanner is responsible for.
        """
        for cls in self.db_classes:
            try:
                session.query(cls).delete()
            except sqla_exc.ProgrammingError:  # Table was deleted or some other problem, attempt to recreate
                logging.getLogger(__name__).error("Drop DB Entries: Critical error, likely a table issue. Attempting to recreate if it was deleted.")
                cls.__table__.create(cogdb.engine)
        session.commit()

    def flush_to_db(self, session, new_objs):
        """
        Flush the parsed values directly into the database.
        Old values will be dropped first as no guarantee same objects.

        Args:
            session: A valid session for db.
            new_objs: A list of list of db objects to put in database.
        """
        self.drop_db_entries(session)

        for objs in new_objs:
            session.add_all(objs)
            session.flush()
        session.commit()

    def users(self, *, row_cnt=None, first_id=1, cls=FortUser):
        """
        Scan the users in the sheet and return sheet user objects.

        Args:
            row_cnt: The starting row for users, zero indexed. Default is user_row.
            first_id: The id to start for the users.
            cls: The class to create for each user found.
        """
        found = []

        if not row_cnt:
            row_cnt = self.user_row - 1

        users = [x[row_cnt:] for x in self.cells_col_major[:2]]
        for cry, name in list(zip(*users)):
            row_cnt += 1
            if name.strip() == '':
                continue

            sheet_user = cls(id=first_id, cry=cry, name=name, row=row_cnt)
            first_id += 1
            if sheet_user in found:
                rows = [other.row for other in found if other == sheet_user] + [row_cnt]
                raise cog.exc.NameCollisionError(cls.__name__, sheet_user.name, rows)

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

        try:
            for col in self.cells_col_major[ind:]:
                kwargs = kwargs_fort_system(col[0:10], order, str(cell_column))
                kwargs['id'] = order
                found += [FortSystem(**kwargs)]

                order += 1
                cell_column.fwd()
        except cog.exc.SheetParsingError:
            pass

        return found

    def prep_systems(self):
        """
        Scan the Prep systems if any into the System db.

        Preps exist in range [D, system_col)
        """
        found, order, cell_column = [], 0, cog.sheets.Column('C')
        first_prep = cog.sheets.column_to_index(str(cell_column))
        first_system = cog.sheets.column_to_index(self.system_col) - 1

        try:
            for col in self.cells_col_major[first_prep:first_system]:
                order = order + 1
                cell_column.fwd()
                col = col[0:10]

                if col[-1].strip() == "TBA":
                    continue

                kwargs = kwargs_fort_system(col, order, str(cell_column))
                kwargs['id'] = 1000 + order

                found += [FortPrep(**kwargs)]
        except cog.exc.SheetParsingError:
            pass

        return found

    def drops(self, systems, users):
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
            merit_cells = self.cells_col_major[ind][10:]

            for user in users:
                try:
                    amount = merit_cells.pop(0).strip()
                    amount = cogdb.schema.parse_int(amount)
                    if not amount:
                        continue

                    found += [(FortDrop(id=cnt, user_id=user.id, system_id=system.id,
                               amount=amount))]
                    cnt += 1

                except ValueError:
                    pass

        return found

    def update_system_column(self):
        """
        Find the first column that has a system cell in it.
        Determined based on TBA columns.

        Returns:
            The A1 format string of the system column.

        Raises:
            SheetParsingError when fails to locate the system column.
        """
        row = self.cells_row_major[9]
        col_count = cog.sheets.Column()

        next_not_tba = False
        for cell in row:
            if next_not_tba and cell.strip() != 'TBA':
                self.system_col = str(col_count)
                return self.system_col

            if cell == 'TBA':
                next_not_tba = True

            col_count.fwd()

        raise cog.exc.SheetParsingError("Unable to determine system column.")

    async def send_batch(self, dicts, input_opt='RAW'):
        """
        Send a batch update made up from premade range/value dicts.
        """
        logging.getLogger(__name__).info("Sending update to Fort Sheet.\n%s", str(dicts))
        await self.asheet.batch_update(dicts, input_opt)
        logging.getLogger(__name__).info("Finished sending update to Fort Sheet.\n%s", str(dicts))

    async def get_batch(self, a1range, dim='ROWS', value_format='UNFORMATTED_VALUE'):
        """
        Get a batch update made up from premade a1range dicts.
        """
        logging.getLogger(__name__).info("Get intel from Fort Sheet.\n%s", str(a1range))
        data = await self.asheet.batch_get(a1range, dim=dim, value_render=value_format)
        logging.getLogger(__name__).info("Finished import from Fort Sheet.\n%s", str(a1range))
        return data

    @staticmethod
    def update_sheet_user_dict(row, cry, name):
        """
        Create an update user dict. See AsyncGSheet.batch_update

        Returns: A list of update dicts to pass to batch_update.
        """
        cell_range = 'A{row}:B{row}'.format(row=row)
        return [{'range': cell_range, 'values': [[cry, name]]}]

    @staticmethod
    def update_system_dict(col, fort_status, um_status):
        """
        Create an update system dict. See AsyncGSheet.batch_update

        Returns: A list of update dicts to pass to batch_update.
        """
        cell_range = '{col}6:{col}7'.format(col=col)
        return [{'range': cell_range, 'values': [[fort_status], [um_status]]}]

    @staticmethod
    def update_drop_dict(system_col, user_row, amount):
        """
        Create an update drop dict. See AsyncGSheet.batch_update

        Returns: A list of update dicts to pass to batch_update.
        """
        cell_range = '{col}{row}:{col}{row}'.format(col=system_col, row=user_row)
        return [{'range': cell_range, 'values': [[amount]]}]

    @staticmethod
    def update_import_mode_dict(range, import_mode):
        """Change import mode from the sheet.

        Args:
            range (string): Range value in the sheet. (E.G. B9:B9)
            import_mode (string): Import mode, can only be True or False.

        Returns: A list of update dicts to pass to batch_update.
        """
        return [{'range': str(range), 'values': [[import_mode]]}]


class UMScanner(FortScanner):
    """
    Scanner for the Hudson undermine sheet.

    args:
        asheet: The AsyncGSheet that connects to the sheet.
    """
    def __init__(self, asheet):
        super().__init__(asheet, [UMHold, UMSystem, UMUser])

        # These are fixed based on current format
        self.system_col = 'D'
        self.user_col = 'B'
        self.user_row = 14
        self.sheet_src = EUMSheet.main

    def __repr__(self):
        return super().__repr__().replace('FortScanner', 'UMScanner')

    def parse_sheet(self, session):
        """
        Parse the updated sheet and return information to directly pass to scan.

        Returns:
            [systems, users, holds]
        """
        systems = self.systems()
        users = self.users(cls=UMUser)
        holds = self.holds(systems, users)

        self.flush_to_db(session, (users, systems, holds))

    def drop_db_entries(self, session):
        """
        Drop the main um entries in the um part of the database.
        """
        for cls in self.db_classes:
            try:
                session.query(cls).filter(cls.sheet_src == self.sheet_src).delete()
            except sqla_exc.ProgrammingError:  # Table was deleted or some other problem, attempt to recreate
                logging.getLogger(__name__).error("Drop DB Entries: Critical error, likely a table issue. Attempting to recreate if it was deleted.")
                cls.__table__.create(cogdb.engine)
        session.commit()

    def users(self, *, row_cnt=None, first_id=1, cls=UMUser):
        """
        Scan the users in the sheet and return sheet user objects.

        Args:
            row_cnt: The starting row for users, zero indexed. Default is user_row.
            first_id: The id to start for the users.
            cls: The class to create for each user found.
        """
        found = super().users(row_cnt=row_cnt, first_id=first_id, cls=cls)
        for user in found:
            user.sheet_src = self.sheet_src

        return found

    def systems(self, *, first_id=1):
        """
        Scan all the systems in the sheet.
        A UM System takes up two adjacent columns.

        Kwargs:
            first_id: The number to start ids at.

        Returns:
            A list of UMSystems to insert into db.
        """
        cell_column = cog.sheets.Column(self.system_col)
        found, cnt, sys_ind = [], first_id, 3

        try:
            while True:
                sys_cells = [x[:self.user_row - 1] for x in
                             self.cells_col_major[sys_ind:sys_ind + 2]]

                if not sys_cells[0][8] or 'Template' in sys_cells[0][8]:
                    break

                kwargs = kwargs_um_system(sys_cells, str(cell_column), sheet_src=self.sheet_src)
                kwargs['id'] = cnt
                cnt += 1
                cls = kwargs.pop('cls')
                found += [(cls(**kwargs))]

                sys_ind += 2
                cell_column.offset(2)
        except cog.exc.SheetParsingError:
            pass

        return found

    def holds(self, systems, users, *, first_id=1):
        """
        Parse the held and redeemed merits that fall under the same column as System.

        Args:
            systems: The SystemUMs parsed from sheet.
            users: The SheetRows parsed from sheet.

        Kwargs:
            first_id: The number to start ids at.

        Returns:
            A list of Hold objects to put in db.
        """
        found, cnt = [], first_id

        for system in systems:
            sys_ind = cog.sheets.column_to_index(system.sheet_col, zero_index=True)
            sys_cells = [x[self.user_row - 1:] for x in
                         self.cells_col_major[sys_ind:sys_ind + 2]]

            for user_ind, row in enumerate(zip(*sys_cells)):
                try:
                    held = cogdb.schema.parse_int(row[0])
                    redeemed = cogdb.schema.parse_int(row[1])
                    if not held and not redeemed:
                        continue

                    found += [UMHold(id=cnt, sheet_src=self.sheet_src,
                                     user_id=users[user_ind].id, system_id=system.id,
                                     held=held, redeemed=redeemed)]
                    cnt += 1
                except (IndexError, ValueError):
                    pass

        return found

    @staticmethod
    def update_systemum_dict(col, progress_us, progress_them, map_offset):
        """
        Create an update system dict. See AsyncGSheet.batch_update

        Returns: A list of update dicts to pass to batch_update.
        """
        cell_range = '{col}10:{col}13'.format(col=col)
        values = [[progress_us], [progress_them], ['Hold Merits'], [map_offset]]
        return [{'range': cell_range, 'values': values}]

    @staticmethod
    def update_systemum_priority_dict(col, priority):
        """
        Create an update system dict. See AsyncGSheet.batch_update

        Args:
            col: The main (left most) column of the system in sheet.
            priority: The new priority to set.

        Returns: A list of update dicts to pass to batch_update.
        """
        column = cog.sheets.Column(col)
        cell_range = '{col}8:{col}8'.format(col=column.fwd())
        return [{'range': cell_range, 'values': [[priority]]}]

    @staticmethod
    def update_hold_dict(system_col, user_row, held, redeemed):
        """
        Create an update hold dict. See AsyncGSheet.batch_update

        Returns: A list of update dicts to pass to batch_update.
        """
        col2 = cog.sheets.Column(system_col).fwd()
        cell_range = '{col1}{row}:{col2}{row}'.format(col1=system_col, col2=col2,
                                                      row=user_row)
        return [{'range': cell_range, 'values': [[held, redeemed]]}]

    @staticmethod
    def slide_templates(sheet_values, values):
        """ Add columns to the left of Templates. Slide the template to the right. """
        index = 1
        for column in sheet_values[0]:
            if 'Template' in column[8]:
                break
            index += 1

        # Deleting columns before / after templates
        del sheet_values[0][:index - 1]
        del sheet_values[0][4:]

        # Adding value to the row 13 when it's empty to pivot the table
        sheet_values[0][1].append('')
        sheet_values[0][3].append('')

        # Saving Control template to edit later
        um_sheet_temp = deepcopy(sheet_values)
        new_um_sheet = deepcopy(sheet_values)
        new_um_sheet_temp = None

        for item in values:
            columns_left_to_update = deepcopy(um_sheet_temp[0][0])
            columns_right_to_update = deepcopy(um_sheet_temp[0][1])
            for i in [3, 6, 7, 8, 9, 10]:
                if i == 3:
                    columns_left_to_update[i] = item['trigger']
                elif i == 6:
                    columns_right_to_update[i] = item['power']
                elif i == 7:
                    columns_right_to_update[i] = item['priority']
                elif i == 8:
                    columns_left_to_update[i] = item['sys_name']
                elif i in (9, 10):
                    columns_left_to_update[i] = 0
            if new_um_sheet_temp:
                new_um_sheet_temp = UMScanner.slide_formula_to_right(new_um_sheet_temp, index)
            else:
                new_um_sheet_temp = UMScanner.slide_formula_to_right(new_um_sheet, index)
            new_um_sheet_temp[0].insert(0, columns_right_to_update)
            new_um_sheet_temp[0].insert(0, columns_left_to_update)
            new_um_sheet[0].insert(0, columns_right_to_update)
            new_um_sheet[0].insert(0, columns_left_to_update)

        # Pivot the table
        new_um_sheet_temp[0] = [[row[i] for row in new_um_sheet_temp[0]] for i in range(13)]

        # Mapping data and sending them to the sheet
        return [{'range': '{}1:13'.format(cog.sheets.Column().offset(index + 2)), 'values': new_um_sheet_temp[0]}]

    @staticmethod
    def slide_formula_to_right(raw_um_sheet, sheet_index):
        """
        Local function that return a List of List with slided cells formula to the right by 2 columns.
        Args:
                raw_um_sheet: List of List returned by get_batch.
                sheet_index: Int, Which column is the first one of the slide. One-base index starting at column D.
        """
        temp_index = sheet_index
        temp = deepcopy(raw_um_sheet)
        for columns in temp[0]:
            column_init = cog.sheets.Column()
            column_name_2 = column_init.offset(temp_index + 2)
            column_name_3 = column_init.offset(1)
            column_name_4 = column_init.offset(1)
            column_name_5 = column_init.offset(1)
            for j in range(len(columns)):
                columns[j] = str(columns[j]) \
                    .replace("{}$".format(column_name_2),
                             "{}$".format(column_name_4)) \
                    .replace(":{}".format(column_name_3),
                             ":{}".format(column_name_5)) \
                    .replace("{}$".format(column_name_3),
                             "{}$".format(column_name_5))
            temp_index += 1
        return temp

    @staticmethod
    def slide_formula_by_offset(column, offset=0, start_ind=2, end_ind=13):
        """
        Slide the formula for any singular column by a given offset.
        Assumption is all formula in columns only refer to main and secondary column.

        Args:
            column: A column of text from a table, some with formula.
            offset: An offset to move the formula, positive will move up, negative down.
            start_ind: The index to start replacing formula.
            end_ind: The index to end replacing formula.

        Returns: Column with formula replaced.
        """
        main_col, sec_col = re.match(r'=SUM\(([A-Z]+).*:([A-Z]+).*\)', column[4]).groups()
        new_main_col = cog.sheets.Column(main_col).offset(offset)
        new_sec_col = cog.sheets.Column(sec_col).offset(offset)

        for ind in range(start_ind, end_ind):
            try:
                temp = column[ind]

                # Skip lines that aren't formula, all start with =
                try:
                    if str(temp)[0] != '=':
                        continue
                except IndexError:
                    continue

                for old, new in ((main_col, new_main_col), (sec_col, new_sec_col)):
                    temp = re.sub('{}(\\$?\\d+)'.format(old), '{}\\1'.format(new), temp)
                    temp = re.sub(":{}\\)".format(old), ":{})".format(new), temp)
                column[ind] = temp
            except (AttributeError, TypeError):
                pass

        return column

    @staticmethod
    def remove_um_system(sheet_values, system_name):
        """
        Remove columns for system_name from the sheet_values passed in.
        This will remove the columns, update formulas in others and pad at the right.

        Args:
            sheet_values: A 2d list of the values. If a 3D list is passed, will convert to 2.
                Important: Expected to be major index by column.
            system_name: The system to remove from the sheet.

        Returns: A payload to update the sheet.
        """
        new_values = []
        seen_system = False
        modify_formula = False
        max_len = max([len(x) for x in sheet_values])
        pad_col = ['' for _ in range(max_len)]

        for col in sheet_values:
            if not col:  # Can have empty column spacers, just pad
                col = pad_col
            elif seen_system:  # Skip secondary column
                seen_system = False
                modify_formula = True
                continue
            elif col[8].strip() == system_name:  # Skip main system column
                seen_system = True
                continue
            elif modify_formula and col[8].strip() != '':  # All formula to right of removal move
                col = UMScanner.slide_formula_by_offset(col, -2)

            new_values += [col]

        new_values += [pad_col, pad_col]
        new_values = cog.util.transpose_table(cog.util.pad_table_to_rectangle(new_values))

        return [{'range': 'D1:CZ', 'values': new_values}]


class SnipeScanner(UMScanner):
    """
    Specialization of the UMScanner for Snipe sheet.

    args:
        asheet: The AsyncGSheet that connects to the sheet.
    """
    def __init__(self, asheet):
        super().__init__(asheet)

        # For now, format is identical to UM Sheet.
        self.sheet_src = EUMSheet.snipe

    def __repr__(self):
        return super().__repr__().replace('FortScanner', 'SnipeScanner')

    def users(self, *, row_cnt=None, first_id=SNIPE_FIRST_ID, cls=UMUser):
        """
        Scan the users in the sheet and return sheet user objects.

        Args:
            row_cnt: The starting row for users, zero indexed. Default is user_row.
            first_id: The id to start for the users.
            cls: The class to create for each user found.

        Kwargs:
            first_id: The number to start ids at.

        Returns:
            A list of UMUsers to insert into db.
        """
        return super().users(row_cnt=row_cnt, first_id=first_id, cls=cls)

    def systems(self, *, first_id=SNIPE_FIRST_ID):
        """
        Scan all the systems in the sheet.
        A UM System takes up two adjacent columns.

        Kwargs:
            first_id: The number to start ids at.

        Returns:
            A list of UMSystems to insert into db.
        """
        return super().systems(first_id=first_id)

    def holds(self, systems, users, *, first_id=SNIPE_FIRST_ID):
        """
        Parse the held and redeemed merits that fall under the same column as System.

        Args:
            systems: The SystemUMs parsed from sheet.
            users: The SheetRows parsed from sheet.

        Kwargs:
            first_id: The number to start ids at.

        Returns:
            A list of Hold objects to put in db.
        """
        return super().holds(systems, users, first_id=first_id)


class KOSScanner(FortScanner):
    """
    Scanner for the Hudson KOS sheet.

    args:
        asheet: The AsyncGSheet that connects to the sheet.
    """
    def __init__(self, asheet):
        super().__init__(asheet, [KOS])

    def __repr__(self):
        return super().__repr__().replace('FortScanner', 'KOSScanner')

    def parse_sheet(self, session):
        """
        Parse the updated sheet and return information to directly pass to scan.

        Returns:
            [kos_entries]

        Raises:
            SheetParsingError - At least two commanders have same name.
        """
        entries = self.kos_entries()

        dupe_entries = entries[:]
        for ent in set(entries):
            dupe_entries.remove(ent)
        if dupe_entries:
            cmdrs = ["CMDR {} duplicated in sheet".format(x.cmdr) for x in dupe_entries]
            raise cog.exc.SheetParsingError("Duplicate CMDRs in KOS sheet.\n\n" + '\n'.join(cmdrs))

        self.flush_to_db(session, (entries,))

    def find_dupe(self, cmdr_name):
        """
        Check for the same cmdr being reported.

        Returns:
            None, no duplicate found.
            A number, the matching row number.
        """
        for cnt, row in enumerate(self.cells_row_major[1:], 2):
            if cmdr_name == row[0]:
                return cnt, row

        return None, None

    def kos_entries(self):
        """
        Process all the entries in the sheet into KOS objects for db.

        Returns:
            A list of KOS objects for db.
        """
        found = []

        for cnt, row in enumerate(self.cells_row_major[1:], 1):
            is_friendly = row[2][0] in ('f', 'F')
            found += [cogdb.schema.KOS(id=cnt, cmdr=row[0], squad=row[1], reason=row[3],
                                       is_friendly=is_friendly)]

        return found

    def next_free_row(self):
        """ Return the next free kos row. """
        return len(self.cells_col_major[0]) + 1

    def add_report_dict(self, kos_info):
        """
        Create an update system dict. See AsyncGSheet.batch_update

        Args:
            kos_info: A kos_info object, see cog.inara

        Returns: A list of update dicts to pass to batch_update.
        """
        values = [kos_info['cmdr'], kos_info['squad'],
                  "FRIENDLY" if kos_info['is_friendly'] else "KILL", kos_info['reason']]
        cell_range = 'A{row}:D{row}'.format(row=self.next_free_row())
        return [{'range': cell_range, 'values': [values]}]


class RecruitsScanner(FortScanner):
    """
    Scanner for the Hudson recruits sheet.

    args:
        asheet: The AsyncGSheet that connects to the sheet.
    """
    def __init__(self, asheet):
        super().__init__(asheet, [])

        self.first_free = 1

    def __repr__(self):
        return super().__repr__().replace('FortScanner', 'RecruitsScanner')

    def parse_sheet(self, session):
        """
        Unused, remains for consistency of interface.
        """
        raise NotImplementedError

    def update_first_free(self):
        """
        Simple go through existing cells to determine last used row.
        """
        for ind, row in enumerate(self.cells_col_major[0], 1):
            self.first_free = ind
            if row.strip() == "":  # Overshot by 1
                self.first_free -= 1
                break
        self.first_free += 1

        # Due to layout, entries start at row 3, something must have gone wrong
        if self.first_free < 3:
            raise cog.exc.InvalidCommandArgs("Could not detect the first free row in sheet. Please try again.")

        return self.first_free

    def add_recruit_dict(self, *, cmdr, discord_name, rank, platform, pmf, notes):
        """
        Create an update hold dict. See AsyncGSheet.batch_update

        Returns: A list of update dicts to pass to batch_update.
        """
        values = [[cmdr, discord_name, str(datetime.date.today())]]
        payload = [{'range': 'A{row}:C{row}'.format(row=self.first_free), 'values': values},
                   {'range': 'E{row}:E{row}'.format(row=self.first_free), 'values': [[rank]]},
                   {'range': 'H{row}:H{row}'.format(row=self.first_free), 'values': [[platform]]},
                   {'range': 'N{row}:O{row}'.format(row=self.first_free), 'values': [[pmf, notes]]}]
        self.first_free += 1  # Increment next row

        return payload


class CarrierScanner(FortScanner):
    """
    Scanner for the Hudson FC kos sheet.

    args:
        asheet: The AsyncGSheet that connects to the sheet.
    """
    def __init__(self, asheet):
        super().__init__(asheet, [])

    def __repr__(self):
        return super().__repr__().replace('FortScanner', 'CarrierScanner')

    def parse_sheet(self, session):
        """
        Push the update of carriers to the database.
        """
        cogdb.query.track_ids_update(session, self.carriers())

    def carriers(self, *, row_cnt=1):
        """
        Scan the carriers in the sheet.
        Expected format:
            Carrier ID | Squadron

        Args:
            row_cnt: The starting row for data, zero-based.

        Returns: A dictionary ready to update db.
        """
        found = {}

        users = [x[row_cnt:] for x in self.cells_col_major[:2]]
        for carrier_id, squad in list(zip(*users)):
            if carrier_id.strip() == '':
                continue

            found[carrier_id] = {
                "id": carrier_id,
                "squad": squad,
                "override": True,
            }

        return found


# FIXME: Currently doesn't insert into db pending move to spy_squirrel
class GalScanner(FortScanner):
    """
    Scanner for the Hudson OCR sheet.

    args:
        asheet: The AsyncGSheet that connects to the sheet.
    """
    def __init__(self, asheet):
        super().__init__(asheet, [])

        # All columns are mapped from letters to indices, so B == 1
        self.start_row = 2  # Usable info starts on this row
        self.prep_consolidation_row = 8  # Consolidation always at this row of prep column
        self.prep_col = 3  # Col where prep info starts
        self.trigger_col = 11  # Col where trigger info starts

    def __repr__(self):
        return super().__repr__().replace('FortScanner', 'GalScanner')

    def parse_sheet(self, _):
        """
        Will not parse, only pushing data.
        """
        pass

    def generate_system_map(self):
        """
        Use this map to correct systm names and ensure system name is not corrupted in ocr sheet.
        Looks up candidates against the EDDB database, maps the CAPS -> Normal system names.
        Any system names corrupted won't be in the map and will generate errors when looked up.

        Dictionary Format:
        {
            "16 CYGNI": "16 Cygni",
            "ADEO": "Adeo",
            ...
        }

        Returns: A dictionary mapping system names from ALL CAPS to normal eddb name.
        """
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            systems_in_sheets = [x.upper() for x in
                                 cogdb.eddb.get_controls_of_power(eddb_session, power='%hudson')]
        systems_in_sheets += [x for x in self.cells_col_major[self.prep_col][2:7] if x]

        # Generate a map for system name correction
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            eddb_systems = eddb_session.query(cogdb.eddb.System).\
                filter(cogdb.eddb.System.name.in_(systems_in_sheets)).\
                all()
            mapping_eddb = {x.name.lower(): x.name for x in eddb_systems}

        return {x: mapping_eddb[x.lower()] for x in systems_in_sheets
                if x.lower() in mapping_eddb}

    def update_dict(self, *, systems, row=3):
        """
        Create an update payload to update all cells on a sheet.

        Returns: A list of update dicts to pass to batch_update.
        """
        now = datetime.datetime.utcnow().replace(microsecond=0)
        end_row = row + len(systems)

        first, second, third = [], [], []
        for spy_system in systems:
            name = spy_system.system.name.upper()
            first += [[name, spy_system.fort, spy_system.um]]
            second += [[name, 0, 0, spy_system.fort_trigger, spy_system.um_trigger]]
            third += [[name, spy_system.held_merits]]
        payload = [
            {'range': f'A{row}:C{end_row}', 'values': first},
            {'range': f'L{row}:P{end_row}', 'values': second},
            {'range': f'R{row}:S{end_row}', 'values': third},
            {'range': 'C1:C1', 'values': [[str(now)]]},
        ]

        return payload

    async def clear_cells(self, *, row=3):
        """
        Use batch_clear to wipe out all existing data in the sheet where we will write.

        Returns: A list of update dicts to pass to batch_update.
        """
        end_row = 400
        ranges = [
            f'A{row}:C{end_row}',
            f'L{row}:P{end_row}',
            f'R{row}:S{end_row}'
        ]
        await self.asheet.batch_clear(ranges)


class FortTracker(FortScanner):
    """
    Scanner for the Main Hudson Fort Tracker sheet.

    args:
        asheet: The AsyncGSheet that connects to the sheet.
    """
    def __init__(self, asheet):
        super().__init__(asheet, [])

    def __repr__(self):
        return super().__repr__().replace('FortScanner', 'FortTracker')


async def init_scanners():
    """
    Initialized all parts related to google sheet scanners.

    Returns:
        A dict where key is name of scanner and value is the scanner.
    """
    scanners, init_coros = {}, []
    paths = cog.util.CONF.paths.unwrap
    cog.sheets.AGCM = cog.sheets.init_agcm(
        cog.util.rel_to_abs(paths['service_json']),
    )

    s_configs = cog.util.CONF.scanners.unwrap
    for key in s_configs:
        s_config = s_configs[key]
        asheet = cog.sheets.AsyncGSheet(s_config['id'], s_config['page'])
        init_coros += [asheet.init_sheet()]
        scanners[key] = getattr(sys.modules[__name__], s_config['cls'])(asheet)
        SCANNERS[key] = scanners[key]

    await asyncio.gather(*init_coros)

    return scanners


#  async def handle_ocr_sheet_update(client):  # pragma: no cover
    #  """
    #  This task is to be run only when the OCR sheet has been updated.
    #  Update the OCR information in the db and then take any actions
    #  required with changes.

    #  Args:
        #  client: The bot client itself.
    #  """
    #  # Update database by triggering manual refresh
    #  ocr_scanner = get_scanner('hudson_gal')
    #  await ocr_scanner.update_cells()
    #  with cfut.ProcessPoolExecutor(max_workers=1) as pool:
        #  await client.loop.run_in_executor(
            #  pool, ocr_scanner.scheduler_run,
        #  )

    #  # Data refreshed, analyse and update
    #  with cogdb.session_scope(cogdb.Session) as session:
        #  cell_updates = cogdb.query.ocr_update_fort_status(session)
        #  if cell_updates:
            #  await get_scanner('hudson_cattle').send_batch(cell_updates)
            #  logging.getLogger(__name__).info("Sent update to sheet.")
            #  logging.getLogger(__name__).info(str(cell_updates))


def get_scanner(name):
    """
    Store scanners in this module for shared use.
    """
    try:
        return SCANNERS[name]
    except KeyError as exc:
        raise cog.exc.InvalidCommandArgs("The scanners are not ready. Please try again in 15 seconds.") from exc
