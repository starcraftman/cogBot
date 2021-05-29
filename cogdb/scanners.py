"""
All sheet scanners are stored here for now

Sheet scanners make heavy use of cog.sheets.AsyncGSheet
"""
import asyncio
import datetime
import logging
import sys
from copy import deepcopy

import cog.exc
import cog.sheets
import cog.util
import cogdb
from cogdb.schema import (FortSystem, FortPrep, FortDrop, FortUser,
                          UMSystem, UMUser, UMHold, KOS,
                          kwargs_fort_system, kwargs_um_system)


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

    def parse_sheet(self, session=None):
        """
        Parse the updated sheet and return information to directly pass to scan.

        Returns:
            [systems, users, drops]
        """
        self.update_system_column()
        systems = self.fort_systems() + self.prep_systems()
        users = self.users()
        drops = self.drops(systems, users)

        if not session:
            session = cogdb.fresh_sessionmaker()()
        self.flush_to_db(session, (users, systems, drops))
        session.close()

    def flush_to_db(self, session, new_objs):
        """
        Flush the parsed values directly into the database.
        This method will purge old entries first.

        Args:
            session: A valid session for db.
            new_objs: A list of list of db objects to put in database.
        """
        for cls in self.db_classes:
            session.query(cls).delete()
            session.commit()

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

    async def get_batch(self, range, dim='ROWS', value_format='UNFORMATTED_VALUE'):
        """
        Get a batch update made up from premade range dicts.
        """
        logging.getLogger(__name__).info("Get intel from Fort Sheet.\n%s", str(range))
        data = await self.asheet.batch_get(range, dim=dim, value_render=value_format)
        logging.getLogger(__name__).info("Finished import from Fort Sheet.\n%s", str(range))
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

    def __repr__(self):
        return super().__repr__().replace('FortScanner', 'UMScanner')

    def parse_sheet(self, session=None):
        """
        Parse the updated sheet and return information to directly pass to scan.

        Returns:
            [systems, users, holds]
        """
        systems = self.systems()
        users = self.users(cls=UMUser)
        holds = self.holds(systems, users)

        if not session:
            session = cogdb.fresh_sessionmaker()()
        self.flush_to_db(session, (users, systems, holds))
        session.close()

    def systems(self):
        """
        Scan all the systems in the sheet.
        A UM System takes up two adjacent columns.

        Returns:
            A list of UMSystems to insert into db.
        """
        cell_column = cog.sheets.Column(self.system_col)
        found, cnt, sys_ind = [], 1, 3

        try:
            while True:
                sys_cells = [x[:self.user_row - 1] for x in
                             self.cells_col_major[sys_ind:sys_ind + 2]]

                if not sys_cells[0][8] or 'Template' in sys_cells[0][8]:
                    break

                kwargs = kwargs_um_system(sys_cells, str(cell_column))
                kwargs['id'] = cnt
                cnt += 1
                cls = kwargs.pop('cls')
                found += [(cls(**kwargs))]

                sys_ind += 2
                cell_column.offset(2)
        except cog.exc.SheetParsingError:
            pass

        return found

    def holds(self, systems, users):
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
            sys_cells = [x[self.user_row - 1:] for x in
                         self.cells_col_major[sys_ind:sys_ind + 2]]

            for user_ind, row in enumerate(zip(*sys_cells)):
                try:
                    held = cogdb.schema.parse_int(row[0])
                    redeemed = cogdb.schema.parse_int(row[1])
                    if not held and not redeemed:
                        continue

                    found += [UMHold(id=cnt, user_id=users[user_ind].id, system_id=system.id,
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
                elif i == 9 or i == 10:
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
    def slide_formula_to_left(raw_um_sheet, sheet_index):
        """
        Local function that return a List of List with slided cells formula to the left by 2 columns.
        Args:
                raw_um_sheet: List of List returned by get_batch.
                sheet_index: Int, Which column is the first one of the slide. One-base index starting at column D.
        """
        temp_index = sheet_index
        temp = deepcopy(raw_um_sheet)
        for columns in temp[0]:
            column_init = cog.sheets.Column()
            column_name_2 = column_init.offset(temp_index + 3)
            column_name_3 = column_init.offset(1)
            column_name_4 = column_init.offset(1)
            column_name_5 = column_init.offset(1)
            for j in range(len(columns)):
                columns[j] = str(columns[j])\
                    .replace("{}$".format(column_name_4),
                             "{}$".format(column_name_2))\
                    .replace(":{}".format(column_name_5),
                             ":{}".format(column_name_3)) \
                    .replace("{}$".format(column_name_5),
                             "{}$".format(column_name_3))

            temp_index += 1
        return temp

    @staticmethod
    def remove_um(sheet_values, systems_names):
        index = 0
        first_system_found = None
        # Find the first iteration of a system to delete
        for column in sheet_values[0]:
            if column[8] in systems_names:
                first_system_found = index
                del sheet_values[0][:index]
                break
            index += 1

        # reset index and adding 13th value to pivot later
        index = 0
        [cell.append('') for cell in sheet_values[0][1::2]]

        # Setup the temporary sheet needed for later computation
        new_um_sheet = deepcopy(sheet_values)
        new_um_sheet_temp = None
        new_um_index = None

        for column in sheet_values[0]:
            if column[8] in systems_names:
                if new_um_sheet_temp:
                    del new_um_sheet_temp[0][new_um_index:new_um_index + 2]
                    new_um_index -= 2
                    index -= 2
                    new_um_sheet_temp = UMScanner.slide_formula_to_left(new_um_sheet_temp, first_system_found + index)
                else:
                    del new_um_sheet[0][:2]
                    new_um_sheet_temp = UMScanner.slide_formula_to_left(new_um_sheet, first_system_found)
                    new_um_index = index - 2
                # Adding 2 empty columns to overwrite the right columns
                [new_um_sheet_temp[0].append(['', '', '', '', '', '', '', '', '', '', '', '', '']) for _ in range(2)]
            if new_um_index:
                new_um_index += 1
            index += 1
        new_um_sheet_temp[0] = [[row[i] for row in new_um_sheet_temp[0]] for i in range(13)]
        return [{'range': '{}1:13'.format(cog.sheets.Column().offset(first_system_found + 3)),
                 'values': new_um_sheet_temp[0]}]


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

    def parse_sheet(self, session=None):
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

        if not session:
            session = cogdb.fresh_sessionmaker()()
        self.flush_to_db(session, (entries,))
        session.close()

    def kos_entries(self):
        """
        Process all the entries in the sheet into KOS objects for db.

        Returns:
            A list of KOS objects for db.
        """
        found = []

        for cnt, row in enumerate(self.cells_row_major[1:], 1):
            is_friendly = row[2][0] in ('f', 'F')
            found += [cogdb.schema.KOS(id=cnt, cmdr=row[0], faction=row[1], reason=row[3],
                                       is_friendly=is_friendly)]

        return found

    def next_free_row(self):
        """ Return the next free kos row. """
        return len(self.cells_col_major[0]) + 1

    def add_report_dict(self, cmdr, faction, reason, is_friendly=False):
        """
        Create an update system dict. See AsyncGSheet.batch_update

        Args:
            cmdr: The cmdr name.
            faction: The faction of the cmdr.
            reason: The reason for adding this user

        Kwargs:
            is_friendly: If the cmdr is friendly or not.

        Returns: A list of update dicts to pass to batch_update.
        """
        values = [cmdr, faction, "FRIENDLY" if is_friendly else "KILL", reason]
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

    def parse_sheet(self, session=None):
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
    Scanner for the Hudson recruits sheet.

    args:
        asheet: The AsyncGSheet that connects to the sheet.
    """
    def __init__(self, asheet):
        super().__init__(asheet, [])

    def __repr__(self):
        return super().__repr__().replace('FortScanner', 'CarrierScanner')

    def parse_sheet(self, session=None):
        """
        Push the update of carriers to the database.
        """
        if not session:
            session = cogdb.fresh_sessionmaker()()

        cogdb.query.track_ids_update(session, self.carriers())
        session.commit()
        session.close()

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


async def init_scanners():
    """
    Initialized all parts related to google sheet scanners.

    Returns:
        A dict where key is name of scanner and value is the scanner.
    """
    scanners, init_coros = {}, []
    paths = cog.util.get_config("paths")
    cog.sheets.AGCM = cog.sheets.init_agcm(
        cog.util.rel_to_abs(paths['service_json']),
    )

    s_configs = cog.util.get_config('scanners')
    for key in s_configs:
        s_config = s_configs[key]
        asheet = cog.sheets.AsyncGSheet(s_config['id'], s_config['page'])
        init_coros += [asheet.init_sheet()]
        scanners[key] = getattr(sys.modules[__name__], s_config['cls'])(asheet)

    await asyncio.gather(*init_coros)

    return scanners
