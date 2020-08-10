"""
All sheet scanners are stored here for now

Sheet scanners make heavy use of cog.sheets.AsyncGSheet
"""
import asyncio
import logging
import sys

import uvloop

import cog.exc
import cog.sheets
import cogdb
from cogdb.schema import (System, PrepSystem, SystemUM, SheetCattle, SheetUM,
                          EFaction, Drop, Hold, kwargs_fort_system, kwargs_um_system,
                          KOS)


class FortScanner():
    """
    Scanner for the Hudson fort sheet.

    args:
        asheet: The AsyncGSheet that connects to the sheet.
        user_args: The arguements to use for users parsing.
        db_classes: The database classes that should be purged on replacement.
    """
    def __init__(self, asheet, user_args=None, db_classes=None):
        self.asheet = asheet
        self.users_args = user_args
        self.db_classes = db_classes if db_classes else [Drop, System, SheetCattle]
        self.users_args = user_args if user_args else [SheetCattle, EFaction.hudson]

        self.cells_row_major = None
        self.cells_column_major = None
        self.system_col = None
        self.user_col = 'B'
        self.user_row = 11

    async def update_cells(self):
        """ Fetch all cells from the sheet. """
        self.cells_row_major = await self.asheet.whole_sheet()
        self.cells_column_major = cog.util.transpose_table(self.cells_row_major)

    def drop_entries(self, session):
        """
        Before scan, drop the matching entries in the table.
        """
        for cls in self.db_classes:
            for matched in session.query(cls):
                session.delete(matched)

    def scan(self, session):
        """
        Scan the entire sheet for all data.
        Update the cells before parsing.
        """
        self.update_system_column()

        systems = self.fort_systems() + self.prep_systems()
        users = self.users(first_id=1)
        drops = self.drops(systems, users)

        self.drop_entries(session)
        session.commit()
        session.add_all(systems + users)
        session.commit()
        session.add_all(drops)
        session.commit()

        return True

    def users(self, *, row_cnt=None, first_id=1):
        """
        Scan the users in the sheet and return sheet user objects.
        N.B. Depends on accuracy of AsyncGSheet.last_row, in underlying sheet.

        Args:
            row_cnt: The starting row for users, zero indexed. Default is user_row.
            first_id: The id to start for the users.
        """
        found = []
        cls, faction = self.users_args

        if not row_cnt:
            row_cnt = self.user_row - 1

        users = [x[row_cnt:self.asheet.last_row] for x in self.cells_column_major[:2]]
        for cry, name in list(zip(*users)):
            row_cnt += 1
            if name.strip() == '':
                continue

            sheet_user = cls(id=first_id, cry=cry, name=name, faction=faction, row=row_cnt)
            first_id += 1
            if sheet_user in found:
                rows = [other.row for other in found if other == sheet_user] + [row_cnt]
                raise cog.exc.NameCollisionError("Fort", sheet_user.name, rows)

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

        for col in self.cells_column_major[ind:]:
            kwargs = kwargs_fort_system(col[0:10], order, str(cell_column))
            kwargs['id'] = order
            found += [System(**kwargs)]

            order += 1
            cell_column.fwd()

        return found

    def prep_systems(self):
        """
        Scan the Prep systems if any into the System db.

        Preps exist in range [D, system_col)
        """
        found, order, cell_column = [], 0, cog.sheets.Column('C')
        first_prep = cog.sheets.column_to_index(str(cell_column))
        first_system = cog.sheets.column_to_index(self.system_col) - 1

        for col in self.cells_column_major[first_prep:first_system]:
            order = order + 1
            cell_column.fwd()
            col = col[0:10]

            if col[-1].strip() == "TBA":
                continue

            kwargs = kwargs_fort_system(col, order, str(cell_column))
            kwargs['id'] = 1000 + order

            found += [PrepSystem(**kwargs)]

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
            merit_cells = self.cells_column_major[ind][10:]

            for user in users:
                try:
                    amount = int(merit_cells.pop(0).strip())
                    found.append(Drop(id=cnt, user_id=user.id, system_id=system.id,
                                      amount=cogdb.schema.parse_int(amount)))
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

    async def send_batch(self, dicts):
        """
        Seend a batch update made up from premade range/value dicts.
        """
        logging.getLogger("cogdb.query").info("Sending update to Fort Sheet.\n%s", str(dicts))
        await self.asheet.batch_update(dicts)

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
        super().__init__(asheet, [SheetUM, EFaction.hudson], [Hold, SystemUM, SheetUM])

        # These are fixed based on current format
        self.system_col = 'D'
        self.user_col = 'B'
        self.user_row = 14

    def scan(self, session):
        """
        Main function, scan the sheet into the database.
        """
        systems = self.systems()
        users = self.users(first_id=1001)
        holds = self.holds(systems, users)

        self.drop_entries(session)
        session.commit()
        session.add_all(systems + users)
        session.commit()
        session.add_all(holds)
        session.commit()

        return True

    def systems(self):
        """
        Scan all the systems in the sheet.
        A UM System takes up two adjacent columns.

        Returns:
            A list of UMSystems to insert into db.
        """
        cell_column = cog.sheets.Column(self.system_col)
        found, cnt, sys_ind = [], 1, 3

        while True:
            sys_cells = [x[:13] for x in self.cells_column_major[sys_ind:sys_ind + 2]]

            if not sys_cells[0][8] or 'Template' in sys_cells[0][8]:
                break

            kwargs = kwargs_um_system(sys_cells, str(cell_column))
            kwargs['id'] = cnt
            cnt += 1
            cls = kwargs.pop('cls')
            found += [(cls(**kwargs))]

            sys_ind += 2
            cell_column.offset(2)

        return found

    def holds(self, systems, users):
        """
        Parse the held and redeemed merits that fall under the same column as System.
        N.B. Depends on accuracy of AsyncGSheet.last_row, in underlying sheet.

        Args:
            systems: The SystemUMs parsed from sheet.
            users: The SheetRows parsed from sheet.

        Returns:
            A list of Hold objects to put in db.
        """
        found, cnt = [], 1

        for system in systems:
            sys_ind = cog.sheets.column_to_index(system.sheet_col, zero_index=True)
            sys_cells = [x[13:self.asheet.last_row] for x
                         in self.cells_column_major[sys_ind:sys_ind + 2]]

            for user_ind, row in enumerate(zip(*sys_cells)):
                held, redeemed = row
                if held.strip() == '' and redeemed.strip() == '':
                    continue

                held = cogdb.schema.parse_int(held)
                redeemed = cogdb.schema.parse_int(redeemed)
                hold = Hold(id=cnt, user_id=users[user_ind].id, system_id=system.id,
                            held=held, redeemed=redeemed)
                found += [hold]

                cnt += 1

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


class KOSScanner(FortScanner):
    """
    Scanner for the Hudson KOS sheet.

    args:
        asheet: The AsyncGSheet that connects to the sheet.
    """
    def __init__(self, asheet):
        super().__init__(asheet, None, [KOS])

    def scan(self, session):
        """
        Main function, scan the sheet into the database.

        Raises:
            SheetParsingError - Duplicate CMDRs detected.
        """
        entries = self.kos_entries()

        dupe_entries = entries[:]
        for ent in set(entries):
            dupe_entries.remove(ent)
        if dupe_entries:
            cmdrs = ["CMDR {} duplicated in sheet".format(x.cmdr) for x in dupe_entries]
            raise cog.exc.SheetParsingError("Duplicate CMDRs in KOS sheet.\n\n" + '\n'.join(cmdrs))

        self.drop_entries(session)
        session.commit()
        session.add_all(entries)
        session.commit()

        return True

    def kos_entries(self):
        """
        Process all the entries in the sheet into KOS objects for db.

        Returns:
            A list of KOS objects for db.
        """
        found = []

        for cnt, row in enumerate(self.cells_row_major[1:], 1):
            try:
                danger = int(row[2])
            except ValueError:
                danger = 0
            is_friendly = row[3][0] in ('f', 'F')
            found += [cogdb.schema.KOS(id=cnt, cmdr=row[0], faction=row[1], danger=danger,
                                       is_friendly=is_friendly)]

        return found

    @staticmethod
    def kos_report_dict(row, *values):
        """
        Create an update system dict. See AsyncGSheet.batch_update

        Args:
            cmdr: The cmdr name.
            faction: The faction of the cmdr.
            danger: The danger rating of cmdr.
            friend_or_kill: If the cmdr is to be killed or not.

        Returns: A list of update dicts to pass to batch_update.
        """
        cell_range = 'A{row}:D{row}'.format(row=row)
        return [{'range': cell_range, 'values': [list(values)]}]


async def init_scanners():
    """
    Initialized all parts related to google sheet scanners.

    Returns:
        A dict where key is name of scanner and value is the scanner.
    """
    scanners, init_coros = {}, []
    paths = cog.util.get_config("paths")
    cog.sheets.AGCM = cog.sheets.init_agcm(paths['json'], paths['token'])

    s_configs = cog.util.get_config('scanners')
    for key in s_configs:
        s_config = s_configs[key]
        asheet = cog.sheets.AsyncGSheet(s_config['id'], s_config['page'])
        init_coros += [asheet.init_sheet()]
        scanners[key] = getattr(sys.modules[__name__], s_config['cls'])(asheet)

    await asyncio.gather(*init_coros)
    await asyncio.gather(*[x.update_cells() for x in scanners.values()])

    return scanners


async def test_fortscanner():
    paths = cog.util.get_config('paths')
    cog.sheets.AGCM = cog.sheets.init_agcm(paths['json'], paths['token'])

    sheet = cog.util.get_config('scanners', 'hudson_cattle')
    asheet = cog.sheets.AsyncGSheet(sheet['id'], sheet['page'])
    await asheet.init_sheet()

    fscan = FortScanner(asheet)
    await fscan.update_cells()
    fscan.update_system_column()

    #  users = fscan.users()
    #  f_sys = fscan.fort_systems()
    #  __import__('pprint').pprint(f_sys)
    #  p_sys = fscan.prep_systems()
    #  merits = fscan.merits(f_sys + p_sys, users)
    fscan.scan(cogdb.Session())


async def test_umscanner():
    paths = cog.util.get_config('paths')
    cog.sheets.AGCM = cog.sheets.init_agcm(paths['json'], paths['token'])

    sheet = cog.util.get_config('scanners', 'hudson_undermine')
    asheet = cog.sheets.AsyncGSheet(sheet['id'], sheet['page'])
    await asheet.init_sheet()

    fscan = UMScanner(asheet)
    await fscan.update_cells()

    #  systems = fscan.systems()
    #  print(systems)
    #  users = fscan.users()
    #  print(users)
    #  merits = fscan.merits(systems, users)
    #  print(merits)
    fscan.scan(cogdb.Session())


async def test_kosscanner():
    paths = cog.util.get_config('paths')
    cog.sheets.AGCM = cog.sheets.init_agcm(paths['json'], paths['token'])

    sheet = cog.util.get_config('scanners', 'hudson_kos')
    asheet = cog.sheets.AsyncGSheet(sheet['id'], sheet['page'])
    await asheet.init_sheet()

    fscan = KOSScanner(asheet)
    await fscan.update_cells()

    fscan.scan(cogdb.Session())


def main():
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.get_event_loop().set_debug(True)

    coro = test_fortscanner
    try:
        if sys.argv[1] == 'um':
            coro = test_umscanner
        elif sys.argv[1] == 'kos':
            coro = test_kosscanner
    except IndexError:
        pass

    loop = asyncio.get_event_loop()
    loop.run_until_complete(coro())


if __name__ == "__main__":
    main()
