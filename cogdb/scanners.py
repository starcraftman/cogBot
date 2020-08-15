"""
All sheet scanners are stored here for now

Sheet scanners make heavy use of cog.sheets.AsyncGSheet
"""
import asyncio
import logging
import sys

import cog.exc
import cog.sheets
import cog.util
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
    def __init__(self, asheet, users_args=None, db_classes=None):
        self.asheet = asheet
        self.db_classes = db_classes if db_classes else [Drop, System, SheetCattle]
        self.users_args = users_args if users_args else [SheetCattle, EFaction.hudson]
        self.lock = cog.util.RWLockWrite()

        self.cells_row_major = None
        self.__cells_col_major = None
        self.system_col = None
        self.user_col = 'B'
        self.user_row = 11

    def __repr__(self):
        keys = ['asheet', 'users_args', 'db_classes', 'lock',
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
        users = self.users(first_id=1)
        drops = self.drops(systems, users)

        if not session:
            session = cogdb.fresh_sessionmaker()()
        self.flush_to_db(session, (systems + users, drops))

    def flush_to_db(self, session, pending):
        """
        Flush the parsed values directly into the database.
        This method will purge old entries first.

        Args:
            session: A valid session for db.
            pending: A list of list of db objects to put in database.
        """
        for cls in self.db_classes:
            # TODO: Polymorphic sheet forces select, maybe drop polymorphism?
            for obj in session.query(cls).all():
                session.delete(obj)
            session.commit()

        for objs in pending:
            session.add_all(objs)
            session.commit()

    def users(self, *, row_cnt=None, first_id=1):
        """
        Scan the users in the sheet and return sheet user objects.

        Args:
            row_cnt: The starting row for users, zero indexed. Default is user_row.
            first_id: The id to start for the users.
        """
        found = []
        cls, faction = self.users_args

        if not row_cnt:
            row_cnt = self.user_row - 1

        users = [x[row_cnt:] for x in self.cells_col_major[:2]]
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

        for col in self.cells_col_major[ind:]:
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

        for col in self.cells_col_major[first_prep:first_system]:
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
            merit_cells = self.cells_col_major[ind][10:]

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
        logging.getLogger("cogdb.query").info("Finished sending update to Fort Sheet.\n%s", str(dicts))

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

    def __repr__(self):
        return super().__repr__().replace('FortScanner', 'UMScanner')

    def parse_sheet(self, session=None):
        """
        Parse the updated sheet and return information to directly pass to scan.

        Returns:
            [systems, users, holds]
        """
        systems = self.systems()
        users = self.users(first_id=1001)
        holds = self.holds(systems, users)

        if not session:
            session = cogdb.fresh_sessionmaker()()
        self.flush_to_db(session, (systems + users, holds))

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
                held, redeemed = row
                if held.strip() == '' and redeemed.strip() == '':
                    continue

                held = cogdb.schema.parse_int(held)
                redeemed = cogdb.schema.parse_int(redeemed)
                try:
                    hold = Hold(id=cnt, user_id=users[user_ind].id, system_id=system.id,
                                held=held, redeemed=redeemed)
                    found += [hold]

                    cnt += 1
                except IndexError:
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


class KOSScanner(FortScanner):
    """
    Scanner for the Hudson KOS sheet.

    args:
        asheet: The AsyncGSheet that connects to the sheet.
    """
    def __init__(self, asheet):
        super().__init__(asheet, None, [KOS])

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
    cog.sheets.AGCM = cog.sheets.init_agcm(
        cog.util.rel_to_abs(paths['json']),
        cog.util.rel_to_abs(paths['token']),
    )

    s_configs = cog.util.get_config('scanners')
    for key in s_configs:
        s_config = s_configs[key]
        asheet = cog.sheets.AsyncGSheet(s_config['id'], s_config['page'])
        init_coros += [asheet.init_sheet()]
        scanners[key] = getattr(sys.modules[__name__], s_config['cls'])(asheet)

    await asyncio.gather(*init_coros)

    return scanners
