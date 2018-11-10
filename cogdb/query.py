"""
Module should handle logic related to querying/manipulating tables from a high level.
"""
from __future__ import absolute_import, print_function
import logging
import os
import sys
import tempfile

import sqlalchemy.exc as sqla_exc
import sqlalchemy.orm.exc as sqla_oexc

import cog.exc
import cog.sheets
from cog.util import substr_match
import cogdb
from cogdb.schema import (DUser, System, PrepSystem, SystemUM, SheetRow, SheetCattle, SheetUM,
                          Drop, Hold, EFaction, ESheetType, kwargs_fort_system, kwargs_um_system,
                          Admin, ChannelPerm, RolePerm, FortOrder, KOS)


DEFER_MISSING = 750
HUDSON_CONTROLS = [
    '16 Cygni', '37 Xi Bootis', '39 Serpentis', 'Abi', 'Adeo', 'Alpha Fornacis',
    'Anlave', 'Aornum', 'Arnemil', 'Atropos', 'BD+42 3917', 'Bhritzameno', 'Burr', 'Dongkum',
    'Epsilon Scorpii', 'Frey', 'G 250-34', 'GD 219', 'Gilgamesh', 'Gliese 868',
    'Groombridge 1618', 'HR 2776', 'Kaushpoos', 'Lalande 39866', 'LHS 1197', 'LHS 142',
    'LHS 1541', 'LHS 3447', 'LHS 3577', 'LHS 3749', 'LHS 3885', 'LHS 6427', 'LP 291-34',
    'LP 580-33', 'LPM 229', 'LTT 15449', 'LTT 15574', 'Lung', 'Lushertha', 'Mariyacoch',
    'Mulachi', 'Muncheim', 'Nanomam', 'NLTT 46621', 'Nurundere', 'Othime', 'Parutis',
    'Phanes', 'Phra Mool', 'Rana', 'Ross 33', 'Shoujeman', 'Sol', 'Tun', 'Vega', 'Venetic',
    'Wat Yu', 'Wolf 25', 'Wolf 867', 'Wolf 906', 'WW Piscis Austrini'
]
WINTERS_CONTROLS = [
    '169 G. Canis Majoris', '18 Puppis', '41 Lambda Hydrae', '54 G. Antlia',
    'Amuzgo', 'Ao Kang', 'BD-21 3153', 'Binjamingi', 'Breksta', 'Bulkuylkana',
    'Bunda', 'C Hydrae', 'Carnoeck', 'Chandra', 'Charunder', 'Crowfor', 'Dierfar',
    'Eir', 'Elli', 'Ennead', 'Erivit', 'Fan Yin', 'Fousang', 'HIP 24655', 'HIP 38747',
    'HIP 44811', 'HIP 47328', 'HIP 50489', 'Kali', 'Kaline', 'Kanati', 'Kaura', 'Kherthaje',
    'Kwattrages', 'LFT 601', 'LFT 926', 'LHS 1887', 'LHS 2150', 'LHS 235', 'LP 417-213',
    'LP 792-33', 'LP 906-9', 'LTT 4337', 'Lumbla', 'Mangwe', 'Mechucos', 'Mendindui',
    'Mexicatese', 'Minmar', 'Miroman', 'Momoirent', 'Morixa', 'Namte', 'Neche',
    'NLTT 19808', 'OU Geminorum', 'Perktomen', 'Ragapajo', 'Reieni', 'Rhea', 'Ross 89',
    'Sanos', 'Sawali', 'Shenggan', 'Simyr', 'Skeggiko O', 'V902 Centauri', 'Velnians',
    'Xiriwal', 'Yam', 'Zeta Trianguli Australis', 'Pepper', 'Hyades Sector IC-K b9-4'
]


def fuzzy_find(needle, stack, obj_attr='zzzz', ignore_case=True):
    """
    Searches for needle in whole stack and gathers matches. Returns match if only 1.

    Raise separate exceptions for NoMatch and MoreThanOneMatch.
    """
    matches = []
    for obj in stack:
        try:
            if substr_match(needle, getattr(obj, obj_attr, obj), ignore_case=ignore_case):
                matches.append(obj)
        except cog.exc.NoMatch:
            pass

    num_matches = len(matches)
    if num_matches == 1:
        return matches[0]
    elif num_matches == 0:
        cls = stack[0].__class__.__name__ if getattr(stack[0], '__class__') else 'string'
        raise cog.exc.NoMatch(needle, cls)
    else:
        raise cog.exc.MoreThanOneMatch(needle, matches, obj_attr)


def dump_db():  # pragma: no cover
    """
    Purely debug function, shunts db contents into file for examination.
    """
    session = cogdb.Session()
    fname = os.path.join(tempfile.gettempdir(), 'dbdump_' + os.environ.get('COG_TOKEN', 'dev'))
    print("Dumping db contents to:", fname)
    with open(fname, 'w') as fout:
        for cls in [DUser, SheetRow, System, SystemUM, Drop, Hold,
                    FortOrder, Admin, RolePerm, ChannelPerm]:
            fout.write('---- ' + str(cls) + ' ----\n')
            fout.writelines([str(obj) + "\n" for obj in session.query(cls)])


def get_duser(session, discord_id):
    """
    Return the DUser that has the same discord_id.

    Raises:
        NoMatch - No possible match found.
    """
    try:
        return session.query(DUser).filter_by(id=discord_id).one()
    except sqla_oexc.NoResultFound:
        raise cog.exc.NoMatch(discord_id, 'DUser')


def ensure_duser(session, member):
    """
    Ensure a member has an entry in the dusers table. A DUser is required by all users.

    Returns: The DUser
    """
    try:
        duser = get_duser(session, member.id)
        duser.display_name = member.display_name
    except cog.exc.NoMatch:
        duser = add_duser(session, member)

    return duser


def add_duser(session, member, *, faction=EFaction.hudson):
    """
    Add a discord user to the database.
    """
    name = member.display_name
    new_duser = DUser(id=member.id, display_name=name,
                      pref_name=name, faction=faction)
    session.add(new_duser)
    session.commit()

    return new_duser


def check_pref_name(session, duser, new_name):
    """
    Check that new name is not taken by another DUser or present as a stray in SheetRows.

    Raises:
        InvalidCommandArgs - DUser.pref_name taken by another DUser.
    """
    others = session.query(DUser).filter(DUser.id != duser.id, DUser.pref_name == new_name).all()
    if others:
        raise cog.exc.InvalidCommandArgs(
            "Sheet name {}, taken by {}.\n\nPlease choose another.".format(
                new_name, others[0].display_name))

    # Note: Unlikely needed, should be caught above. However, no fixed relationship guaranteeing.
    # for sheet in session.query(SheetRow).filter(SheetRow.name == new_name).all():
        # if sheet.duser(session).id != duser.id:
            # raise cog.exc.InvalidCommandArgs("Sheet name {}, taken by {}.\n\nPlease choose another.".format(new_name, sheet.duser(session).display_name))


def next_sheet_row(session, *, cls, faction, start_row):
    """
    Find the next available row to add a SheetRow for.

    Must scan all users, gaps may exist in sheet.
    """
    try:
        users = session.query(cls).filter_by(faction=faction).order_by(cls.row).all()
        last_user = users[0]
        next_row = users[-1].row + 1
        for user in users[1:]:
            if last_user.row + 1 != user.row:
                next_row = last_user.row + 1
                break
            last_user = user
    except IndexError:
        next_row = start_row

    return next_row


def add_sheet(session, name, **kwargs):
    """
    Simply add user past last user in sheet.

    Kwargs:
        cry: The cry to use.
        faction: By default Hudson. Any of EFaction.
        type: By default cattle sheet. Any of SheetRow subclasses.
        start_row: Starting row if none inserted.
    """
    faction = kwargs.get('faction', EFaction.hudson)
    cls = getattr(sys.modules[__name__], kwargs.get('type', ESheetType.cattle))
    cry = kwargs.get('cry', '')

    next_row = next_sheet_row(session, cls=cls, faction=faction,
                              start_row=kwargs['start_row'])
    sheet = cls(name=name, cry=cry, row=next_row, faction=faction)
    session.add(sheet)
    session.commit()

    return sheet


def fort_get_medium_systems(session):
    """
    Return unfortified systems designated for small/medium ships.
    """
    mediums = session.query(System).all()
    unforted = [med for med in mediums if "S/M" in med.notes and not med.is_fortified and not
                med.skip and not med.missing < DEFER_MISSING]
    return unforted


def fort_get_systems(session, mediums=True):
    """
    Return a list of all Systems. PrepSystems are not included.

    args:
        mediums: If false, exclude all systems designated for j
                 Determined by "S/M" being in notes.
    """
    query = session.query(System).filter(System.type != 'prep')
    if not mediums:
        med_names = [med.name for med in fort_get_medium_systems(session)]
        query = query.filter(System.name.notin_(med_names))

    return query.all()


def fort_get_preps(session):
    """
    Return a list of all PrepSystems.
    """
    return session.query(PrepSystem).all()


def fort_find_current_index(session):
    """
    Scan Systems from the beginning to find next unfortified target that is not Othime.

    Raises:
        NoMoreTargets - No more targets left OR a serious problem with data.
    """
    for ind, system in enumerate(fort_get_systems(session)):
        if system.is_fortified or system.skip or system.missing < DEFER_MISSING:
            continue

        return ind

    lines = [
        "**Critical Error**",
        "----------------",
        "Fort information invalid, cannot determine fort targets.",
        "\nPlease check the fort sheet, it may be broken.",
        "Once the sheet displays properly run: `!admin scan`",
    ]
    raise cog.exc.NoMoreTargets('\n'.join(lines))


def fort_find_system(session, system_name, search_all=True):
    """
    Return the System with System.name that matches.
    If search_all True, search all systems.
    If search_all False, search from current target forward.

    Raises:
        NoMatch - No possible match found.
        MoreThanOneMatch - Too many matches possible, ask user to resubmit.
    """
    try:
        return session.query(System).filter_by(name=system_name).one()
    except (sqla_oexc.NoResultFound, sqla_oexc.MultipleResultsFound):
        index = 0 if search_all else fort_find_current_index(session)
        systems = fort_get_systems(session)[index:] + fort_get_preps(session)
        return fuzzy_find(system_name, systems, 'name')


def fort_get_systems_by_state(session):
    """
    Return a dictionary that lists the systems states below:

        left: Has neither been fortified nor undermined.
        fortified: Has been fortified and not undermined.
        undermined: Has been undermined and not fortified.
        cancelled: Has been both fortified and undermined.
    """
    log = logging.getLogger('cogdb.query')
    states = {
        'cancelled': [],
        'fortified': [],
        'left': [],
        'undermined': [],
        'skipped': [],
    }

    for system in fort_get_systems(session):
        log.info('STATE - %s', system)
        if system.is_fortified and system.is_undermined:
            states['cancelled'].append(system)
        if system.is_undermined:
            states['undermined'].append(system)
        if system.is_fortified:
            states['fortified'].append(system)
        if not system.is_fortified and not system.skip:
            states['left'].append(system)
        if system.skip:
            states['skipped'].append(system)

    return states


def fort_get_targets(session):
    """
    Returns a list of Systems that should be fortified.

    - First System is not Othime and is unfortified.
    - Second System if prsent is Othime, only when not fortified.
    - All Systems after are prep targets.
    """
    targets = fort_order_get(session)
    if targets:
        return targets[:1]

    current = fort_find_current_index(session)
    systems = fort_get_systems(session)
    targets = [systems[current]]

    mediums = fort_get_medium_systems(session)
    if mediums and mediums[0].name != targets[0].name:
        targets.append(mediums[0])

    targets += fort_get_preps(session)

    return targets


def fort_get_next_targets(session, count=1):
    """
    Return next 'count' fort targets.
    """
    systems = fort_order_get(session)
    start = 1
    if not systems:
        systems = fort_get_systems(session)
        start = fort_find_current_index(session) + 1

    targets = []
    for system in systems[start:]:
        if system.is_fortified or system.skip or system.missing < DEFER_MISSING:
            continue

        targets.append(system)
        count = count - 1

        if count == 0:
            break

    return targets


def fort_get_deferred_targets(session):
    """
    Return all deferred targets under deferal amount.
    """
    return [system for system in fort_get_systems(session)
            if system.missing < DEFER_MISSING and not system.is_fortified]


def fort_add_drop(session, *, user, system, amount):
    """
    Add a Drop for 'amount' to the database where Drop intersects at:
        System.name and SUser.name
    If fort exists, increment its value. Else add it to database.

    Kwargs: system, user, amount

    Returns: The Drop object.

    Raises:
        InvalidCommandArgs: User requested an amount outside bounds [-800, 800]
    """
    if amount not in range(-800, 801):
        raise cog.exc.InvalidCommandArgs('Drop amount must be in range [-800, 800]')

    try:
        drop = session.query(Drop).filter_by(user_id=user.id, system_id=system.id).one()
    except sqla_oexc.NoResultFound:
        drop = Drop(user_id=user.id, system_id=system.id, amount=0)
        session.add(drop)

    log = logging.getLogger('cogdb.query')
    log.info('ADD_DROP - Before: Drop %s, System %s', drop, system)
    drop.amount = max(0, drop.amount + amount)
    system.fort_status = system.fort_status + amount
    session.commit()
    log.info('ADD_DROP - After: Drop %s, System %s', drop, system)

    return drop


def fort_order_get(_):
    """
    Get the order of systems to fort.

    If any systems have been completed, remove them from the list.

    Returns: [] if no systems set, else a list of System objects.
    """
    systems = []
    dsession = cogdb.Session()  # Isolate deletions, feels a bit off though
    for fort_order in dsession.query(FortOrder).order_by(FortOrder.order):
        system = dsession.query(System).filter_by(name=fort_order.system_name).one()
        if system.is_fortified or system.missing < DEFER_MISSING:
            dsession.delete(fort_order)
        else:
            systems += [system]

    dsession.commit()
    return systems


def fort_order_set(session, system_names):
    """
    Simply set the systems in the order desired.

    Ensure systems are actually valid before.
    """
    try:
        for ind, system_name in enumerate(system_names):
            if not isinstance(system_name, System):
                system_name = fort_find_system(session, system_name).name
            session.add(FortOrder(order=ind, system_name=system_name))
        session.commit()
    except (sqla_exc.IntegrityError, sqla_oexc.FlushError):
        session.rollback()
        raise cog.exc.InvalidCommandArgs("Duplicate system specified, check your command!")
    except cog.exc.NoMatch:
        session.rollback()
        raise cog.exc.InvalidCommandArgs("System '{}' not found in fort systems.".format(system_name))


def fort_order_drop(session, systems):
    """
    Drop the given system_names from the override table.
    """
    for system_name in systems:
        try:
            if isinstance(system_name, System):
                system_name = system_name.name
            session.delete(session.query(FortOrder).filter_by(system_name=system_name).one())
        except sqla_oexc.NoResultFound:
            pass

    session.commit()


class SheetScanner(object):
    """
    Scan a sheet to populate the database with information
    Also provide methods to update the sheet with new data

    Process whole sheets at a time with gsheet.whole_sheet()

    Important Note:
        Calls to modify the sheet should be asynchronous.
        Register them as futures and allow them to finish without waiting on.
    """
    def __init__(self, gsheet, user_args, db_classes):
        """
        Args:
            sheet_key: The name of the sheet information in the config, i.e. 'hudson_cattle'
            user_args: The type of user to create on parsing.
            db_classes: The classes this scanner manages in the db.
        """
        self._gsheet = gsheet
        self.users_args = user_args
        self.db_classes = db_classes
        self.cells = None
        self.user_col = None
        self.user_row = None
        self.system_col = None

    @property
    def gsheet(self):
        """
        Return on demand GSheet.
        """
        # FIXME: Testing hack, I'm open to suggestions.
        if isinstance(self._gsheet, type({})):
            paths = cog.util.get_config('paths')
            return cog.sheets.GSheet(self._gsheet,
                                     cog.util.rel_to_abs(paths['json']),
                                     cog.util.rel_to_abs(paths['token']))

        return self._gsheet

    def drop_entries(self, session):
        """
        Before scan, drop the matching entries in the table.
        """
        for cls in self.db_classes:
            for matched in session.query(cls):
                session.delete(matched)

    def scan(self):
        """
        Main function, scan the sheet into the database.
        """
        raise NotImplementedError

    def users(self, cls, faction, first_id=1):
        """
        Scan the users in the sheet and return SUser objects.

        Args:
            cls: The subclass of SheetRow
            faction: The faction owning the sheet.
        """
        log = logging.getLogger('cogdb.query')
        row = self.user_row - 1
        user_column = cog.sheets.column_to_index(self.user_col)
        cry_column = user_column - 1

        found = []
        cnt = first_id
        for user in self.cells[user_column][row:]:
            row += 1
            log.debug('SCANNER - row %d -> user %s', row, user)

            if user == '':  # Users sometimes miss an entry
                continue

            try:
                cry = self.cells[cry_column][row - 1]
            except IndexError:
                cry = ''

            sheet_user = cls(id=cnt, name=user, faction=faction, row=row, cry=cry)
            cnt += 1
            if sheet_user in found:
                rows = [other.row for other in found if other == sheet_user] + [row]
                sheet_type = 'Fort' if 'Fort' in self.__class__.__name__ else 'Undermining'
                raise cog.exc.NameCollisionError(sheet_type, sheet_user.name, rows)

            found.append(sheet_user)
            log.info('SCANNER - ADDING row %d -> user %s, cry: %s', row, user, cry)

        return found

    def systems(self):
        """
        Scan and parse the system information.
        """
        raise NotImplementedError

    def merits(self, systems, users):
        """
        Scan and parse the merit information by system and user.
        """
        raise NotImplementedError

    def update_sheet_user(self, row, cry, name):
        """
        Update the user cry and name on the given row.
        """
        col1 = cog.sheets.Column(self.user_col).prev()
        cell_range = '!{col1}{row}:{col2}{row}'.format(row=row, col1=col1,
                                                       col2=self.user_col)
        return self.gsheet.update(cell_range, [[cry, name]])


class FortScanner(SheetScanner):
    """
    Scanner for the Hudson fort sheet.

    args:
        gsheet: Either a dictionary to create a GSheet from or a premade GSheet.
    """
    def __init__(self, gsheet):
        super().__init__(gsheet, (SheetCattle, EFaction.hudson),
                         [Drop, System, SheetCattle])
        self.system_col = None
        self.user_col = 'B'
        self.user_row = 11

    def scan(self):
        """
        Main function, scan the sheet into the database.
        """
        self.cells = self.gsheet.whole_sheet()
        self.system_col = self.find_system_column()

        systems = self.fort_systems() + self.prep_systems()
        users = self.users(*self.users_args, first_id=1)
        merits = self.merits(systems, users)

        session = cogdb.Session()
        self.drop_entries(session)
        session.commit()
        session.add_all(systems + users)
        session.commit()
        session.add_all(merits)
        session.commit()

        return True

    def systems(self):
        return self.fort_systems() + self.prep_systems()

    def fort_systems(self):
        """
        Scan and parse the system information into System objects.
        """
        log = logging.getLogger('cogdb.query')
        found = []
        cell_column = cog.sheets.Column(self.system_col)
        first_system = cog.sheets.column_to_index(str(cell_column))
        order = 1

        try:
            for col in self.cells[first_system:]:
                log.debug('FSYSSCAN - Cells: %s', str(col[0:10]))
                kwargs = kwargs_fort_system(col, order, str(cell_column))
                kwargs['id'] = order
                log.debug('FSYSSCAN - Kwargs: %s', str(kwargs))

                found.append(System(**kwargs))
                log.info('FSYSSCAN - System Added: %s', found[-1])
                order = order + 1
                cell_column.next()
        except cog.exc.SheetParsingError:
            pass

        return found

    def prep_systems(self):
        """
        Scan the Prep systems if any into the System db.

        Preps exist in range [D, system_col)
        """
        log = logging.getLogger('cogdb.query')
        found = []
        cell_column = cog.sheets.Column('D')
        first_prep = cog.sheets.column_to_index(str(cell_column))
        first_system = cog.sheets.column_to_index(self.system_col)
        order = 1

        try:
            for col in self.cells[first_prep:first_system]:
                log.debug('PSYSSCAN - Cells: %s', str(col[0:10]))
                kwargs = kwargs_fort_system(col, order, str(cell_column))
                kwargs['id'] = 1000 + order
                log.debug('PSYSSCAN - Kwargs: %s', str(kwargs))
                order = order + 1
                cell_column.next()

                if kwargs['name'] == 'TBA':
                    continue

                found.append(PrepSystem(**kwargs))
                log.info('PSYSSCAN - System Added: %s', found[-1])
        except cog.exc.SheetParsingError:
            pass

        return found

    def merits(self, systems, users):
        """
        Scan the fortification area of the sheet and return Drop objects representing
        merits each user has dropped in System.

        Args:
            systems: The list of Systems in the order entered in the sheet.
            users: The list of Users in order the order entered in the sheet.
        """
        log = logging.getLogger('cogdb.query')
        found = []

        cnt = 1
        for system in systems:
            sys_ind = cog.sheets.column_to_index(system.sheet_col)
            try:
                for user in users:
                    amount = self.cells[sys_ind][user.row - 1]

                    if isinstance(amount, type('')):
                        amount = amount.strip()
                    if amount == '':  # Some rows just placeholders if empty
                        continue

                    found.append(Drop(id=cnt, user_id=user.id, system_id=system.id,
                                      amount=cogdb.schema.parse_int(amount)))
                    cnt += 1
                    log.info('DROPSCAN - Adding: %s', found[-1])
            except IndexError:
                pass  # No more amounts in column

        return found

    def find_system_column(self):
        """
        Find the first column that has a system cell in it.
        Determined based on cell's background color.

        Raises:
            SheetParsingError when fails to locate expected anchor in cells.
        """
        if not self.cells:
            raise cog.exc.SheetParsingError("No cells set to parse.")

        col_count = cog.sheets.Column()
        for column in self.cells:
            if 'Frey' in column:
                return str(col_count)

            col_count.next()

        raise cog.exc.SheetParsingError("Unable to determine system column.")

    def update_drop(self, system_col, user_row, amount):
        """
        Update a drop to the sheet.
        """
        cell_range = '!{col}{row}:{col}{row}'.format(col=system_col, row=user_row)
        self.gsheet.update(cell_range, [[amount]])

    def update_system(self, col, fort_status, um_status):
        """
        Update the system column of the sheet.
        """
        cell_range = '!{col}{start}:{col}{end}'.format(col=col, start=6, end=7)
        self.gsheet.update(cell_range, [[fort_status, um_status]], dim='COLUMNS')


class UMScanner(SheetScanner):
    """
    Scanner for the Hudson undermine sheet.

    args:
        gsheet: Either a dictionary to create a GSheet from or a premade GSheet.
    """
    def __init__(self, gsheet):
        super().__init__(gsheet, (SheetUM, EFaction.hudson), [Hold, SystemUM, SheetUM])
        # These are fixed based on current format
        self.system_col = 'D'
        self.user_col = 'B'
        self.user_row = 14

    def scan(self):
        """
        Main function, scan the sheet into the database.
        """
        self.cells = self.gsheet.whole_sheet()

        systems = self.systems()
        users = self.users(*self.users_args, first_id=1001)
        merits = self.merits(systems, users)

        session = cogdb.Session()
        self.drop_entries(session)
        session.commit()
        session.add_all(systems + users)
        session.commit()
        session.add_all(merits)
        session.commit()

        return True

    def systems(self):
        """
        Scan all the systems in the sheet.
        A UM System takes up two adjacent columns.
        """
        log = logging.getLogger('cogdb.query')
        cell_column = cog.sheets.Column(self.system_col)

        found = []
        try:
            cnt = 1
            while True:
                col = cog.sheets.column_to_index(str(cell_column))
                log.debug('UMSYSSCAN - Cells: %s', str(col))
                kwargs = kwargs_um_system(self.cells[col:col + 2], str(cell_column))
                kwargs['id'] = cnt
                cnt += 1
                log.debug('UMSYSSCAN - Kwargs: %s', str(kwargs))

                cls = kwargs.pop('cls')
                found.append(cls(**kwargs))
                log.info('UMSYSSCAN - System Added: %s', found[-1])
                cell_column.offset(2)
        except cog.exc.SheetParsingError:
            pass

        return found

    def held_merits(self, systems, users, holds):
        """
        Parse the held merits that fall under the same column as System.

        Args:
            systems: The SystemUMs parsed from sheet.
            users: The SheetRows parsed from sheet.
            holds: The partially finished Holds.
        """
        log = logging.getLogger('cogdb.query')

        for system in systems:
            col_ind = cog.sheets.column_to_index(system.sheet_col)

            try:
                cnt = len(holds) + 1
                for user in users:
                    held = self.cells[col_ind][user.row - 1]

                    if isinstance(held, type('')):
                        held = held.strip()
                    if held == '':
                        continue

                    key = '{}_{}'.format(system.id, user.id)
                    hold = holds.get(key, Hold(id=cnt, user_id=user.id, system_id=system.id,
                                               held=0, redeemed=0))
                    if hold.id == cnt:
                        cnt += 1
                    hold.held += cogdb.schema.parse_int(held)

                    holds[key] = hold
                    log.info('HOLDSCAN - Held merits: %s %s', key, hold)

            except IndexError:
                pass  # No more in column

        return holds

    def redeemed_merits(self, systems, users, holds):
        """
        Parse the redeemed merits that fall under the same column as System.

        Args:
            systems: The SystemUMs parsed from sheet.
            users: The SheetRows parsed from sheet.
            holds: The partially finished Holds.
        """
        log = logging.getLogger('cogdb.query')
        for system in systems:
            col_ind = cog.sheets.column_to_index(system.sheet_col) + 1

            try:
                cnt = len(holds) + 1
                for user in users:
                    redeemed = self.cells[col_ind][user.row - 1]

                    if isinstance(redeemed, type('')):
                        redeemed = redeemed.strip()
                    if redeemed == '':
                        continue

                    key = '{}_{}'.format(system.id, user.id)
                    hold = holds.get(key, Hold(id=cnt, user_id=user.id, system_id=system.id,
                                               held=0, redeemed=0))
                    if hold.id == cnt:
                        cnt += 1
                    hold.redeemed += cogdb.schema.parse_int(redeemed)

                    holds[key] = hold
                    log.info('HOLDSCAN - Redeemed merits: %s %s', key, hold)
            except IndexError:
                pass  # No more in column

        return holds

    def merits(self, systems, users):
        """
        Scan the merits in the held/redeemed area.

        Args:
            systems: The list of Systems in the order entered in the sheet.
            users: The list of Users in order the order entered in the sheet.
        """
        holds = {}
        self.held_merits(systems, users, holds)
        self.redeemed_merits(systems, users, holds)
        return list(holds.values())

    # Calls to modify the sheet All asynchronous, register them as futures and move on.
    def update_hold(self, system_col, user_row, held, redeemed):
        """
        Update a hold on the sheet.
        """
        col2 = cog.sheets.Column(system_col).next()
        cell_range = '!{col1}{row1}:{col2}{row2}'.format(col1=system_col, col2=col2,
                                                         row1=user_row, row2=user_row + 1)
        self.gsheet.update(cell_range, [[held, redeemed]])

    def update_system(self, col, progress_us, progress_them, map_offset):
        """
        Update the system column of the sheet.
        """
        cell_range = '!{col}{start}:{col}{end}'.format(col=col, start=10, end=13)
        self.gsheet.update(cell_range, [[progress_us, progress_them, 'Hold Merits', map_offset]], dim='COLUMNS')


class KOSScanner(SheetScanner):
    """
    Scanner for the Hudson KOS sheet.

    args:
        gsheet: Either a dictionary to create a GSheet from or a premade GSheet.
    """
    def __init__(self, gsheet):
        super().__init__(gsheet, (SheetUM, EFaction.hudson), [Hold, SystemUM, SheetUM])

    def scan(self):
        """
        Main function, scan the sheet into the database.
        """
        self.cells = self.gsheet.whole_sheet(dim='ROWS')
        kos_rows = self.parse_rows()
        print(str(kos_rows))

        session = cogdb.Session()
        self.drop_entries(session)
        session.commit()
        session.add_all(kos_rows)
        session.commit()

        return True

    def parse_rows(self):
        rows = []
        row = 1  # row 0 is header

        try:
            while self.cells[row][0] != '':
                data = self.cells[row]
                try:
                    danger = int(data[2])
                except ValueError:
                    danger = 1
                is_friendly = str(data[3]).lower()[0].startswith('F')
                rows += [cogdb.schema.KOS(id=row, cmdr=data[0], faction=data[1], danger=danger,
                                          is_friendly=is_friendly)]

                row += 1
        except IndexError:
            pass

        return rows

    def update_whole_sheet(self, entries):
        """
        Update the whole KOS sheet, pass in queried KOS objects.
        """
        cell_range = '!A2:D{}'.format(1 + len(entries))
        entries = [[ent.cmdr, ent.faction, str(ent.danger), ent.friendly_output] for ent in entries]
        self.gsheet.update(cell_range, entries)


def um_find_system(session, system_name):
    """
    Find the SystemUM with system_name
    """
    try:
        return session.query(SystemUM).filter_by(name=system_name).one()
    except (sqla_oexc.NoResultFound, sqla_oexc.MultipleResultsFound):
        systems = session.query(SystemUM).all()
        return fuzzy_find(system_name, systems, 'name')


def um_get_systems(session, exclude_finished=True):
    """
    Return a list of all current undermining targets.

    kwargs:
        finished: Return just the finished targets.
    """
    systems = session.query(SystemUM).all()
    if exclude_finished:
        systems = [system for system in systems if not system.is_undermined]

    return systems


def um_reset_held(session, user):
    """
    Reset all held merits to 0.
    """
    holds = session.query(Hold).filter_by(user_id=user.id).all()
    for hold in holds:
        hold.held = 0

    session.commit()
    return holds


def um_redeem_merits(session, user):
    """
    Redeem all held merits for user.
    """
    total = 0
    holds = session.query(Hold).filter_by(user_id=user.id).all()
    for hold in holds:
        total += hold.held
        hold.redeemed = hold.redeemed + hold.held
        hold.held = 0

    session.commit()
    return (holds, total)


def um_add_hold(session, **kwargs):
    """
    Add or update the user's Hold, that is their UM merits held or redeemed.
        System.name and SUser.name
    If Hold exists, increment the held value. Otherwise add it to database.

    Returns: The Hold object.

    Raises:
        InvalidCommandArgs: Hold cannot be negative.
    """
    system = kwargs['system']
    user = kwargs['user']
    held = kwargs['held']

    if held < 0:
        raise cog.exc.InvalidCommandArgs('Hold amount must be in range [0, \u221E]')

    try:
        hold = session.query(Hold).filter_by(user_id=user.id,
                                             system_id=system.id).one()
    except sqla_oexc.NoResultFound:
        hold = Hold(user_id=user.id, system_id=system.id, held=0, redeemed=0)
        session.add(hold)

    hold.held = held
    session.commit()

    return hold


def um_all_held_merits(session):
    """
    Return a list of lists that show all users with merits still held.

    List of the form:
    [
        [CMDR, system_name_1, system_name_2, ...],
        [cmdrname, merits_system_1, merits_system_2, ...],
        [cmdrname, merits_system_1, merits_system_2, ...],
    ]
    """
    c_dict = {}
    for merit in session.query(Hold).filter(Hold.held > 0).order_by(Hold.system_id).all():
        try:
            c_dict[merit.user.name][merit.system.name] = merit
        except KeyError:
            c_dict[merit.user.name] = {merit.system.name: merit}

    systems = session.query(SystemUM).order_by(SystemUM.id).all()
    system_names = [sys.name for sys in systems]
    rows = []
    for cmdr in c_dict:
        row = [cmdr]
        for system_name in system_names:
            try:
                row += [c_dict[cmdr][system_name].held]
            except KeyError:
                row += [0]

        rows += [row]

    return [['CMDR'] + system_names] + rows


def get_admin(session, member):
    """
    If the member is an admin, return the Admin.
    Otherwise, raise NoMatch.
    """
    try:
        return session.query(Admin).filter_by(id=member.id).one()
    except sqla_oexc.NoResultFound:
        raise cog.exc.NoMatch(member.display_name, 'Admin')


def add_admin(session, member):
    """
    Add a new admin.
    """
    try:
        session.add(Admin(id=member.id))
        session.commit()
    except (sqla_exc.IntegrityError, sqla_oexc.FlushError):
        raise cog.exc.InvalidCommandArgs("Member {} is already an admin.".format(member.display_name))


def add_channel_perm(session, cmd, server_name, channel_name):
    try:
        session.add(ChannelPerm(cmd=cmd, server=server_name, channel=channel_name))
        session.commit()
    except (sqla_exc.IntegrityError, sqla_oexc.FlushError):
        raise cog.exc.InvalidCommandArgs("Channel permission already exists.")


def add_role_perm(session, cmd, server_name, role_name):
    try:
        session.add(RolePerm(cmd=cmd, server=server_name, role=role_name))
        session.commit()
    except (sqla_exc.IntegrityError, sqla_oexc.FlushError):
        raise cog.exc.InvalidCommandArgs("Role permission already exists.")


def remove_channel_perm(session, cmd, server_name, channel_name):
    try:
        session.delete(session.query(ChannelPerm).
                       filter_by(cmd=cmd, server=server_name, channel=channel_name).one())
        session.commit()
    except sqla_oexc.NoResultFound:
        raise cog.exc.InvalidCommandArgs("Channel permission does not exist.")


def remove_role_perm(session, cmd, server_name, role_name):
    try:
        session.delete(session.query(RolePerm).
                       filter_by(cmd=cmd, server=server_name, role=role_name).one())
        session.commit()
    except sqla_oexc.NoResultFound:
        raise cog.exc.InvalidCommandArgs("Role permission does not exist.")


def check_perms(msg, args):
    """
    Check if a user is authorized to issue this command.
    Checks will be made against channel and user roles.

    Raises InvalidPerms if any permission issue.
    """
    session = cogdb.Session()
    check_channel_perms(session, args.cmd, msg.channel.server.name, msg.channel.name)
    check_role_perms(session, args.cmd, msg.channel.server.name, msg.author.roles)


def check_channel_perms(session, cmd, server_name, channel_name):
    """
    A user is allowed to issue a command if:
        a) no restrictions for the cmd
        b) the channel is whitelisted in the restricted channels

    Raises InvalidPerms if fails permission check.
    """
    channels = [perm.channel for perm in session.query(ChannelPerm).
                filter_by(cmd=cmd, server=server_name)]
    if channels and channel_name not in channels:
        raise cog.exc.InvalidPerms("The '{}' command is not permitted on this channel.".format(
            cmd.lower()))


def check_role_perms(session, cmd, server_name, member_roles):
    """
    A user is allowed to issue a command if:
        a) no roles set for the cmd
        b) he matches ANY of the set roles

    Raises InvalidPerms if fails permission check.
    """
    perm_roles = set([perm.role for perm in session.query(RolePerm).
                      filter_by(cmd=cmd, server=server_name)])
    member_roles = set([role.name for role in member_roles])
    if perm_roles and len(member_roles - perm_roles) == len(member_roles):
        raise cog.exc.InvalidPerms("You do not have the roles for the command.")


def complete_control_name(partial, include_winters=False):
    """
    Provide name completion of Federal controls without db query.
    """
    systems = HUDSON_CONTROLS[:]
    if include_winters:
        systems += WINTERS_CONTROLS

    return fuzzy_find(partial, systems)


def kos_add_cmdr(session, terms):
    """
    Add a kos cmdr to the database.

    Raises:
        InvalidCommandArgs, ValueError - Problem with input values.
    """
    try:
        danger = int(terms[2])
        if danger < 0 or danger > 10:
            raise cog.exc.InvalidCommandArgs("KOS: Danger should be [0, 10].")
        is_friendly = int(terms[3])
        # TODO: Map kill -> 0, friendly -> 1, perhaps using k/h startswith
        if is_friendly not in [0, 1]:
            raise cog.exc.InvalidCommandArgs("KOS: 0 == hostile or 1 == friendly")

        new_kos = KOS(cmdr=terms[0], faction=terms[1], danger=danger, is_friendly=is_friendly)
    except IndexError:
        raise cog.exc.InvalidCommandArgs("KOS: Insufficient terms for the kos command, see help.")
    except ValueError:
        raise cog.exc.InvalidCommandArgs("KOS: Check the command help.")

    session.add(new_kos)
    session.commit()

    return new_kos


def kos_search_cmdr(session, term):
    """
    Search for a kos entry for cmdr.
    """
    term = '%' + str(term) + '%'
    return session.query(KOS).filter(KOS.cmdr.ilike(term)).all()
