"""
Test the schema for the database.
"""
from __future__ import absolute_import, print_function
import copy
import datetime

import decorator
import mock
import pytest

import cog.exc
import cogdb
import cogdb.schema
from cogdb.schema import (DUser, System, Drop, Hold, Command,
                          SheetRow, HudsonCattle, HudsonUM,
                          SystemUM, UMControl, UMExpand, UMOppose,
                          EFaction, ESheetType, kwargs_um_system, kwargs_fort_system)

from tests.cogdb import CELLS, FMT_CELLS, UM_CELLS

SYSTEMS_DATA = [
    ['', 1, 4910, 0, 4322, 4910, 0, 116.99, '', 'Frey'],
    ['', 1, 8425, 0, 3844, 5422, 0, 99.51, '', 'Nurundere'],
    ['', 1, 5974, 0, 0, 0, 0, 80, '', 'LHS 3749'],
    ['', 1, 5211, 0, 0, 2500, 0, 75, '', 'Sol'],
    ['', 1, 7239, 0, 0, 0, 0, 102.4, '', 'Dongkum'],
]
SYSTEMSUM_DATA = [
    [
        ['', 0, 0, 14878, 13950, -452, 'Sec: Medium', 'Sol', 'Cemplangpa', 13830, 1, 0, 1380],
        [0, 0, 0, 0, 0, 0, ''],
    ],
    [
        ['Exp', 0, 0, 364298, 160472, 127278, 'Sec: Low', 'Dongkum', 'Burr', 161630, 35, 0, 76548],
        [0, 0, 0, 0, 0, 0, ''],
    ],
    [
        ['Opp', 0, 0, 59877, 10470, 12147, 'Sec: Low', 'Atropos', 'AF Leopris', 47739, 1.69, 0, 23960],
        [0, 0, 0, 0, 0, 0, ''],
    ],
]
SYSTEMUM_EXPAND = [
    ['Exp', 0, 0, 364298, 160472, 127278, 'Sec: Low', 'Dongkum', 'Burr', 161630, 35, 0, 76548],
    [0, 0, 0, 0, 0, 0, ''],
]
SYSTEMUM_OPPOSE = [
    ['Opp', 0, 0, 59877, 10470, 12147, 'Sec: Low', 'Atropos', 'AF Leopris', 47739, 1.69, 0, 23960],
    [0, 0, 0, 0, 0, 0, ''],
]
SYSTEMUM_CONTROL = [
    ['', 0, 0, 14878, 13950, -452, 'Sec: Medium', 'Unknown', 'Cemplangpa', 13830, 1, 0, 1380],
    [0, 0, 0, 0, 0, 0, ''],
]


def db_cleanup(function):
    """
    Clean the whole database. Guarantee it is empty.
    """
    def wrapper(function, *args, **kwargs):
        try:
            function(*args, **kwargs)
        finally:
            cogdb.schema.drop_tables(all=True)
            session = cogdb.Session()

            classes = [DUser, SheetRow, System, SystemUM, Drop, Hold, Command]
            for cls in classes:
                assert session.query(cls) == []

    return decorator.decorator(wrapper, function)


@pytest.fixture
def mock_sheet():
    fake_sheet = mock.Mock()
    fake_sheet.whole_sheet.return_value = CELLS
    fake_sheet.get_with_formatting.return_value = FMT_CELLS

    yield fake_sheet


@pytest.fixture
def mock_umsheet():
    fake_sheet = mock.Mock()
    fake_sheet.whole_sheet.return_value = UM_CELLS

    yield fake_sheet


@pytest.fixture
def f_dusers():
    """
    Fixture to insert some test DUsers.
    """
    session = cogdb.Session()
    dusers = (
        DUser(discord_id='1000', display_name='GearsandCogs',
              capacity=750, pref_name='GearsandCogs', faction=EFaction.hudson),
        DUser(discord_id='1001', display_name='rjwhite',
              capacity=450, pref_name='rjwhite', faction=EFaction.hudson),
        DUser(discord_id='1002', display_name='vampyregtx',
              capacity=700, pref_name='not_vamp', faction=EFaction.hudson),
    )
    session.add_all(dusers)
    session.commit()

    yield dusers

    session.query(DUser).delete()


@pytest.fixture
def f_sheets():
    """
    Fixture to insert some test SheetRows.

    Depends on: f_dusers
    """
    session = cogdb.Session()
    dusers = session.query(DUser).all()
    assert dusers

    sheets = (
        HudsonCattle(name=dusers[0].pref_name, row=15, cry='Gears are forting late!'),
        HudsonUM(name=dusers[0].pref_name, row=18, cry='Gears are pew pew!'),
        HudsonCattle(name=dusers[1].pref_name, row=16, cry=''),
        HudsonUM(name=dusers[1].pref_name, row=19, cry='Shooting time'),
        HudsonCattle(name=dusers[2].pref_name, row=17, cry='Vamp the boss'),
    )
    session.add_all(sheets)
    session.commit()

    yield sheets

    session.query(SheetRow).delete()


@pytest.fixture
def f_commands():
    """
    Fixture to insert some test Commands.

    Depends on: f_dusers
    Last element of fixture is dtime of insertions.
    """
    session = cogdb.Session()
    dusers = session.query(DUser).all()
    assert dusers

    dtime = datetime.datetime.now()
    commands = (
        Command(discord_id=dusers[0].discord_id, cmd_str='drop 400', date=dtime),
        Command(discord_id=dusers[0].discord_id, cmd_str='drop 700', date=dtime),
        Command(discord_id=dusers[0].discord_id, cmd_str='user', date=dtime),
    )
    session.add_all(commands)
    session.commit()

    yield commands + (dtime,)

    session.query(Command).delete()


@pytest.fixture
def f_systems():
    """
    Fixture to insert some test Systems.
    """
    session = cogdb.Session()

    order = 1
    column = 'F'
    systems = []
    for data in SYSTEMS_DATA:
        systems.append(System(**kwargs_fort_system(data, order, column)))
        order += 1
        column = chr(ord(column) + 1)
    session.add_all(systems)
    session.commit()

    yield systems

    session.query(System).delete()


@pytest.fixture
def f_drops():
    """
    Fixture to insert some test Drops.

    Depends on: f_sheets, f_systems
    """
    session = cogdb.Session()
    users = session.query(HudsonCattle).all()
    systems = session.query(System).all()

    drops = (
        Drop(amount=700, user_id=users[0].id, system_id=systems[0].id),
        Drop(amount=400, user_id=users[0].id, system_id=systems[1].id),
        Drop(amount=1200, user_id=users[1].id, system_id=systems[0].id),
        Drop(amount=1800, user_id=users[2].id, system_id=systems[0].id),
        Drop(amount=800, user_id=users[1].id, system_id=systems[1].id),
    )
    session.add_all(drops)
    session.commit()

    yield drops

    session.query(Drop).delete()


@pytest.fixture
def f_systemsum():
    """
    Fixture to insert some test Systems.
    """
    session = cogdb.Session()

    column = 'D'
    systems = []
    for data in SYSTEMSUM_DATA[:1]:
        systems.append(SystemUM.factory(kwargs_um_system(data, column)))
        column = chr(ord(column) + 2)
    session.add_all(systems)
    session.commit()

    yield systems

    session.query(SystemUM).delete()


@pytest.fixture
def f_holds():
    """
    Fixture to insert some test Holds.

    Depends on: f_sheets, f_systemsum
    """
    session = cogdb.Session()
    users = session.query(HudsonUM).all()
    systems = session.query(SystemUM).all()

    drops = (
        Hold(held=0, redeemed=4000, user_id=users[1].id, system_id=systems[0].id),
        Hold(held=400, redeemed=1550, user_id=users[1].id, system_id=systems[0].id),
        Hold(held=2200, redeemed=5800, user_id=users[1].id, system_id=systems[0].id),
        # Hold(held=450, redeemed=2000, user_id=users[0].id, system_id=systems[0].id),
        # Hold(held=2400, redeemed=0, user_id=users[0].id, system_id=systems[1].id),
        # Hold(held=0, redeemed=1200, user_id=users[1].id, system_id=systems[0].id),
    )
    session.add_all(drops)
    session.commit()

    yield drops

    session.query(Hold).delete()


# def db_data(function):
    # """
    # Wrap a test and setup database with dummy data.
    # """
    # def wrapper(function, *args, **kwargs):
        # session = cogdb.Session()
        # mock_sheet = mock.Mock()
        # mock_sheet.whole_sheet.return_value = CELLS
        # mock_sheet.get_with_formatting.return_value = FMT_CELLS
        # scanner = cogdb.query.FortScanner(mock_sheet)
        # scanner.scan(session)

        # duser = DUser(discord_id='1111', display_name='GearsandCogs',
                                   # capacity=0, pref_name='GearsandCogs')
        # cmd = Command(discord_id=duser.discord_id,
                                   # cmd_str='drop 700', date=datetime.datetime.now())
        # session.add(cmd)
        # session.add(duser)
        # session.commit()

        # function(*args, **kwargs)

    # return decorator.decorator(wrapper, function)


def test_drop_tables_all(f_dusers, f_sheets, f_systems, f_drops, f_systemsum, f_holds):
    session = cogdb.Session()

    classes = [DUser, SheetRow, System, SystemUM, Drop, Hold]
    for cls in classes:
        assert session.query(cls).all()
    cogdb.schema.drop_tables(all=True)
    for cls in classes:
        assert session.query(cls).all() == []


def test_drop_scanned_tables(f_dusers, f_sheets, f_systems, f_drops, f_systemsum, f_holds):
    session = cogdb.Session()

    classes = [DUser, SheetRow, System, SystemUM, Drop, Hold]
    for cls in classes:
        assert session.query(cls).all()
    cogdb.schema.drop_tables(all=False)

    classes.remove(DUser)
    for cls in classes:
        assert session.query(cls).all() == []
    assert session.query(DUser).all()


def test_duser__eq__(f_dusers, f_sheets):
    duser = f_dusers[0]
    assert duser != DUser(discord_id='1111', display_name='test user',
                          pref_name='test user')
    assert duser == DUser(discord_id=duser.discord_id, display_name='test user',
                          pref_name='test user')


def test_duser__repr__(f_dusers, f_sheets):
    duser = f_dusers[0]
    assert repr(duser) == "DUser(discord_id='1000', display_name='GearsandCogs', "\
                          "pref_name='GearsandCogs', faction='hudson', capacity=750)"
    assert duser == eval(repr(duser))


def test_duser__str__(f_dusers, f_sheets):
    duser = f_dusers[0]
    assert str(duser) == "DUser(discord_id='1000', display_name='GearsandCogs', "\
                          "pref_name='GearsandCogs', faction='hudson', capacity=750)"


def test_duser_get_sheet(f_dusers, f_sheets):
    duser = f_dusers[0]
    assert not duser.get_sheet('winters_um')
    assert duser.get_sheet('hudson_cattle')
    assert isinstance(duser.get_sheet(ESheetType.hudson_cattle), HudsonCattle)


def test_duser_cattle(f_dusers, f_sheets):
    duser = f_dusers[0]
    session = cogdb.Session()
    cattle = session.query(HudsonCattle).filter(HudsonCattle.name == duser.pref_name).one()

    assert duser.cattle == cattle
    assert isinstance(duser.cattle, HudsonCattle)
    duser.switch_faction()
    assert not duser.cattle


def test_duser_undermine(f_dusers, f_sheets):
    duser = f_dusers[0]
    session = cogdb.Session()
    undermine = session.query(HudsonUM).filter(HudsonUM.name == duser.pref_name).one()

    assert duser.undermine == undermine
    assert isinstance(duser.undermine, HudsonUM)
    duser.switch_faction()
    assert not duser.undermine


def test_duser_switch_faction(f_dusers, f_sheets):
    duser = f_dusers[0]

    assert duser.faction == EFaction.hudson
    duser.switch_faction()
    assert duser.faction == EFaction.winters
    duser.switch_faction(EFaction.hudson)
    assert duser.faction == EFaction.hudson


def test_sheetrow__eq__(f_dusers, f_sheets):
    sheet = f_sheets[0]
    equal = HudsonCattle(type='hudson_cattle', name='GearsandCogs', row=15,
                         cry='Gears are forting late!')
    assert sheet == equal
    equal.name = 'notGears'
    assert sheet != equal


def test_sheetrow__repr__(f_dusers, f_sheets):
    sheet = f_sheets[0]
    assert repr(sheet) == "HudsonCattle(type='hudson_cattle', name='GearsandCogs', "\
                          "row=15, cry='Gears are forting late!')"

    assert sheet == eval(repr(sheet))


def test_sheetrow__str__(f_dusers, f_sheets):
    sheet = f_sheets[0]
    assert str(sheet) == "id=1, HudsonCattle(type='hudson_cattle', name='GearsandCogs', "\
                         "row=15, cry='Gears are forting late!')"


def test_system__eq__(f_systems):
    system = f_systems[0]

    assert system == System(name='Frey')
    assert system != System(name='Sol')


def test_system__repr__(f_systems):
    system = f_systems[0]

    assert repr(system) == "System(name='Frey', cmdr_merits=4322, fort_status=4910, "\
                           "trigger=4910, um_status=0, undermine=0.0, distance=116.99, "\
                           "notes='', sheet_col='F', sheet_order=1)"
    assert system == eval(repr(system))


def test_system__str__(f_systems):
    system = f_systems[0]

    assert str(system) == "id=1, System(name='Frey', cmdr_merits=4322, fort_status=4910, "\
                          "trigger=4910, um_status=0, undermine=0.0, distance=116.99, "\
                          "notes='', sheet_col='F', sheet_order=1)"


def test_system_short_display(f_systems):
    system = f_systems[0]
    assert system.short_display() == 'Frey :Fortified: 4910/4910'

    system.fort_status = 4000
    system.cmdr_merits = 4000
    assert system.short_display() == 'Frey :Fortifying: 4000/4910\nMissing: 910'
    assert system.short_display(missing=False) == 'Frey :Fortifying: 4000/4910'


def test_system_set_status(f_systems):
    system = f_systems[0]
    assert system.fort_status == 4910
    assert system.um_status == 0

    system.set_status('4000')
    assert system.fort_status == 4000
    assert system.um_status == 0

    system.set_status('2200:2000')
    assert system.fort_status == 2200
    assert system.um_status == 2000


def test_system_current_status(f_systems):
    system = f_systems[0]
    assert system.current_status == 4910


def test_system_skip(f_systems):
    system = f_systems[0]
    assert system.skip is False

    system.notes = 'Leave for now.'
    assert system.skip is True


def test_system_is_fortified(f_systems):
    system = f_systems[0]
    system.fort_status = system.trigger
    assert system.is_fortified is True

    system.fort_status = system.fort_status // 2
    assert system.is_fortified is False


def test_system_is_undermined(f_systems):
    system = f_systems[0]

    system.undermine = 1.0
    assert system.is_undermined is True

    system.undermine = 0.4
    assert system.is_undermined is False


def test_system_missing(f_systems):
    system = f_systems[0]
    system.cmdr_merits = 0

    system.fort_status = system.trigger - 1000
    assert system.missing == 1000

    system.fort_status = system.trigger
    assert system.missing == 0

    system.fort_status = system.trigger + 1000
    assert system.missing == 0


def test_system_completion(f_systems):
    system = f_systems[0]
    assert system.completion == '100.0'
    system.trigger = 0
    assert system.completion == '0.0'


def test_system_table_row(f_systems):
    system = f_systems[0]
    system.notes = 'Leave'
    assert system.table_row == ('Frey', '   0', '4910/4910 (100.0%/0.0%)', 'Leave')


def test_drop__eq__(f_dusers, f_sheets, f_systems, f_drops):
    user = f_sheets[0]
    system = f_systems[0]
    drop = f_drops[0]
    assert drop == Drop(amount=700, user_id=user.id,
                                     system_id=system.id)
    assert drop.user == user
    assert drop.system == system


def test_drop__repr__(f_dusers, f_sheets, f_systems, f_drops):
    drop = f_drops[0]
    assert repr(drop) == "Drop(system_id=1, user_id=1, amount=700)"
    assert drop == eval(repr(drop))


def test_drop__str__(f_dusers, f_sheets, f_systems, f_drops):
    drop = f_drops[0]
    assert str(drop) == "id=1, system_name='Frey', user_name='GearsandCogs', "\
                        "Drop(system_id=1, user_id=1, amount=700)"


def test_system_um__repr__(f_dusers, f_sheets, f_systemsum, f_holds):
    system = f_systemsum[0]

    assert repr(system) == "SystemUM(name='Cemplangpa', type='control', sheet_col='D', "\
            "goal=14878, security='Medium', notes='', "\
            "progress_us=13830, progress_them=1.0, "\
            "close_control='Sol', map_offset=1380)"
    assert system == eval(repr(system))


def test_system_um__str__(f_dusers, f_sheets, f_systemsum, f_holds):
    system = f_systemsum[0]

    assert str(system) == "Type: Control, Name: Cemplangpa\n"\
                          "Completion: 103%, Missing: -452\n"\
                          "Security: Medium, Close Control: Sol"

def test_system_um__eq__(f_dusers, f_sheets, f_systemsum, f_holds):

    expect = SystemUM.factory(kwargs_um_system(SYSTEMSUM_DATA[0], 'F'))
    system = f_systemsum[0]

    assert system == expect


def test_system_um_cmdr_merits(f_dusers, f_sheets, f_systemsum, f_holds):
    system = f_systemsum[0]

    assert system.cmdr_merits == 13950


def test_system_um_missing(f_dusers, f_sheets, f_systemsum, f_holds):
    system = f_systemsum[0]

    system.progress_us = 0
    assert system.missing == -452

    system.progress_us = 15000
    system.map_offset = 0
    assert system.missing == -122


def test_system_um_completion(f_dusers, f_sheets, f_systemsum, f_holds):
    system = f_systemsum[0]

    assert system.completion == '103%'


def test_kwargs_system_um():
    expect = {
        'close_control': 'Dongkum',
        'goal': 364298,
        'name': 'Burr',
        'notes': '',
        'progress_us': 161630,
        'progress_them': 35.0,
        'security': 'Low',
        'sheet_col': 'D',
        'cls': UMExpand,
        'map_offset': 76548,
    }
    sys_cols = copy.deepcopy(SYSTEMUM_EXPAND)
    assert cogdb.schema.kwargs_um_system(sys_cols, 'D') == expect

    sys_cols[0][0] = 'Opp.'
    expect['cls'] = UMOppose
    assert cogdb.schema.kwargs_um_system(sys_cols, 'D') == expect

    sys_cols[0][0] = ''
    expect['cls'] = UMControl
    assert cogdb.schema.kwargs_um_system(sys_cols, 'D') == expect

    with pytest.raises(cog.exc.SheetParsingError):
        cogdb.schema.kwargs_um_system([], 'D')


def test_kwargs_fort_system():
    expect = {
        'cmdr_merits': 4322,
        'distance': 116.99,
        'fort_status': 4910,
        'name': 'Frey',
        'notes': '',
        'sheet_col': 'F',
        'sheet_order': 1,
        'trigger': 4910,
        'um_status': 0,
        'undermine': 0.0,
    }
    assert cogdb.schema.kwargs_fort_system(SYSTEMS_DATA[0], 1, 'F') == expect

    with pytest.raises(cog.exc.SheetParsingError):
        cogdb.schema.kwargs_fort_system(['' for _ in range(0, 10)], 1, 'A')

    with pytest.raises(cog.exc.SheetParsingError):
        cogdb.schema.kwargs_fort_system([], 1, 'A')

    with pytest.raises(cog.exc.SheetParsingError):
        cogdb.schema.kwargs_fort_system(SYSTEMS_DATA[0][:-2], 1, 'A')


def test_parse_int():
    assert cogdb.schema.parse_int('') == 0
    assert cogdb.schema.parse_int('2') == 2
    assert cogdb.schema.parse_int(5) == 5


def test_parse_float():
    assert cogdb.schema.parse_float('') == 0.0
    assert cogdb.schema.parse_float('2') == 2.0
    assert cogdb.schema.parse_float(0.5) == 0.5
