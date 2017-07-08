"""
Test the schema for the database.
"""
from __future__ import absolute_import, print_function
import copy
import datetime

import mock
import pytest

import cog.exc
import cogdb
import cogdb.schema
from cogdb.schema import (DUser, System, Drop, Hold, Command,
                          SheetRow, SheetCattle, SheetUM,
                          SystemUM, UMControl, UMExpand, UMOppose,
                          EFaction, ESheetType, kwargs_um_system, kwargs_fort_system)

from tests.data import SYSTEMS_DATA, SYSTEMSUM_DATA, SYSTEMUM_EXPAND


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


def test_drop_tables_all(session, f_dusers, f_sheets, f_systems, f_drops, f_systemsum, f_holds):
    classes = [DUser, SheetRow, System, SystemUM, Drop, Hold]
    for cls in classes:
        assert session.query(cls).all()
    cogdb.schema.drop_tables(all=True)
    for cls in classes:
        assert session.query(cls).all() == []


def test_drop_scanned_tables(session, f_dusers, f_sheets, f_systems, f_drops, f_systemsum, f_holds):
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
    assert duser != DUser(id='1111', display_name='test user',
                          pref_name='test user')
    assert duser == DUser(id=duser.id, display_name='test user',
                          pref_name='test user')


def test_duser__repr__(f_dusers, f_sheets):
    duser = f_dusers[0]
    assert repr(duser) == "DUser(id='1000', display_name='GearsandCogs', "\
                          "pref_name='GearsandCogs', faction='hudson', capacity=750)"
    assert duser == eval(repr(duser))


def test_duser__str__(f_dusers, f_sheets):
    duser = f_dusers[0]
    assert str(duser) == "DUser(id='1000', display_name='GearsandCogs', "\
                          "pref_name='GearsandCogs', faction='hudson', capacity=750)"


def test_duser_get_sheet(f_dusers, f_sheets):
    duser = f_dusers[0]
    assert not duser.get_sheet('um', faction=EFaction.winters)
    assert duser.get_sheet('cattle')
    assert isinstance(duser.get_sheet(ESheetType.cattle), SheetCattle)


def test_duser_cattle(session, f_dusers, f_sheets):
    duser = f_dusers[0]
    cattle = session.query(SheetCattle).filter(SheetCattle.name == duser.pref_name).one()

    assert duser.cattle == cattle
    assert isinstance(duser.cattle, SheetCattle)
    duser.switch_faction()
    assert not duser.cattle


def test_duser_undermine(session, f_dusers, f_sheets):
    duser = f_dusers[0]
    undermine = session.query(SheetUM).filter(SheetUM.name == duser.pref_name).one()

    assert duser.undermine == undermine
    assert isinstance(duser.undermine, SheetUM)
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
    equal = SheetCattle(name='GearsandCogs', type=ESheetType.cattle, faction=EFaction.hudson,
                        row=15, cry='Gears are forting late!')
    assert sheet == equal
    equal.name = 'notGears'
    assert sheet != equal


def test_sheetrow__repr__(f_dusers, f_sheets):
    sheet = f_sheets[0]
    assert repr(sheet) == "SheetCattle(name='GearsandCogs', type='cattle', faction='hudson', "\
                          "row=15, cry='Gears are forting late!')"

    assert sheet == eval(repr(sheet))


def test_sheetrow__str__(f_dusers, f_sheets):
    sheet = f_sheets[0]
    assert str(sheet) == "id=1, SheetCattle(name='GearsandCogs', type='cattle', faction='hudson', "\
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
    assert drop == Drop(amount=700, user_id=user.id, system_id=system.id)
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
