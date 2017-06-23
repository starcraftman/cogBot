"""
Test the schema for the database.
"""
from __future__ import absolute_import, print_function
import datetime

import mock
import pytest

import cog.exc
import cogdb
import cogdb.schema

from tests.cogdb import CELLS, FMT_CELLS

SYSTEM_DATA = ['', 1, 4910, 0, 4322, 4910, 0, 116.99, '', 'Frey']


@pytest.fixture
def mock_sheet():
    fake_sheet = mock.Mock()
    fake_sheet.whole_sheet.return_value = CELLS
    fake_sheet.get_with_formatting.return_value = FMT_CELLS
    yield fake_sheet


def db_cleanup(function):
    """
    Clean the whole database. Guarantee it is empty.
    """
    def call():
        try:
            function()
        finally:
            cogdb.schema.drop_all_tables()
            session = cogdb.Session()
            assert session.query(cogdb.schema.DUser).all() == []
            assert session.query(cogdb.schema.SUser).all() == []
            assert session.query(cogdb.schema.Command).all() == []
            assert session.query(cogdb.schema.Fort).all() == []
            assert session.query(cogdb.schema.System).all() == []
    return call


def db_data(function):
    """
    Wrap a test and setup database with dummy data.
    """
    def call():
        session = cogdb.Session()
        mock_sheet = mock.Mock()
        mock_sheet.whole_sheet.return_value = CELLS
        mock_sheet.get_with_formatting.return_value = FMT_CELLS
        scanner = cogdb.query.SheetScanner(mock_sheet)
        scanner.scan(session)

        duser = cogdb.schema.DUser(discord_id='1111', display_name='GearsandCogs',
                                   capacity=0, sheet_name='GearsandCogs')
        cmd = cogdb.schema.Command(discord_id=duser.discord_id,
                                   cmd_str='drop 700', date=datetime.datetime.now())
        session.add(cmd)
        session.add(duser)
        session.commit()

        function()
    return call


@db_cleanup
@db_data
def test_drop_all_tables():
    session = cogdb.Session()
    user = cogdb.schema.SUser(sheet_name='test user', sheet_row=2)
    session.add(user)
    session.commit()

    assert session.query(cogdb.schema.DUser).all()
    assert session.query(cogdb.schema.SUser).all()
    assert session.query(cogdb.schema.Command).all()
    assert session.query(cogdb.schema.Fort).all()
    assert session.query(cogdb.schema.System).all()
    cogdb.schema.drop_all_tables()
    assert session.query(cogdb.schema.DUser).all() == []
    assert session.query(cogdb.schema.SUser).all() == []
    assert session.query(cogdb.schema.Command).all() == []
    assert session.query(cogdb.schema.Fort).all() == []
    assert session.query(cogdb.schema.System).all() == []


@db_cleanup
@db_data
def test_drop_scanned_tables():
    session = cogdb.Session()
    user = cogdb.schema.SUser(sheet_name='test user', sheet_row=2)
    session.add(user)
    session.commit()

    assert session.query(cogdb.schema.DUser).all()
    assert session.query(cogdb.schema.SUser).all()
    assert session.query(cogdb.schema.Command).all()
    assert session.query(cogdb.schema.Fort).all()
    assert session.query(cogdb.schema.System).all()
    cogdb.schema.drop_scanned_tables()
    assert session.query(cogdb.schema.DUser).all()
    assert session.query(cogdb.schema.SUser).all() == []
    assert session.query(cogdb.schema.Command).all()
    assert session.query(cogdb.schema.Fort).all() == []
    assert session.query(cogdb.schema.System).all() == []


@db_cleanup
def test_command__eq__():
    dtime = datetime.datetime.now()
    duser = cogdb.schema.DUser(discord_id='1111', display_name='test user', capacity=0,
                               sheet_name='test user')
    cmd = cogdb.schema.Command(discord_id=duser.discord_id, cmd_str='drop 400', date=dtime)

    session = cogdb.Session()
    session.add_all([duser, cmd])
    session.commit()

    assert cmd.duser == duser
    assert cmd == cogdb.schema.Command(discord_id=duser.discord_id, cmd_str='drop 400', date=dtime)


@db_cleanup
def test_command__repr__():
    dtime = datetime.datetime.now()
    duser = cogdb.schema.DUser(discord_id='1111', display_name='test user', capacity=0,
                               sheet_name='test user')
    cmd = cogdb.schema.Command(discord_id=duser.discord_id, cmd_str='drop 400', date=dtime)

    session = cogdb.Session()
    session.add_all([duser, cmd])
    session.commit()

    assert repr(cmd) == "Command(discord_id='1111', cmd_str='drop 400', date={!r})".format(dtime)
    assert cmd == eval(repr(cmd).replace('Command', 'cogdb.schema.Command'))


@db_cleanup
def test_command__str__():
    dtime = datetime.datetime.now()
    duser = cogdb.schema.DUser(discord_id='1111', display_name='test user', capacity=0,
                               sheet_name='test user')
    cmd = cogdb.schema.Command(discord_id=duser.discord_id, cmd_str='drop 400', date=dtime)

    session = cogdb.Session()
    session.add_all([duser, cmd])
    session.commit()

    assert str(cmd) == "id=1, display_name='test user', Command(discord_id='1111', "\
                       "cmd_str='drop 400', date={!r})".format(dtime)


@db_cleanup
def test_duser__eq__():
    duser = cogdb.schema.DUser(discord_id='1111', display_name='test user', capacity=0,
                               sheet_name='test user')
    suser = cogdb.schema.SUser(sheet_name='test user', sheet_row=2)

    session = cogdb.Session()
    session.add_all([suser, duser])
    session.commit()

    assert suser.duser == duser
    assert duser.suser == suser
    assert duser == cogdb.schema.DUser(discord_id='1111', display_name='test user', capacity=0,
                                       sheet_name='test user')


@db_cleanup
def test_duser__repr__():
    duser = cogdb.schema.DUser(discord_id='1111', display_name='test user', capacity=0,
                               sheet_name='test user')
    suser = cogdb.schema.SUser(sheet_name='test user', sheet_row=2)

    session = cogdb.Session()
    session.add_all([suser, duser])
    session.commit()

    assert repr(duser) == "DUser(display_name='test user', discord_id='1111', "\
                          "capacity=0, sheet_name='test user')"
    assert duser == eval(repr(duser).replace('DUser', 'cogdb.schema.DUser'))


@db_cleanup
def test_duser__str__():
    duser = cogdb.schema.DUser(discord_id='1111', display_name='test user', capacity=0,
                               sheet_name='test user')
    suser = cogdb.schema.SUser(sheet_name='test user', sheet_row=2)

    session = cogdb.Session()
    session.add_all([suser, duser])
    session.commit()

    assert str(duser) == "id=1, DUser(display_name='test user', discord_id='1111', "\
                         "capacity=0, sheet_name='test user')"


@db_cleanup
def test_suser__eq__():
    duser = cogdb.schema.DUser(discord_id='1111', display_name='test user', capacity=0,
                               sheet_name='test user')
    suser = cogdb.schema.SUser(sheet_name='test user', sheet_row=2)

    session = cogdb.Session()
    session.add_all([suser, duser])
    session.commit()

    assert suser == cogdb.schema.SUser(sheet_name='test user', sheet_row=2)
    assert suser.duser == duser
    assert duser.suser == suser


@db_cleanup
def test_suser__repr__():
    duser = cogdb.schema.DUser(discord_id='1111', display_name='test user', capacity=0,
                               sheet_name='test user')
    suser = cogdb.schema.SUser(sheet_name='test user', sheet_row=2)

    session = cogdb.Session()
    session.add_all([suser, duser])
    session.commit()

    assert repr(suser) == "SUser(sheet_name='test user', sheet_row=2)"
    assert suser == eval(repr(suser).replace('SUser', 'cogdb.schema.SUser'))


@db_cleanup
def test_suser__str__():
    duser = cogdb.schema.DUser(discord_id='1111', display_name='test user', capacity=0,
                               sheet_name='test user')
    suser = cogdb.schema.SUser(sheet_name='test user', sheet_row=2)

    session = cogdb.Session()
    session.add_all([suser, duser])
    session.commit()

    assert str(suser) == "id=1, SUser(sheet_name='test user', sheet_row=2)"


@db_cleanup
def test_suser_merits():
    suser = cogdb.schema.SUser(sheet_name='test user', sheet_row=2)
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'G')
    system2 = cogdb.schema.System(**result)
    system2.name = 'Sol'
    session = cogdb.Session()
    session.add_all([system, system2, suser])
    session.commit()

    fort = cogdb.schema.Fort(amount=400, user_id=suser.id, system_id=system.id)
    fort2 = cogdb.schema.Fort(amount=200, user_id=suser.id, system_id=system2.id)
    session = cogdb.Session()
    session.add_all([fort, fort2])
    session.commit()

    assert suser.merits == 600


@db_cleanup
def test_fort__eq__():
    user = cogdb.schema.SUser(sheet_name='test user', sheet_row=2)
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)

    session = cogdb.Session()
    session.add_all([system, user])
    session.commit()
    fort = cogdb.schema.Fort(amount=400, user_id=user.id, system_id=system.id)
    session.add(fort)
    session.commit()

    assert fort == cogdb.schema.Fort(amount=400, user_id=user.id, system_id=system.id)
    assert fort.suser == user
    assert fort.system == system


@db_cleanup
def test_fort__repr__():
    user = cogdb.schema.SUser(sheet_name='test user', sheet_row=2)
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)

    session = cogdb.Session()
    session.add_all([system, user])
    session.commit()
    fort = cogdb.schema.Fort(amount=400, user_id=user.id, system_id=system.id)
    session.add(fort)
    session.commit()

    assert repr(fort) == "Fort(user_id=1, system_id=1, amount=400)"
    assert fort == eval(repr(fort).replace('Fort', 'cogdb.schema.Fort'))


@db_cleanup
def test_fort__str__():
    user = cogdb.schema.SUser(sheet_name='test user', sheet_row=2)
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)

    session = cogdb.Session()
    session.add_all([system, user])
    session.commit()
    fort = cogdb.schema.Fort(amount=400, user_id=user.id, system_id=system.id)
    session.add(fort)
    session.commit()

    assert str(fort) == "id=1, sheet_name='test user', system_name='Frey', "\
                        "Fort(user_id=1, system_id=1, amount=400)"


@db_cleanup
def test_system__eq__():
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)

    session = cogdb.Session()
    session.add(system)
    session.commit()

    assert system == cogdb.schema.System(**result)


@db_cleanup
def test_system__repr__():
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)

    session = cogdb.Session()
    session.add(system)
    session.commit()

    assert repr(system) == "System(name='Frey', sheet_order=0, sheet_col='F', "\
                           "cmdr_merits=4322, fort_status=4910, trigger=4910, "\
                           "undermine=0.0, um_status=0, distance=116.99, notes='')"
    assert system == eval(repr(system).replace('System', 'cogdb.schema.System'))


@db_cleanup
def test_system__str__():
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)

    session = cogdb.Session()
    session.add(system)
    session.commit()

    assert str(system) == "id=1, System(name='Frey', sheet_order=0, sheet_col='F', "\
                          "cmdr_merits=4322, fort_status=4910, trigger=4910, "\
                          "undermine=0.0, um_status=0, distance=116.99, notes='')"


def test_system_short_display():
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    assert system.short_display() == 'Frey :Fortifying: 4910/4910'

    system.fort_status = 4000
    system.cmdr_merits = 4000
    assert system.short_display() == 'Frey :Fortifying: 4000/4910, missing: 910'


def test_system_current_status():
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    assert system.current_status == 4910


def test_system_skip():
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    assert system.skip is False

    system.notes = 'Leave for now.'
    assert system.skip is True


def test_system_is_fortified():
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    system.fort_status = system.trigger
    assert system.is_fortified is True

    system.fort_status = system.fort_status // 2
    assert system.is_fortified is False


def test_system_is_undermined():
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)

    system.undermine = 1.0
    assert system.is_undermined is True

    system.undermine = 0.4
    assert system.is_undermined is False


def test_system_missing():
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    system.cmdr_merits = 0

    system.fort_status = system.trigger - 1000
    assert system.missing == 1000

    system.fort_status = system.trigger
    assert system.missing == 0

    system.fort_status = system.trigger + 1000
    assert system.missing == 0


def test_system_completion():
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    assert system.completion == '100.0'
    system.trigger = 0
    assert system.completion == '0.0'


def test_system_table_row():
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)

    system.notes = 'Leave'
    assert system.table_row == ('Frey', '   0', '4910/4910 (100.0%/0.0%)', 'Leave')


def test_parse_int():
    assert cogdb.schema.parse_int('') == 0
    assert cogdb.schema.parse_int('2') == 2
    assert cogdb.schema.parse_int(5) == 5


def test_parse_float():
    assert cogdb.schema.parse_float('') == 0.0
    assert cogdb.schema.parse_float('2') == 2.0
    assert cogdb.schema.parse_float(0.5) == 0.5


def test_system_result_dict():
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    assert result['undermine'] == 0.0
    assert result['trigger'] == 4910
    assert result['cmdr_merits'] == 4322
    assert result['fort_status'] == 4910
    assert result['notes'] == ''
    assert result['name'] == 'Frey'
    assert result['sheet_col'] == 'F'
    assert result['sheet_order'] == 0

    with pytest.raises(cog.exc.SheetParsingError):
        cogdb.schema.system_result_dict(['' for _ in range(0, 10)], 1, 'A')

    with pytest.raises(cog.exc.SheetParsingError):
        cogdb.schema.system_result_dict([], 1, 'A')

    with pytest.raises(cog.exc.SheetParsingError):
        cogdb.schema.system_result_dict(SYSTEM_DATA[:-2], 1, 'A')
