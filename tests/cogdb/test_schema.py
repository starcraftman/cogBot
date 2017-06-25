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


def duser_and_suser(function):
    def call():
        duser = cogdb.schema.DUser(discord_id='1111', display_name='test user',
                                   pref_name='test user')
        suser = cogdb.schema.SUser(name='test user', row=2)

        session = cogdb.Session()
        session.add_all([suser, duser])
        session.commit()
        duser.set_cattle(suser)
        session.commit()

        function(session=session, duser=duser, suser=suser)

    return call


def dec_fort(function):
    def call():
        suser = cogdb.schema.SUser(name='test user', row=2)
        result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
        system = cogdb.schema.System(**result)

        session = cogdb.Session()
        session.add_all([system, suser])
        session.commit()
        fort = cogdb.schema.Fort(amount=400, user_id=suser.id, system_id=system.id)
        session.add(fort)
        session.commit()

        function(session=session, suser=suser, system=system, fort=fort)

    return call


def dec_cmd(function):
    def call():
        session = cogdb.Session()
        dtime = datetime.datetime.now()
        duser = cogdb.schema.DUser(discord_id='1111', display_name='test user', capacity=0,
                                   pref_name='test user')
        session.add(duser)
        session.commit()

        cmd = cogdb.schema.Command(discord_id=duser.discord_id, cmd_str='drop 400', date=dtime)
        session.add(cmd)
        session.commit()

        function(session=session, duser=duser, cmd=cmd, dtime=dtime)

    return call


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
                                   capacity=0, pref_name='GearsandCogs')
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
    user = cogdb.schema.SUser(name='test user', row=2)
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
    user = cogdb.schema.SUser(name='test user', row=2)
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
@dec_cmd
def test_command__eq__(**kwargs):
    cmd, duser, dtime = (kwargs['cmd'], kwargs['duser'], kwargs['dtime'])
    assert cmd.duser == duser
    assert cmd == cogdb.schema.Command(discord_id=duser.discord_id, cmd_str='drop 400', date=dtime)


@db_cleanup
@dec_cmd
def test_command__repr__(**kwargs):
    cmd, dtime = (kwargs['cmd'], kwargs['dtime'])
    assert repr(cmd) == "Command(discord_id='1111', cmd_str='drop 400', date={!r})".format(dtime)
    assert cmd == eval(repr(cmd).replace('Command', 'cogdb.schema.Command'))


@db_cleanup
@dec_cmd
def test_command__str__(**kwargs):
    cmd, dtime = (kwargs['cmd'], kwargs['dtime'])
    assert str(cmd) == "id=1, display_name='test user', Command(discord_id='1111', "\
                       "cmd_str='drop 400', date={!r})".format(dtime)


@db_cleanup
@duser_and_suser
def test_duser__eq__(**kwargs):
    suser, duser = (kwargs['suser'], kwargs['duser'])
    assert suser.duser == duser
    assert duser.suser == suser
    assert duser == cogdb.schema.DUser(discord_id='1111', display_name='test user',
                                       pref_name='test user')


@db_cleanup
@duser_and_suser
def test_duser__repr__(**kwargs):
    duser = kwargs['duser']
    assert repr(duser) == "DUser(discord_id='1111', display_name='test user', "\
                          "pref_name='test user', capacity=0, cattle_id=1)"
    assert duser == eval(repr(duser).replace('DUser', 'cogdb.schema.DUser'))


@db_cleanup
@duser_and_suser
def test_duser__str__(**kwargs):
    duser = kwargs['duser']
    assert str(duser) == "DUser(discord_id='1111', display_name='test user', "\
                         "pref_name='test user', capacity=0, cattle_id=1)"


@db_cleanup
@duser_and_suser
def test_duser_set_cattle(**kwargs):
    duser, suser = (kwargs['duser'], kwargs['suser'])
    suser.id = 10
    duser.set_cattle(suser)
    assert duser.cattle_id == 10


@db_cleanup
@duser_and_suser
def test_suser__eq__(**kwargs):
    suser, duser = (kwargs['suser'], kwargs['duser'])
    assert suser == cogdb.schema.SUser(name='test user', row=2)
    assert suser.duser == duser
    assert duser.suser == suser


@db_cleanup
@duser_and_suser
def test_suser__repr__(**kwargs):
    suser = kwargs['suser']
    assert repr(suser) == "SUser(name='test user', row=2, cry='')"
    assert suser == eval(repr(suser).replace('SUser', 'cogdb.schema.SUser'))


@db_cleanup
@duser_and_suser
def test_suser__str__(**kwargs):
    suser = kwargs['suser']
    assert str(suser) == "id=1, SUser(name='test user', row=2, cry='')"


@db_cleanup
def test_suser_merits():
    suser = cogdb.schema.SUser(name='test user', row=2)
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'G')
    result['name'] = 'Sol'
    system2 = cogdb.schema.System(**result)
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
@dec_fort
def test_fort__eq__(**kwargs):
    suser, system, fort = (kwargs['suser'], kwargs['system'], kwargs['fort'])
    assert fort == cogdb.schema.Fort(amount=400, user_id=suser.id,
                                     system_id=system.id)
    assert fort.suser == suser
    assert fort.system == system


@db_cleanup
@dec_fort
def test_fort__repr__(**kwargs):
    fort = kwargs['fort']
    assert repr(fort) == "Fort(system_id=1, user_id=1, amount=400)"
    assert fort == eval(repr(fort).replace('Fort', 'cogdb.schema.Fort'))


@db_cleanup
@dec_fort
def test_fort__str__(**kwargs):
    fort = kwargs['fort']
    assert str(fort) == "id=1, system_name='Frey', suser_name='test user', "\
                        "Fort(system_id=1, user_id=1, amount=400)"


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

    assert repr(system) == "System(name='Frey', cmdr_merits=4322, fort_status=4910, "\
                           "trigger=4910, um_status=0, undermine=0.0, distance=116.99, "\
                           "notes='', sheet_col='F', sheet_order=0)"
    assert system == eval(repr(system).replace('System', 'cogdb.schema.System'))


@db_cleanup
def test_system__str__():
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)

    session = cogdb.Session()
    session.add(system)
    session.commit()

    assert str(system) == "id=1, System(name='Frey', cmdr_merits=4322, fort_status=4910, "\
                          "trigger=4910, um_status=0, undermine=0.0, distance=116.99, "\
                          "notes='', sheet_col='F', sheet_order=0)"


def test_system_short_display():
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    assert system.short_display() == 'Frey :Fortifying: 4910/4910'

    system.fort_status = 4000
    system.cmdr_merits = 4000
    assert system.short_display() == 'Frey :Fortifying: 4000/4910\nMissing: 910'
    assert system.short_display(missing=False) == 'Frey :Fortifying: 4000/4910'


def test_system_set_status():
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    assert system.fort_status == 4910

    system.set_status('4000')
    system.fort_status = 4000
    system.um_status = 0

    system.set_status('2200:2000')
    system.fort_status = 2200
    system.um_status = 2000


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
