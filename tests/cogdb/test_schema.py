"""
Test the schema for the database.
"""
from __future__ import absolute_import, print_function
import datetime

import pytest

import cog.exc
import cogdb
import cogdb.schema

from tests.cogdb import CELLS

SYSTEM_DATA = ['', 1, 4910, 0, 4322, 4910, 0, 116.99, '', 'Frey']


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

        user_col, user_row = cogdb.query.first_user_row(CELLS)
        scanner = cogdb.query.SheetScanner(CELLS, 'F', user_col, user_row)
        systems = scanner.systems()
        users = scanner.users()
        session.add_all(systems + users)
        session.commit()

        forts = scanner.forts(systems, users)
        session.add_all(forts)
        session.commit()

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
def test_command_creation():
    duser = cogdb.schema.DUser(discord_id='1111', display_name='test user', capacity=0,
                               sheet_name='test user')
    dtime = datetime.datetime.now()
    cmd = cogdb.schema.Command(discord_id=duser.discord_id, cmd_str='drop 400', date=dtime)

    session = cogdb.Session()
    session.add(duser)
    session.add(cmd)
    session.commit()

    assert repr(cmd) == "Command(discord_id='1111', cmd_str='drop 400', date={!r})".format(dtime)
    assert str(cmd) == "id=1, display_name='test user', Command(discord_id='1111', cmd_str='drop 400', date={!r})".format(dtime)
    assert cmd.duser == duser

    Command = cogdb.schema.Command
    assert cmd == eval(repr(cmd))


@db_cleanup
def test_duser_creation():
    duser = cogdb.schema.DUser(discord_id='1111', display_name='test user', capacity=0,
                               sheet_name='test user')
    suser = cogdb.schema.SUser(sheet_name='test user', sheet_row=2)

    session = cogdb.Session()
    session.add(duser)
    session.add(suser)
    session.commit()

    assert repr(duser) == "DUser(display_name='test user', discord_id='1111', capacity=0, sheet_name='test user')"
    assert str(duser) == "id=1, DUser(display_name='test user', discord_id='1111', capacity=0, sheet_name='test user')"
    assert suser.duser == duser
    assert duser.suser == suser

    DUser = cogdb.schema.DUser
    assert duser == eval(repr(duser))


@db_cleanup
def test_suser_creation():
    duser = cogdb.schema.DUser(discord_id='1111', display_name='test user', capacity=0,
                               sheet_name='test user')
    suser = cogdb.schema.SUser(sheet_name='test user', sheet_row=2)
    assert repr(suser) == "SUser(sheet_name='test user', sheet_row=2)"
    assert str(suser) == "id=None, SUser(sheet_name='test user', sheet_row=2)"

    session = cogdb.Session()
    session.add(suser)
    session.add(duser)
    session.commit()

    assert repr(suser) == "SUser(sheet_name='test user', sheet_row=2)"
    assert str(suser) == "id=1, SUser(sheet_name='test user', sheet_row=2)"
    assert suser.duser == duser
    assert duser.suser == suser

    SUser = cogdb.schema.SUser
    assert suser == eval(repr(suser))


@db_cleanup
def test_fort_creation():
    user = cogdb.schema.SUser(sheet_name='test user', sheet_row=2)
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)

    session = cogdb.Session()
    session.add(system)
    session.add(user)
    session.commit()

    fort = cogdb.schema.Fort(amount=400, user_id=user.id, system_id=system.id)
    session.add(fort)
    session.commit()
    assert repr(fort) == "Fort(user_id=1, system_id=1, amount=400)"
    assert str(fort) == "id=1, sheet_name='test user', system_name='Frey', Fort(user_id=1, system_id=1, amount=400)"
    assert fort.suser == user
    assert fort.system == system

    Fort = cogdb.schema.Fort
    assert fort == eval(repr(fort))


@db_cleanup
def test_system_creation():
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)

    assert system.id is None
    session = cogdb.Session()
    session.add(system)
    session.commit()

    assert system.id == 1
    assert repr(system) == "System(name='Frey', sheet_order=0, sheet_col='F', cmdr_merits=4322, fort_status=4910, trigger=4910, undermine=0.0, notes='')"
    assert str(system) == "id=1, System(name='Frey', sheet_order=0, sheet_col='F', cmdr_merits=4322, fort_status=4910, trigger=4910, undermine=0.0, notes='')"

    System = cogdb.schema.System
    assert system == eval(repr(system))


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

    with pytest.raises(cog.exc.IncorrectData):
        cogdb.schema.system_result_dict([], 1, 'A')

    with pytest.raises(cog.exc.IncorrectData):
        cogdb.schema.system_result_dict(SYSTEM_DATA[:-2], 1, 'A')
