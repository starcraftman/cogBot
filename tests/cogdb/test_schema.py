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

from tests.cogdb import CELLS, FMT_CELLS, UM_CELLS

SYSTEM_DATA = ['', 1, 4910, 0, 4322, 4910, 0, 116.99, '', 'Frey']


def db_cleanup(function):
    """
    Clean the whole database. Guarantee it is empty.
    """
    def wrapper(function, *args, **kwargs):
        try:
            function(*args, **kwargs)
        finally:
            cogdb.schema.drop_all_tables()
            session = cogdb.Session()
            assert session.query(cogdb.schema.DUser).all() == []
            assert session.query(cogdb.schema.SUser).all() == []
            assert session.query(cogdb.schema.SUserUM).all() == []
            assert session.query(cogdb.schema.SystemUM).all() == []
            assert session.query(cogdb.schema.System).all() == []
            assert session.query(cogdb.schema.Hold).all() == []
            assert session.query(cogdb.schema.Hold).all() == []
            assert session.query(cogdb.schema.Command).all() == []
    return decorator.decorator(wrapper, function)


def db_data(function):
    """
    Wrap a test and setup database with dummy data.
    """
    def wrapper(function, *args, **kwargs):
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

        function(*args, **kwargs)
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


def dec_system_um(function):
    def call():
        sys_cols = UM_CELLS[3:5]
        kwargs = cogdb.schema.kwargs_um_system(sys_cols, 'D')
        system = cogdb.schema.SystemUM(**kwargs)

        function(kwargs=kwargs, system=system)

    return call


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


def dec_drop(function):
    def call():
        suser = cogdb.schema.SUser(name='test user', row=2)
        result = cogdb.schema.kwargs_fort_system(SYSTEM_DATA, 0, 'F')
        system = cogdb.schema.System(**result)

        session = cogdb.Session()
        session.add_all([system, suser])
        session.commit()
        drop = cogdb.schema.Drop(amount=400, user_id=suser.id, system_id=system.id)
        session.add(drop)
        session.commit()

        function(session=session, suser=suser, system=system, drop=drop)

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
    assert session.query(cogdb.schema.Drop).all()
    assert session.query(cogdb.schema.System).all()
    cogdb.schema.drop_all_tables()
    assert session.query(cogdb.schema.DUser).all() == []
    assert session.query(cogdb.schema.SUser).all() == []
    assert session.query(cogdb.schema.Command).all() == []
    assert session.query(cogdb.schema.Drop).all() == []
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
    assert session.query(cogdb.schema.Drop).all()
    assert session.query(cogdb.schema.System).all()
    cogdb.schema.drop_scanned_tables()
    assert session.query(cogdb.schema.DUser).all()
    assert session.query(cogdb.schema.SUser).all() == []
    assert session.query(cogdb.schema.Command).all()
    assert session.query(cogdb.schema.Drop).all() == []
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
    result = cogdb.schema.kwargs_fort_system(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    result = cogdb.schema.kwargs_fort_system(SYSTEM_DATA, 0, 'G')
    result['name'] = 'Sol'
    system2 = cogdb.schema.System(**result)
    session = cogdb.Session()
    session.add_all([system, system2, suser])
    session.commit()

    fort = cogdb.schema.Drop(amount=400, user_id=suser.id, system_id=system.id)
    fort2 = cogdb.schema.Drop(amount=200, user_id=suser.id, system_id=system2.id)
    session = cogdb.Session()
    session.add_all([fort, fort2])
    session.commit()

    assert suser.merits == 600


@db_cleanup
@dec_drop
def test_drop__eq__(**kwargs):
    suser, system, drop = (kwargs['suser'], kwargs['system'], kwargs['drop'])
    assert drop == cogdb.schema.Drop(amount=400, user_id=suser.id,
                                     system_id=system.id)
    assert drop.suser == suser
    assert drop.system == system


@db_cleanup
@dec_drop
def test_drop__repr__(**kwargs):
    drop = kwargs['drop']
    assert repr(drop) == "Drop(system_id=1, user_id=1, amount=400)"
    assert drop == eval(repr(drop).replace('Drop', 'cogdb.schema.Drop'))


@db_cleanup
@dec_drop
def test_drop__str__(**kwargs):
    drop = kwargs['drop']
    assert str(drop) == "id=1, system_name='Frey', suser_name='test user', "\
                        "Drop(system_id=1, user_id=1, amount=400)"


@db_cleanup
def test_system__eq__():
    result = cogdb.schema.kwargs_fort_system(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)

    session = cogdb.Session()
    session.add(system)
    session.commit()

    assert system == cogdb.schema.System(**result)


@db_cleanup
def test_system__repr__():
    result = cogdb.schema.kwargs_fort_system(SYSTEM_DATA, 0, 'F')
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
    result = cogdb.schema.kwargs_fort_system(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)

    session = cogdb.Session()
    session.add(system)
    session.commit()

    assert str(system) == "id=1, System(name='Frey', cmdr_merits=4322, fort_status=4910, "\
                          "trigger=4910, um_status=0, undermine=0.0, distance=116.99, "\
                          "notes='', sheet_col='F', sheet_order=0)"


def test_system_short_display():
    result = cogdb.schema.kwargs_fort_system(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    assert system.short_display() == 'Frey :Fortifying: 4910/4910'

    system.fort_status = 4000
    system.cmdr_merits = 4000
    assert system.short_display() == 'Frey :Fortifying: 4000/4910\nMissing: 910'
    assert system.short_display(missing=False) == 'Frey :Fortifying: 4000/4910'


def test_system_set_status():
    result = cogdb.schema.kwargs_fort_system(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    assert system.fort_status == 4910

    system.set_status('4000')
    system.fort_status = 4000
    system.um_status = 0

    system.set_status('2200:2000')
    system.fort_status = 2200
    system.um_status = 2000


def test_system_current_status():
    result = cogdb.schema.kwargs_fort_system(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    assert system.current_status == 4910


def test_system_skip():
    result = cogdb.schema.kwargs_fort_system(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    assert system.skip is False

    system.notes = 'Leave for now.'
    assert system.skip is True


def test_system_is_fortified():
    result = cogdb.schema.kwargs_fort_system(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    system.fort_status = system.trigger
    assert system.is_fortified is True

    system.fort_status = system.fort_status // 2
    assert system.is_fortified is False


def test_system_is_undermined():
    result = cogdb.schema.kwargs_fort_system(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)

    system.undermine = 1.0
    assert system.is_undermined is True

    system.undermine = 0.4
    assert system.is_undermined is False


def test_system_missing():
    result = cogdb.schema.kwargs_fort_system(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    system.cmdr_merits = 0

    system.fort_status = system.trigger - 1000
    assert system.missing == 1000

    system.fort_status = system.trigger
    assert system.missing == 0

    system.fort_status = system.trigger + 1000
    assert system.missing == 0


def test_system_completion():
    result = cogdb.schema.kwargs_fort_system(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)
    assert system.completion == '100.0'
    system.trigger = 0
    assert system.completion == '0.0'


def test_system_table_row():
    result = cogdb.schema.kwargs_fort_system(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)

    system.notes = 'Leave'
    assert system.table_row == ('Frey', '   0', '4910/4910 (100.0%/0.0%)', 'Leave')


@dec_system_um
def test_system_um__repr__(**kwargs):
    system = kwargs['system']

    assert repr(system) == "SystemUM(name='Burr', type='expand', sheet_col='D', "\
        "completion=-0.84, goal=364297, cmdr_merits=160472, missing=127277, progress_us=161630, "\
        "progress_them=35.0, trigger=6939, margin=0.5, security='Sec: Low', notes='', "\
        "close_control='Dongkum')"
    assert system == eval(repr(system).replace('SystemUM', 'cogdb.schema.SystemUM'))


@dec_system_um
def test_system_um__str__(**kwargs):
    system = kwargs['system']
    assert str(system) == "id=None, SystemUM(name='Burr', type='expand', sheet_col='D', "\
        "completion=-0.84, goal=364297, cmdr_merits=160472, missing=127277, progress_us=161630, "\
        "progress_them=35.0, trigger=6939, margin=0.5, security='Sec: Low', notes='', "\
        "close_control='Dongkum')"


@dec_system_um
def test_system_um__eq__(**kwargs):
    system = kwargs['system']

    assert system == cogdb.schema.SystemUM(name='Burr', sheet_col='D', completion=-0.84,
                                           goal=364297, cmdr_merits=160472, missing=127277,
                                           progress_us=161630, progress_them=35.0, trigger=6939,
                                           margin=0.5, security='Sec: Low', notes='',
                                           close_control='Dongkum')


@dec_system_um
def test_kwargs_system_um(**kwargs):
    expect = {
        'close_control': 'Dongkum',
        'cmdr_merits': 160472,
        'completion': -0.84,
        'goal': 364297,
        'margin': 0.5,
        'missing': 127277,
        'name': 'Burr',
        'notes': '',
        'progress_us': 161630,
        'progress_them': 35.0,
        'security': 'Sec: Low',
        'sheet_col': 'D',
        'trigger': 6939,
        'type': 'expand',
    }
    sys_cols = copy.deepcopy(UM_CELLS[3:5])
    assert cogdb.schema.kwargs_um_system(sys_cols, 'D') == expect

    sys_cols[0][0] = 'Opp.'
    expect['type'] = cogdb.schema.SystemUM.T_OPPOSE
    assert cogdb.schema.kwargs_um_system(sys_cols, 'D') == expect

    sys_cols[0][0] = ''
    expect['type'] = cogdb.schema.SystemUM.T_CONTROL
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
        'sheet_order': 0,
        'trigger': 4910,
        'um_status': 0,
        'undermine': 0.0,
    }
    assert cogdb.schema.kwargs_fort_system(SYSTEM_DATA, 0, 'F') == expect

    with pytest.raises(cog.exc.SheetParsingError):
        cogdb.schema.kwargs_fort_system(['' for _ in range(0, 10)], 1, 'A')

    with pytest.raises(cog.exc.SheetParsingError):
        cogdb.schema.kwargs_fort_system([], 1, 'A')

    with pytest.raises(cog.exc.SheetParsingError):
        cogdb.schema.kwargs_fort_system(SYSTEM_DATA[:-2], 1, 'A')


def test_parse_int():
    assert cogdb.schema.parse_int('') == 0
    assert cogdb.schema.parse_int('2') == 2
    assert cogdb.schema.parse_int(5) == 5


def test_parse_float():
    assert cogdb.schema.parse_float('') == 0.0
    assert cogdb.schema.parse_float('2') == 2.0
    assert cogdb.schema.parse_float(0.5) == 0.5
