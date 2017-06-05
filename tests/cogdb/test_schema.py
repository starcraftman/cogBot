"""
Test the schema for the database.
"""
from __future__ import absolute_import, print_function

import pytest

import cog.exc
import cogdb
import cogdb.schema


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
            assert session.query(cogdb.schema.System).all() == []
            assert session.query(cogdb.schema.User).all() == []
            assert session.query(cogdb.schema.Fort).all() == []
    return call


@db_cleanup
def test_drop_tables():
    session = cogdb.Session()
    user = cogdb.schema.User(sheet_name='test user', sheet_row=2)

    session.add(user)
    session.commit()

    assert session.query(cogdb.schema.User).all()
    cogdb.schema.drop_all_tables()
    assert not session.query(cogdb.schema.User).all()


@db_cleanup
def test_user_creation():
    user = cogdb.schema.User(sheet_name='test user', sheet_row=2)
    assert user.id is None
    assert repr(user) == "<User(sheet_name='test user', sheet_row='2')>"
    assert str(user) == "ID='None', <User(sheet_name='test user', sheet_row='2')>"

    session = cogdb.Session()
    session.add(user)
    session.commit()

    assert user.id == 1
    assert repr(user) == "<User(sheet_name='test user', sheet_row='2')>"
    assert str(user) == "ID='1', <User(sheet_name='test user', sheet_row='2')>"


@db_cleanup
def test_fort_creation():
    user = cogdb.schema.User(sheet_name='test user', sheet_row=2)
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)

    session = cogdb.Session()
    session.add(system)
    session.add(user)
    session.commit()

    fort = cogdb.schema.Fort(amount=400, user_id=user.id, system_id=system.id)
    session.add(fort)
    session.commit()
    assert repr(fort) == "<Fort(user='test user', system='Frey', amount='400')>"
    assert str(fort) == "ID='1', <Fort(user='test user', system='Frey', amount='400')>"


@db_cleanup
def test_system_creation():
    result = cogdb.schema.system_result_dict(SYSTEM_DATA, 0, 'F')
    system = cogdb.schema.System(**result)

    assert system.id is None
    session = cogdb.Session()
    session.add(system)
    session.commit()

    assert system.id == 1
    assert repr(system) == "<System(name='Frey', sheet_order='0', sheet_col='F', merits='4322', fort_status='4910', trigger='4910', undermine='0.0', notes='')>"
    assert str(system) == "ID='1', <System(name='Frey', sheet_order='0', sheet_col='F', merits='4322', fort_status='4910', trigger='4910', undermine='0.0', notes='')>"


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

    system.undermine = 100.0
    assert system.is_undermined is True

    system.undermine = 40.0
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

    assert system.table_row == ('Frey', '4910/4910 (100.0%)', '   0', '0.0%', '')
    system.notes = 'Leave'
    assert system.table_row == ('Frey', '4910/4910 (100.0%)', 'N/A', '0.0%', 'Leave')


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
