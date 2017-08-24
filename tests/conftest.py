"""
Used for pytest fixtures and anything else test setup/teardown related.
"""
from __future__ import absolute_import, print_function

import mock
import pytest

import cogdb
import cogdb.query
from cogdb.schema import (DUser, PrepSystem, System, SystemUM, Drop, Hold,
                          SheetRow, SheetCattle, SheetUM,
                          EFaction, kwargs_um_system, kwargs_fort_system)
from tests.data import CELLS_FORT, CELLS_FORT_FMT, CELLS_UM, SYSTEMS_DATA, SYSTEMSUM_DATA


# @pytest.yield_fixture(scope='function', autouse=True)
# def around_all_tests(session):
    # """
    # Executes before and after EVERY test.

    # Can be helpful for tracking bugs, like dirty database after test.
    # Disabled unless needed. Non-trivial overhead.
    # """

    # yield

    # classes = [DUser, SheetRow, System, SystemUM, Drop, Hold]
    # for cls in classes:
        # assert not session.query(cls).all()


@pytest.fixture
def mock_sheet(db_cleanup):
    fake_sheet = mock.Mock()
    fake_sheet.whole_sheet.return_value = CELLS_FORT
    fake_sheet.get_with_formatting.return_value = CELLS_FORT_FMT

    yield fake_sheet


@pytest.fixture
def mock_umsheet(db_cleanup):
    fake_sheet = mock.Mock()
    fake_sheet.whole_sheet.return_value = CELLS_UM

    yield fake_sheet


@pytest.fixture
def session():
    return cogdb.Session()


@pytest.fixture
def db_cleanup(session):
    """
    Clean the whole database. Guarantee it is empty.
    Used when tests don't use a fixture.
    """
    yield

    cogdb.schema.empty_tables(session, perm=True)

    classes = [DUser, SheetRow, System, SystemUM, Drop, Hold]
    for cls in classes:
        assert session.query(cls).all() == []


@pytest.fixture
def f_dusers(session):
    """
    Fixture to insert some test DUsers.
    """
    dusers = (
        DUser(id='1000', display_name='GearsandCogs',
              pref_name='GearsandCogs', faction=EFaction.hudson),
        DUser(id='1001', display_name='rjwhite',
              pref_name='rjwhite', faction=EFaction.hudson),
        DUser(id='1002', display_name='vampyregtx',
              pref_name='not_vamp', faction=EFaction.hudson),
    )
    session.add_all(dusers)
    session.commit()

    yield dusers

    for matched in session.query(DUser):
        session.delete(matched)
    session.commit()


@pytest.fixture
def f_sheets(session):
    """
    Fixture to insert some test SheetRows.

    Depends on: f_dusers
    """
    dusers = session.query(DUser).all()
    assert dusers

    sheets = (
        SheetCattle(id=1, name=dusers[0].pref_name, row=15, cry='Gears are forting late!'),
        SheetUM(id=2, name=dusers[0].pref_name, row=18, cry='Gears are pew pew!'),
        SheetCattle(id=3, name=dusers[1].pref_name, row=16, cry=''),
        SheetUM(id=4, name=dusers[1].pref_name, row=19, cry='Shooting time'),
        SheetCattle(id=5, name=dusers[2].pref_name, row=17, cry='Vamp the boss'),
    )
    session.add_all(sheets)
    session.commit()

    yield sheets

    for matched in session.query(SheetRow):
        session.delete(matched)
    session.commit()


@pytest.fixture
def f_systems(session):
    """
    Fixture to insert some test Systems.
    """
    order = 1
    column = 'F'
    systems = []
    for data in SYSTEMS_DATA:
        kwargs = kwargs_fort_system(data, order, column)
        kwargs['id'] = order
        systems.append(System(**kwargs))
        order += 1
        column = chr(ord(column) + 1)
    session.add_all(systems)
    session.commit()

    yield systems

    for matched in session.query(System):
        session.delete(matched)
    session.commit()


@pytest.fixture
def f_prepsystem(session):
    prep = PrepSystem(id=100, name='Muncheim', trigger=10000, fort_status=5100, um_status=0,
                      undermine=0.0, distance=65.55, notes='Atropos', sheet_col='D',
                      sheet_order=0)
    session.add(prep)
    session.commit()

    yield prep

    for matched in session.query(PrepSystem):
        session.delete(matched)
    session.commit()


@pytest.fixture
def f_drops(session):
    """
    Fixture to insert some test Drops.

    Depends on: f_sheets, f_systems
    """
    users = session.query(SheetCattle).all()
    systems = session.query(System).all()

    drops = (
        Drop(id=1, amount=700, user_id=users[0].id, system_id=systems[0].id),
        Drop(id=2, amount=400, user_id=users[0].id, system_id=systems[1].id),
        Drop(id=3, amount=1200, user_id=users[1].id, system_id=systems[0].id),
        Drop(id=4, amount=1800, user_id=users[2].id, system_id=systems[0].id),
        Drop(id=5, amount=800, user_id=users[1].id, system_id=systems[1].id),
    )
    session.add_all(drops)
    session.commit()

    yield drops

    for matched in session.query(Drop):
        session.delete(matched)
    session.commit()


@pytest.fixture
def f_systemsum(session):
    """
    Fixture to insert some test Systems.
    """
    column = 'D'
    systems = []
    count = 1
    for data in SYSTEMSUM_DATA:
        kwargs = kwargs_um_system(data, column)
        kwargs['id'] = count
        systems.append(SystemUM.factory(kwargs))
        column = chr(ord(column) + 2)
        count += 1
    session.add_all(systems)
    session.commit()

    yield systems

    for matched in session.query(SystemUM):
        session.delete(matched)
    session.commit()


@pytest.fixture
def f_holds(session):
    """
    Fixture to insert some test Holds.

    Depends on: f_sheets, f_systemsum
    """
    users = session.query(SheetUM).all()
    systems = session.query(SystemUM).all()

    holds = (
        Hold(id=1, held=0, redeemed=4000, user_id=users[0].id, system_id=systems[0].id),
        Hold(id=2, held=400, redeemed=1550, user_id=users[0].id, system_id=systems[1].id),
        Hold(id=3, held=2200, redeemed=5800, user_id=users[0].id, system_id=systems[2].id),
        Hold(id=4, held=450, redeemed=2000, user_id=users[1].id, system_id=systems[0].id),
        Hold(id=5, held=2400, redeemed=0, user_id=users[1].id, system_id=systems[1].id),
        Hold(id=6, held=0, redeemed=1200, user_id=users[1].id, system_id=systems[2].id),
    )
    session.add_all(holds)
    session.commit()

    yield holds

    for matched in session.query(Hold):
        session.delete(matched)
    session.commit()
