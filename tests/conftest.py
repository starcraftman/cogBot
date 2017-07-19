"""
Used for pytest fixtures and anything else test setup/teardown related.
"""
from __future__ import absolute_import, print_function
import datetime

import mock
import pytest

import cogdb
import cogdb.query
from cogdb.schema import (DUser, PrepSystem, System, SystemUM, Drop, Hold, Command,
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

    cogdb.schema.drop_tables(all=True)

    classes = [DUser, SheetRow, System, SystemUM, Drop, Hold, Command]
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

    session.query(DUser).delete()


@pytest.fixture
def f_sheets(session):
    """
    Fixture to insert some test SheetRows.

    Depends on: f_dusers
    """
    dusers = session.query(DUser).all()
    assert dusers

    sheets = (
        SheetCattle(name=dusers[0].pref_name, row=15, cry='Gears are forting late!'),
        SheetUM(name=dusers[0].pref_name, row=18, cry='Gears are pew pew!'),
        SheetCattle(name=dusers[1].pref_name, row=16, cry=''),
        SheetUM(name=dusers[1].pref_name, row=19, cry='Shooting time'),
        SheetCattle(name=dusers[2].pref_name, row=17, cry='Vamp the boss'),
    )
    session.add_all(sheets)
    session.commit()

    yield sheets

    session.query(SheetRow).delete()


@pytest.fixture
def f_commands(session):
    """
    Fixture to insert some test Commands.

    Depends on: f_dusers
    Last element of fixture is dtime of insertions.
    """
    dusers = session.query(DUser).all()
    assert dusers

    dtime = datetime.datetime.now()
    commands = (
        Command(discord_id=dusers[0].id, cmd_str='drop 400', date=dtime),
        Command(discord_id=dusers[0].id, cmd_str='drop 700', date=dtime),
        Command(discord_id=dusers[0].id, cmd_str='user', date=dtime),
    )
    session.add_all(commands)
    session.commit()

    yield commands + (dtime,)

    session.query(Command).delete()


@pytest.fixture
def f_systems(session):
    """
    Fixture to insert some test Systems.
    """
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
def f_prepsystem(session):
    prep = PrepSystem(name='Muncheim', trigger=10000, fort_status=5100, um_status=0,
                      undermine=0.0, distance=65.55, notes='Atropos', sheet_col='D',
                      sheet_order=0)
    session.add(prep)
    session.commit()

    yield prep

    session.query(PrepSystem).delete()


@pytest.fixture
def f_drops(session):
    """
    Fixture to insert some test Drops.

    Depends on: f_sheets, f_systems
    """
    users = session.query(SheetCattle).all()
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
def f_systemsum(session):
    """
    Fixture to insert some test Systems.
    """
    column = 'D'
    systems = []
    for data in SYSTEMSUM_DATA:
        systems.append(SystemUM.factory(kwargs_um_system(data, column)))
        column = chr(ord(column) + 2)
    session.add_all(systems)
    session.commit()

    yield systems

    session.query(SystemUM).delete()


@pytest.fixture
def f_holds(session):
    """
    Fixture to insert some test Holds.

    Depends on: f_sheets, f_systemsum
    """
    users = session.query(SheetUM).all()
    systems = session.query(SystemUM).all()

    holds = (
        Hold(held=0, redeemed=4000, user_id=users[0].id, system_id=systems[0].id),
        Hold(held=400, redeemed=1550, user_id=users[0].id, system_id=systems[1].id),
        Hold(held=2200, redeemed=5800, user_id=users[0].id, system_id=systems[2].id),
        Hold(held=450, redeemed=2000, user_id=users[1].id, system_id=systems[0].id),
        Hold(held=2400, redeemed=0, user_id=users[1].id, system_id=systems[1].id),
        Hold(held=0, redeemed=1200, user_id=users[1].id, system_id=systems[2].id),
    )
    session.add_all(holds)
    session.commit()

    yield holds

    session.query(Hold).delete()
