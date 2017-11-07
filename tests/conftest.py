"""
Used for pytest fixtures and anything else test setup/teardown related.
"""
from __future__ import absolute_import, print_function
import copy
import datetime
import os
import sys

import aiomock
import mock
import pytest
try:
    import zmq.asyncio
    LOOP = zmq.asyncio.ZMQEventLoop
    loop = LOOP()
    loop.set_debug(True)
    print("Test loop policy:", str(loop))
except ImportError:
    print("Missing zmq lib.")
    sys.exit(1)

import cogdb
import cogdb.query
from cogdb.schema import (DUser, PrepSystem, System, SystemUM, Drop, Hold,
                          UMExpand, UMOppose, UMControl,
                          SheetRow, SheetCattle, SheetUM,
                          EFaction, Admin, ChannelPerm, RolePerm, FortOrder)
from tests.data import CELLS_FORT, CELLS_FORT_FMT, CELLS_UM


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


REASON_SLOW = 'Slow as blocking to sheet. To enable, ensure os.environ ALL_TESTS=True'
SHEET_TEST = pytest.mark.skipif(not os.environ.get('ALL_TESTS'), reason=REASON_SLOW)
PROC_TEST = SHEET_TEST


@pytest.fixture
def event_loop():
    """
    Provide a a new test loop for each test.
    Save system wide loop policy, and use uvloop if available.

    To test either:
        1) Mark with pytest.mark.asyncio
        2) event_loop.run_until_complete(asyncio.gather(futures))
    """
    loop = LOOP()
    loop.set_debug(True)

    yield loop

    loop.close()


@pytest.fixture
def session():
    session = cogdb.Session()

    yield cogdb.Session()

    session.close()


@pytest.fixture
def side_session():
    return cogdb.SideSession()


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
    systems = [
        System(id=1, name='Frey', fort_status=4910, trigger=4910, fort_override=0.7, um_status=0, undermine=0.0, distance=116.99, notes='', sheet_col='G', sheet_order=1),
        System(id=2, name='Nurundere', fort_status=5422, trigger=8425, fort_override=0.6, um_status=0, undermine=0.0, distance=99.51, notes='', sheet_col='H', sheet_order=2),
        System(id=3, name='LHS 3749', fort_status=1850, trigger=5974, um_status=0, undermine=0.0, distance=55.72, notes='', sheet_col='I', sheet_order=3),
        System(id=4, name='Sol', fort_status=2500, trigger=5211, um_status=2250, undermine=0.0, distance=28.94, notes='Leave For Grinders', sheet_col='J', sheet_order=4),
        System(id=5, name='Dongkum', fort_status=7000, trigger=7239, um_status=0, undermine=0.0, distance=81.54, notes='', sheet_col='K', sheet_order=5),
        System(id=6, name='Alpha Fornacis', fort_status=0, trigger=6476, um_status=0, undermine=0.0, distance=67.27, notes='', sheet_col='L', sheet_order=6),
        System(id=7, name='Phra Mool', fort_status=0, trigger=7968, um_status=0, undermine=0.0, distance=93.02, notes='', sheet_col='M', sheet_order=7),
        System(id=26, name='Othime', fort_status=0, trigger=7367, um_status=0, undermine=0.0, distance=83.68, notes='Priority for S/M ships (no L pads)', sheet_col='AF', sheet_order=26),
        System(id=57, name='WW Piscis Austrini', fort_status=0, trigger=8563, um_status=0, undermine=0.0, distance=101.38, notes='', sheet_col='BK', sheet_order=57),
        System(id=58, name='LPM 229', fort_status=0, trigger=9479, um_status=0, undermine=0.0, distance=112.98, notes='', sheet_col='BL', sheet_order=58),
    ]
    session.add_all(systems)
    session.commit()

    yield systems

    for matched in session.query(System):
        session.delete(matched)
    session.commit()


@pytest.fixture
def f_prepsystem(session):
    prep = PrepSystem(id=100, name='Rhea', trigger=10000, fort_status=5100, um_status=0,
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
    systems = [
        UMControl(id=1, name='Cemplangpa', sheet_col='D', goal=14878, security='Medium', notes='', progress_us=15000, progress_them=1.0, close_control='Sol', map_offset=1380),
        UMControl(id=2, name='Pequen', sheet_col='F', goal=12500, security='Anarchy', notes='', progress_us=10500, progress_them=0.5, close_control='Atropos', map_offset=0),
        UMExpand(id=3, name='Burr', sheet_col='H', goal=364298, security='Low', notes='', progress_us=161630, progress_them=35.0, close_control='Dongkum', map_offset=76548),
        UMOppose(id=4, name='AF Leopris', sheet_col='J', goal=59877, security='Low', notes='', progress_us=47739, progress_them=1.69, close_control='Atropos', map_offset=23960),
        UMControl(id=5, name='Empty', sheet_col='K', goal=10000, security='Medium', notes='', progress_us=0, progress_them=0.0, close_control='Rana', map_offset=0),
    ]
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


@pytest.fixture
def f_admins(session):
    """
    Fixture to insert some test admins.

    Depends on: f_dusers
    """
    admins = (
        Admin(id="1000", date=datetime.datetime(2017, 9, 26, 13, 34, 39, 721018)),
        Admin(id="1001", date=datetime.datetime(2017, 9, 26, 13, 34, 48, 327031)),
    )
    session.add_all(admins)
    session.commit()

    yield admins

    for matched in session.query(Admin):
        session.delete(matched)
    session.commit()


@pytest.fixture
def f_cperms(session):
    """ Channel perms fixture. """
    perms = (
        ChannelPerm(cmd="Drop", server="Gears Hideout", channel="operations"),
    )
    session.add_all(perms)
    session.commit()

    yield perms

    for matched in session.query(ChannelPerm):
        session.delete(matched)
    session.commit()


@pytest.fixture
def f_rperms(session):
    """ Role perms fixture. """
    perms = (
        RolePerm(cmd="Drop", server="Gears Hideout", role="FRC Member"),
    )
    session.add_all(perms)
    session.commit()

    yield perms

    for matched in session.query(RolePerm):
        session.delete(matched)
    session.commit()


@pytest.fixture
def f_fortorders(session):
    """ Fort order fixture. """
    systems = (
        FortOrder(order=1, system_name='Sol'),
        FortOrder(order=2, system_name='LPM 229'),
        FortOrder(order=3, system_name='Othime'),
    )
    session.add_all(systems)
    session.commit()

    yield systems

    for matched in session.query(FortOrder):
        session.delete(matched)
    session.commit()


@pytest.fixture
def f_testbed(f_dusers, f_sheets, f_systems, f_prepsystem, f_systemsum, f_drops, f_holds):

    yield [f_dusers, f_sheets, f_systems, f_prepsystem, f_systemsum, f_drops, f_holds]


@pytest.fixture()
def mock_fortsheet(db_cleanup):
    fake_sheet = mock.Mock()
    fake_sheet.whole_sheet.return_value = CELLS_FORT
    fake_sheet.get_with_formatting.return_value = copy.deepcopy(CELLS_FORT_FMT)

    yield fake_sheet


@pytest.fixture()
def mock_umsheet(db_cleanup):
    fake_sheet = mock.Mock()
    fake_sheet.whole_sheet.return_value = CELLS_UM

    return fake_sheet


# Fake objects look like discord data classes
class FakeObject(object):
    """
    A fake class to impersonate Data Classes from discord.py
    """
    oid = 0

    @classmethod
    def next_id(cls):
        cls.oid += 1
        return '{}-{}'.format(cls.__name__, cls.oid)

    def __init__(self, name, id=None):
        if not id:
            id = self.__class__.next_id()
        self.id = id
        self.name = name

    def __repr__(self):
        return "{}: {} {}".format(self.__class__.__name__, self.id, self.name)


class Server(FakeObject):
    def __init__(self, name, id=None):
        super().__init__(name, id)
        self.channels = []

    def add(self, channel):
        self.channels.append(channel)

    def __repr__(self):
        channels = "\n  Channels: " + ", ".join([cha.name for cha in self.channels])
        return super().__repr__() + channels


class Channel(FakeObject):
    def __init__(self, name, *, srv=None, id=None):
        super().__init__(name, id)
        self.server = srv

    def __repr__(self):
        return super().__repr__() + ", Server: {}".format(self.server.name)


class Member(FakeObject):
    def __init__(self, name, roles, *, id=None):
        super().__init__(name, id)
        self.display_name = self.name
        self.roles = roles

    @property
    def mention(self):
        return self.display_name

    def __repr__(self):
        roles = "Roles:  " + ", ".join([rol.name for rol in self.roles])
        return super().__repr__() + ", Display: {} ".format(self.display_name) + roles


class Role(FakeObject):
    def __init__(self, name, srv=None, *, id=None):
        super().__init__(name, id)
        self.server = srv

    def __repr__(self):
        return super().__repr__() + "\n  {}".format(self.server)


class Message(FakeObject):
    def __init__(self, content, author, srv, channel, mentions, *, id=None):
        super().__init__(None, id)
        self.author = author
        self.channel = channel
        self.content = content
        self.mentions = mentions
        self.server = srv

    @property
    def timestamp(self):
        return datetime.datetime.utcnow()

    def __repr__(self):
        return super().__repr__() + "\n  Content: {}\n  Author: {}\n  Channel: {}\n  Server: {}".format(
            self.content, self.author, self.channel, self.server)


def fake_servers():
    """ Generate fake discord servers for testing. """
    srv = Server("Gears' Hideout")
    channels = [
        Channel("feedback", srv=srv),
        Channel("live_hudson", srv=srv),
        Channel("private_dev", srv=srv)
    ]
    for cha in channels:
        srv.add(cha)

    return [srv]


def fake_msg_gears(content):
    """ Generate fake message with GearsandCogs as author. """
    srv = fake_servers()[0]
    roles = [Role('Cookie Lord', srv), Role('Hudson', srv)]
    aut = Member("GearsandCogs", roles, id="1000")
    return Message(content, aut, srv, srv.channels[1], None)


def fake_msg_newuser(content):
    """ Generate fake message with GearsandCogs as author. """
    srv = fake_servers()[0]
    roles = [Role('Hudson', srv)]
    aut = Member("newuser", roles, id="1003")
    return Message(content, aut, srv, srv.channels[1], None)


@pytest.fixture
def f_bot():
    """
    Return a mocked bot.

    Bot must have methods:
        bot.send_message
        bot.send_ttl_message
        bot.delete_message
        bot.emoji.fix - EmojiResolver tested elsewhere
        bot.loop.run_in_executor, None, func, *args

    Bot must have attributes:
        bot.uptime
        bot.prefix
    """
    fake_bot = aiomock.AIOMock(uptime=5, prefix="!")
    fake_bot.send_message.async_return_value = None
    fake_bot.send_ttl_message.async_return_value = None
    fake_bot.delete_message.async_return_value = None
    fake_bot.emoji.fix = lambda x, y: x
    fake_bot.servers = fake_servers()

    def fake_exec(_, func, *args):
        return func(*args)
    fake_bot.loop.run_in_executor.async_side_effect = fake_exec

    yield fake_bot
