"""
Used for pytest fixtures and anything else test setup/teardown related.
"""
import copy
import datetime
import os
import sys

import aiofiles
import aiomock
import mock
import pytest
try:
    import uvloop
    LOOP = uvloop.new_event_loop
    L_SHOW = LOOP()
    L_SHOW.set_debug(True)
    print("Test loop policy:", str(L_SHOW))
    del L_SHOW
except ImportError:
    print("Missing: uvloop")
    sys.exit(1)

import cog.util
import cogdb
import cogdb.query
from cogdb.schema import (DiscordUser, FortSystem, FortPrep, FortDrop, FortUser, FortOrder,
                          UMSystem, UMExpand, UMOppose, UMUser, UMHold, KOS,
                          AdminPerm, ChannelPerm, RolePerm,
                          TrackSystem, TrackSystemCached, TrackByID)
from tests.data import CELLS_FORT, CELLS_FORT_FMT, CELLS_UM


@pytest.fixture(scope='function', autouse=True)
def around_all_tests(session):
    """
    Executes before and after EVERY test.

    Can be helpful for tracking bugs, like dirty database after test.
    Disabled unless needed. Non-trivial overhead.
    """
    start = datetime.datetime.now(datetime.timezone.utc)
    yield
    print(" Time", datetime.datetime.now(datetime.timezone.utc) - start, end="")

    classes = [DiscordUser, FortUser, FortSystem, FortDrop, UMSystem, UMUser, UMHold,
               KOS, AdminPerm, ChannelPerm, RolePerm,
               TrackSystem, TrackSystemCached, TrackByID]
    for cls in classes:
        assert not session.query(cls).all()


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
    with cogdb.session_scope(cogdb.Session, expire_on_commit=False) as session:
        yield session


@pytest.fixture
def side_session():
    with cogdb.session_scope(cogdb.SideSession) as session:
        yield session


@pytest.fixture
def eddb_session():
    with cogdb.session_scope(cogdb.EDDBSession) as session:
        yield session


@pytest.fixture
def db_cleanup(session):
    """
    Clean the whole database. Guarantee it is empty.
    Used when tests don't use a fixture.
    """
    yield

    cogdb.schema.empty_tables(session, perm=True)

    classes = [DiscordUser, FortUser, FortSystem, FortDrop, FortOrder, UMUser, UMSystem, UMHold,
               KOS, TrackSystem, TrackSystemCached, TrackByID]
    for cls in classes:
        assert session.query(cls).all() == []


@pytest.fixture
def f_dusers(session):
    """
    Fixture to insert some test DiscordUsers.
    """
    dusers = (
        DiscordUser(id=1, display_name='User1', pref_name='User1'),
        DiscordUser(id=2, display_name='User2', pref_name='User2'),
        DiscordUser(id=3, display_name='User3', pref_name='User3'),
    )
    session.add_all(dusers)
    session.commit()

    yield dusers

    session.rollback()
    session.query(DiscordUser).delete()
    session.commit()


@pytest.fixture
def f_dusers_many(session):
    """
    Fixture to insert many DiscordUsers to cover key constraints with scanners.
    """
    dusers = []
    for ind in range(1, 201):
        name = "User{}".format(ind)
        dusers += [DiscordUser(id=ind, display_name=name, pref_name=name)]
    session.add_all(dusers)
    session.commit()

    yield dusers

    session.rollback()
    session.query(DiscordUser).delete()
    session.commit()


@pytest.fixture
def f_fort_testbed(session):
    """
    Fixture to insert some test SheetRows.

    Returns: (users, systems, drops)
    """
    dusers = session.query(DiscordUser).all()
    assert dusers

    users = (
        FortUser(id=dusers[0].id, name=dusers[0].pref_name, row=15, cry='User1 are forting late!'),
        FortUser(id=dusers[1].id, name=dusers[1].pref_name, row=16, cry=''),
        FortUser(id=dusers[2].id, name=dusers[2].pref_name, row=17, cry='User3 is the boss'),
    )
    systems = (
        FortSystem(id=1, name='Frey', fort_status=4910, trigger=4910, fort_override=0.7, um_status=0, undermine=0.0, distance=116.99, notes='', sheet_col='G', sheet_order=1),
        FortSystem(id=2, name='Nurundere', fort_status=5422, trigger=8425, fort_override=0.6, um_status=0, undermine=0.0, distance=99.51, notes='', sheet_col='H', sheet_order=2),
        FortSystem(id=3, name='LHS 3749', fort_status=1850, trigger=5974, um_status=0, undermine=0.0, distance=55.72, notes='', sheet_col='I', sheet_order=3),
        FortSystem(id=4, name='Sol', fort_status=2500, trigger=5211, um_status=2250, undermine=0.0, distance=28.94, notes='Leave For Grinders', sheet_col='J', sheet_order=4),
        FortSystem(id=5, name='Dongkum', fort_status=7000, trigger=7239, um_status=0, undermine=0.0, distance=81.54, notes='', sheet_col='K', sheet_order=5),
        FortSystem(id=6, name='Alpha Fornacis', fort_status=0, trigger=6476, um_status=0, undermine=0.0, distance=67.27, notes='', sheet_col='L', sheet_order=6),
        FortSystem(id=7, name='Phra Mool', fort_status=0, trigger=7968, um_status=0, undermine=0.0, distance=93.02, notes='Skip it now', sheet_col='M', sheet_order=7),
        FortSystem(id=8, name='Othime', fort_status=0, trigger=7367, um_status=0, undermine=0.0, distance=83.68, notes='Priority for S/M ships (no L pads)', sheet_col='AF', sheet_order=26),
        FortSystem(id=9, name='WW Piscis Austrini', fort_status=0, trigger=8563, um_status=0, undermine=1.2, distance=101.38, notes='', sheet_col='BK', sheet_order=57),
        FortSystem(id=10, name='LPM 229', fort_status=0, trigger=9479, um_status=0, undermine=1.0, distance=112.98, notes='', sheet_col='BL', sheet_order=58),
        FortPrep(id=1000, name='Rhea', trigger=10000, fort_status=5100, um_status=0, undermine=0.0, distance=65.55, notes='Atropos', sheet_col='D', sheet_order=0)
    )
    drops = (
        FortDrop(id=1, amount=700, user_id=users[0].id, system_id=systems[0].id),
        FortDrop(id=2, amount=400, user_id=users[0].id, system_id=systems[1].id),
        FortDrop(id=3, amount=1200, user_id=users[1].id, system_id=systems[0].id),
        FortDrop(id=4, amount=1800, user_id=users[2].id, system_id=systems[0].id),
        FortDrop(id=5, amount=800, user_id=users[1].id, system_id=systems[1].id),
    )
    session.add_all(users + systems)
    session.flush()
    session.add_all(drops)
    session.commit()

    yield users, systems, drops

    session.rollback()
    for cls in (FortDrop, FortSystem, FortUser):
        session.query(cls).delete()
    session.commit()


@pytest.fixture
def f_um_testbed(session):
    """
    Fixture to insert some test Systems.

    Returns: (users, systems, holds)
    """
    dusers = session.query(DiscordUser).all()
    assert dusers

    users = (
        UMUser(id=dusers[0].id, name=dusers[0].pref_name, row=18, cry='We go pew pew!'),
        UMUser(id=dusers[1].id, name=dusers[1].pref_name, row=19, cry='Shooting time'),
    )
    systems = (
        UMSystem(id=1, name='Cemplangpa', sheet_col='D', goal=14878, security='Medium', notes='',
                 progress_us=15000, progress_them=1.0, close_control='Sol', priority='Medium',
                 map_offset=1380),
        UMSystem(id=2, name='Pequen', sheet_col='F', goal=12500, security='Anarchy', notes='',
                 progress_us=10500, progress_them=0.5, close_control='Atropos', priority='Low',
                 map_offset=0),
        UMExpand(id=3, name='Burr', sheet_col='H', goal=364298, security='Low', notes='',
                 progress_us=161630, progress_them=35.0, close_control='Dongkum', priority='Medium',
                 map_offset=76548),
        UMOppose(id=4, name='AF Leopris', sheet_col='J', goal=59877, security='Low', notes='',
                 progress_us=47739, progress_them=1.69, close_control='Atropos', priority='low',
                 map_offset=23960),
        UMSystem(id=5, name='Empty', sheet_col='K', goal=10000, security='Medium', notes='',
                 progress_us=0, progress_them=0.0, close_control='Rana', priority='Low',
                 map_offset=0),
    )
    holds = (
        UMHold(id=1, held=0, redeemed=4000, user_id=dusers[0].id, system_id=systems[0].id),
        UMHold(id=2, held=400, redeemed=1550, user_id=dusers[0].id, system_id=systems[1].id),
        UMHold(id=3, held=2200, redeemed=5800, user_id=dusers[0].id, system_id=systems[2].id),
        UMHold(id=4, held=450, redeemed=2000, user_id=dusers[1].id, system_id=systems[0].id),
        UMHold(id=5, held=2400, redeemed=0, user_id=dusers[1].id, system_id=systems[1].id),
        UMHold(id=6, held=0, redeemed=1200, user_id=dusers[1].id, system_id=systems[2].id),
    )
    session.add_all(users + systems)
    session.flush()
    session.add_all(holds)
    session.commit()

    yield users, systems, holds

    session.rollback()
    for cls in (UMHold, UMSystem, UMUser):
        session.query(cls).delete()
    session.commit()


@pytest.fixture
def f_admins(session):
    """
    Fixture to insert some test admins.

    Depends on: f_dusers
    """
    admins = (
        AdminPerm(id=1, date=datetime.datetime(2017, 9, 26, 13, 34, 39, 721018)),
        AdminPerm(id=2, date=datetime.datetime(2017, 9, 26, 13, 34, 48, 327031)),
    )
    session.add_all(admins)
    session.commit()

    yield admins

    session.query(AdminPerm).delete()
    session.commit()


@pytest.fixture
def f_cperms(session):
    """ Channel perms fixture. """
    perms = (
        ChannelPerm(cmd="Drop", server_id=10, channel_id=2001),
    )
    session.add_all(perms)
    session.commit()

    yield perms

    session.query(ChannelPerm).delete()
    session.commit()


@pytest.fixture
def f_rperms(session):
    """ Role perms fixture. """
    perms = (
        RolePerm(cmd="Drop", server_id=10, role_id=3001),
    )
    session.add_all(perms)
    session.commit()

    yield perms

    session.query(RolePerm).delete()
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

    session.query(FortOrder).delete()
    session.commit()


@pytest.fixture
def f_kos(session):
    """
    Fixture to insert some test SheetRows.
    """
    kos_rows = (
        KOS(id=1, cmdr='good_guy', faction="Hudson", reason="Very good", is_friendly=1),
        KOS(id=2, cmdr='good_guy_pvp', faction="Hudson", reason="Very good pvp", is_friendly=1),
        KOS(id=3, cmdr='bad_guy', faction="Hudson", reason="Pretty bad guy", is_friendly=0),
    )
    session.add_all(kos_rows)
    session.commit()

    yield kos_rows

    session.query(KOS).delete()
    session.commit()


@pytest.fixture
def f_testbed(f_dusers, f_fort_testbed, f_um_testbed):

    yield [f_dusers, f_fort_testbed, f_um_testbed]


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
class FakeObject():
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

    def __str__(self):
        return "{}: {}".format(self.__class__.__name__, self.name)


class Server(FakeObject):
    def __init__(self, name, id=None):
        super().__init__(name, id)
        self.channels = []

    def add(self, channel):
        self.channels.append(channel)

    # def __repr__(self):
        # channels = "\n  Channels: " + ", ".join([cha.name for cha in self.channels])
        # return super().__repr__() + channels


class Channel(FakeObject):
    def __init__(self, name, *, srv=None, id=None):
        super().__init__(name, id)
        self.guild = srv
        self.all_delete_messages = []

    # def __repr__(self):
        # return super().__repr__() + ", Server: {}".format(self.server.name)

    async def delete_messages(self, messages):
        for msg in messages:
            msg.is_deleted = True
        self.all_delete_messages += messages


class Member(FakeObject):
    def __init__(self, name, roles, *, id=None):
        super().__init__(name, id)
        self.discriminator = '12345'
        self.display_name = self.name
        self.roles = roles

    @property
    def mention(self):
        return self.display_name

    # def __repr__(self):
        # roles = "Roles:  " + ", ".join([rol.name for rol in self.roles])
        # return super().__repr__() + ", Display: {} ".format(self.display_name) + roles


class Role(FakeObject):
    def __init__(self, name, srv=None, *, id=None):
        super().__init__(name, id)
        self.guild = srv

    # def __repr__(self):
        # return super().__repr__() + "\n  {}".format(self.server)


class Message(FakeObject):
    def __init__(self, content, author, srv, channel, mentions, *, id=None):
        super().__init__(None, id)
        self.author = author
        self.channel = channel
        self.content = content
        self.mentions = mentions
        self.guild = srv
        self.is_deleted = False

    # def __repr__(self):
        # return super().__repr__() + "\n  Content: {}\n  Author: {}\n  Channel: {}\n  Server: {}".format(
            # self.content, self.author, self.channel, self.server)

    @property
    def created_at(self):
        return datetime.datetime.now(datetime.timezone.utc)

    @property
    def edited_at(self):
        return datetime.datetime.now(datetime.timezone.utc)

    async def delete(self):
        self.is_deleted = True


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
    """ Generate fake message with User1 as author. """
    srv = fake_servers()[0]
    roles = [Role('Cookie Lord', srv), Role('Hudson', srv, id=3001)]
    aut = Member("User1", roles, id=1)
    return Message(content, aut, srv, srv.channels[1], None)


def fake_msg_newuser(content):
    """ Generate fake message with NewUser as author. """
    srv = fake_servers()[0]
    roles = [Role('Hudson', srv)]
    aut = Member("NewUser", roles, id=4)
    return Message(content, aut, srv, srv.channels[1], None)


@pytest.fixture
def f_bot():
    """
    Return a mocked bot.

    Bot must have methods:
        bot.send_message
        bot.send_long_message
        bot.send_ttl_message
        bot.delete_message
        bot.emoji.fix - EmojiResolver tested elsewhere
        bot.loop.run_in_executor, None, func, *args
        bot.get_member_by_substr

    Bot must have attributes:
        bot.uptime
        bot.prefix
    """
    member = aiomock.Mock()
    member.mention = "@Sidewinder40"
    fake_bot = aiomock.AIOMock(uptime=5, prefix="!")
    fake_bot.send_message.async_return_value = fake_msg_gears("A message to send.")
    fake_bot.send_ttl_message.async_return_value = fake_msg_gears("A ttl message to send.")
    fake_bot.send_long_message.async_return_value = fake_msg_gears("A long message to send.")
    fake_bot.get_member_by_substr.return_value = member
    fake_bot.wait_for.async_return_value = None  # Whenever wait_for needed, put message here.
    fake_bot.emoji.fix = lambda x, y: x
    fake_bot.guilds = fake_servers()
    fake_bot.get_channel_by_name.return_value = 'private_dev'

    def fake_exec(_, func, *args):
        return func(*args)
    fake_bot.loop.run_in_executor.async_side_effect = fake_exec

    cog.util.BOT = fake_bot

    yield fake_bot

    cog.util.BOT = None


@pytest.fixture
def f_asheet():
    """
    Return a mocked AsyncGSheet object.

    This is a VERY fake object with real methods that can do simple things:
        - pull in data from a filename
        - determine last row/column
        - fake the transmissions but store sent
    """
    asheet = aiomock.Mock()
    asheet.worksheet = aiomock.Mock()
    asheet.filename = None

    async def batch_update_send_(dicts, value):
        asheet.batch_update_sent = dicts

    async def batch_update_get_(*args, dim='', value_render=''):
        return asheet.batch_get.async_return_value

    async def init_():
        asheet.init_called = True

    async def values_col_(ind):
        async with aiofiles.open(asheet.filename, 'r') as fin:
            cells = eval(await fin.read())
            return cog.util.transpose_table(cells)[ind]

    async def values_row_(ind):
        async with aiofiles.open(asheet.filename, 'r') as fin:
            cells = eval(await fin.read())
            return cells[ind]

    async def whole_sheet_():
        async with aiofiles.open(asheet.filename, 'r') as fin:
            return eval(await fin.read())

    asheet.batch_get.async_return_value = None
    asheet.batch_update = batch_update_send_
    asheet.batch_get = batch_update_get_
    asheet.init_sheet = init_
    asheet.cells_get_range.async_return_value = None
    asheet.cells_updatea.async_return_value = None
    asheet.values_col = values_col_
    asheet.values_row = values_row_
    asheet.whole_sheet = whole_sheet_

    yield asheet


@pytest.fixture
def f_asheet_fortscanner(f_asheet):
    """
    Return a mocked AsyncGSheet for the fortscanner.
    """
    f_asheet.filename = cog.util.rel_to_abs('tests', 'test_input.fortscanner.txt')

    yield f_asheet


@pytest.fixture
def f_asheet_umscanner(f_asheet):
    """
    Return a mocked AsyncGSheet for the fortscanner.
    """
    f_asheet.filename = cog.util.rel_to_abs('tests', 'test_input.umscanner.txt')

    yield f_asheet


@pytest.fixture
def f_asheet_kos(f_asheet):
    """
    Return a mocked AsyncGSheet for the fortscanner.
    """
    f_asheet.filename = cog.util.rel_to_abs('tests', 'test_input.kos.txt')

    yield f_asheet


@pytest.fixture
def f_track_testbed(session):
    """
    Setup the database with dummy data for track tests.
    """
    track_systems = (
        TrackSystem(system="Rhea", distance=15),
        TrackSystem(system="Nanomam", distance=15),
    )
    track_systems_cached = (
        TrackSystemCached(system="44 chi Draconis"),
        TrackSystemCached(system="Acihaut"),
        TrackSystemCached(system="Bodedi"),
        TrackSystemCached(system="DX 799"),
        TrackSystemCached(system="G 239-25"),
        TrackSystemCached(system="Lalande 18115"),
        TrackSystemCached(system="LFT 880"),
        TrackSystemCached(system="LHS 1885"),
        TrackSystemCached(system="LHS 215"),
        TrackSystemCached(system="LHS 221"),
        TrackSystemCached(system="LHS 2459"),
        TrackSystemCached(system="LHS 246"),
        TrackSystemCached(system="LHS 262"),
        TrackSystemCached(system="LHS 283"),
        TrackSystemCached(system="LHS 6128"),
        TrackSystemCached(system="LP 5-88"),
        TrackSystemCached(system="LP 64-194"),
        TrackSystemCached(system="Nang Ta-khian"),
        TrackSystemCached(system="Nanomam"),
        TrackSystemCached(system="Tollan"),
        TrackSystemCached(system="Amun"),
        TrackSystemCached(system="BD-13 2439"),
        TrackSystemCached(system="LP 726-6"),
        TrackSystemCached(system="LQ Hydrae"),
        TrackSystemCached(system="Masans"),
        TrackSystemCached(system="Orishpucho"),
        TrackSystemCached(system="Rhea"),
        TrackSystemCached(system="Santal"),
    )
    date = datetime.datetime(year=2000, month=1, day=10, hour=0, minute=0, second=0, microsecond=0)
    days_2 = datetime.timedelta(days=2)
    track_ids = (
        TrackByID(id="J3J-WVT", squad="CLBF", updated_at=date),
        TrackByID(id="XNL-3XQ", squad="CLBF", updated_at=date),
        TrackByID(id="J3N-53B", squad="CLBF", updated_at=date + days_2),
        TrackByID(id="OVE-111", squad="Manual", override=True, updated_at=date + days_2),
    )
    session.add_all(track_systems + track_systems_cached + track_ids)
    session.commit()

    yield track_systems, track_systems_cached, track_ids

    session.rollback()
    for cls in (TrackSystem, TrackSystemCached, TrackByID):
        session.query(cls).delete()
    session.commit()
