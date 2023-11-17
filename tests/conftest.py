# pylint: disable=redefined-outer-name,missing-function-docstring,unused-argument,redefined-builtin,missing-class-docstring
"""
Used for pytest fixtures and anything else test setup/teardown related.
"""
import copy
import datetime
import os
import pathlib
import shutil
import sys
import tempfile

import aiofiles
import aiomock
import mock
import pytest
import sqlalchemy.orm as sql_orm
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
import cogdb.eddb
from cogdb.eddb import (SModule, SModuleGroup, SModuleSold, SCommodity, SCommodityGroup, SCommodityPricing)
from cogdb.schema import (DiscordUser, FortSystem, FortPrep, FortDrop, FortUser, FortOrder,
                          UMSystem, UMExpand, UMOppose, UMUser, UMHold, EUMSheet, KOS,
                          AdminPerm, ChannelPerm, RolePerm,
                          TrackSystem, TrackSystemCached, TrackByID,
                          Global, Vote, EVoteType,
                          Consolidation, SheetRecord)
import pvp.schema
from pvp.schema import (PVPCmdr, PVPKill, PVPDeath, PVPDeathKiller, PVPInterdicted, PVPInterdiction,
                        PVPInterdictedKill, PVPInterdictedDeath, PVPInterdictionKill, PVPInterdictionDeath,
                        PVPEscapedInterdicted, PVPLocation, PVPLog, PVPMatch, PVPMatchPlayer, PVPMatchState,
                        PVPInara, PVPInaraSquad)
from tests.data import CELLS_FORT, CELLS_FORT_FMT, CELLS_UM


GITHUB_FAIL = pytest.mark.skipif(os.environ.get('GITHUB') == "True", reason="Failing only on github CI.")
PVP_TIMESTAMP = 1671655377
REASON_SLOW = 'Slow as blocking to sheet. To enable, ensure os.environ ALL_TESTS=True'
SHEET_TEST = pytest.mark.skipif(not os.environ.get('ALL_TESTS'), reason=REASON_SLOW)
PROC_TEST = SHEET_TEST


@pytest.fixture(scope='function', autouse=True)
def around_all_tests():
    """
    Executes before and after EVERY test.

    Can be helpful for tracking bugs, like dirty database after test.
    Disabled unless needed. Non-trivial overhead.
    """
    start = datetime.datetime.utcnow()

    yield

    print(" Time", datetime.datetime.utcnow() - start, end="")

    # FIXME: Bit of a hack to prevent unclosed sessions from impeding assertions
    sql_orm.close_all_sessions()
    with cogdb.session_scope(cogdb.Session) as session:
        session.query(Global).delete()

    # To be used to find tests not cleaning up
    #  classes = [
        # DiscordUser, FortUser, FortSystem, FortDrop, FortOrder,
        #  UMSystem, UMUser, UMHold,
        #  KOS, AdminPerm, ChannelPerm, RolePerm,
        #  TrackSystem, TrackSystemCached, TrackByID
    # ]
        #  for cls in classes:
            #  assert not session.query(cls).all()

    #  with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        #  assert eddb_session.query(cogdb.eddb.Ship).all()


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
    with cogdb.session_scope(cogdb.Session) as session:
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
def db_cleanup():
    """
    Clean the whole database. Guarantee it is empty.
    Used when tests don't use a fixture.
    """
    yield

    with cogdb.session_scope(cogdb.Session) as session:
        cogdb.schema.empty_tables(session, perm=True)

        classes = [DiscordUser, FortUser, FortSystem, FortDrop, FortOrder, UMUser, UMSystem, UMHold,
                   KOS, TrackSystem, TrackSystemCached, TrackByID, AdminPerm, ChannelPerm, RolePerm]
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
        name = f"User{ind}"
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
        FortPrep(id=1000, name='Rhea', trigger=10000, fort_status=5100, um_status=0, undermine=0.0, distance=65.55, notes='Atropos', sheet_col='D', sheet_order=0),
        FortPrep(id=1001, name='PrepDone', trigger=10000, fort_status=12500, um_status=0, undermine=0.0, distance=65.55, notes='Atropos', sheet_col='E', sheet_order=0),
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
        UMUser(id=dusers[2].id, name=dusers[2].pref_name, sheet_src=EUMSheet.snipe, row=18, cry='Sniping away'),
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
        UMSystem(id=6, name='LeaveIt', sheet_col='L', goal=10000, security='Medium', notes='',
                 progress_us=9000, progress_them=0.0, close_control='Rana', priority='Leave For Now',
                 map_offset=0, sheet_src=EUMSheet.main),
        UMSystem(id=10007, name='ToSnipe', sheet_col='D', goal=100000, security='Medium', notes='',
                 progress_us=0, progress_them=0.0, close_control='Rana', priority='Low',
                 map_offset=0, sheet_src=EUMSheet.snipe),
    )
    holds = (
        UMHold(id=1, held=0, redeemed=4000, user_id=dusers[0].id, system_id=systems[0].id),
        UMHold(id=2, held=400, redeemed=1550, user_id=dusers[0].id, system_id=systems[1].id),
        UMHold(id=3, held=2200, redeemed=5800, user_id=dusers[0].id, system_id=systems[2].id),
        UMHold(id=4, held=450, redeemed=2000, user_id=dusers[1].id, system_id=systems[0].id),
        UMHold(id=5, held=2400, redeemed=0, user_id=dusers[1].id, system_id=systems[1].id),
        UMHold(id=6, held=0, redeemed=1200, user_id=dusers[1].id, system_id=systems[2].id),
        UMHold(id=7, sheet_src=EUMSheet.snipe, held=5000, redeemed=1200, user_id=dusers[2].id, system_id=systems[-1].id),
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
        AdminPerm(id=1, date=datetime.datetime(2017, 9, 26, 13, 34, 39)),
        AdminPerm(id=2, date=datetime.datetime(2017, 9, 26, 13, 34, 48)),
    )
    session.add_all(admins)
    session.commit()

    yield admins

    session.rollback()
    session.query(AdminPerm).delete()
    session.commit()


@pytest.fixture
def f_cperms(session):
    """ Channel perms fixture. """
    perms = (
        ChannelPerm(cmd="drop", guild_id=10, channel_id=2001),
    )
    session.add_all(perms)
    session.commit()

    yield perms

    session.rollback()
    session.query(ChannelPerm).delete()
    session.commit()


@pytest.fixture
def f_rperms(session):
    """ Role perms fixture. """
    perms = (
        RolePerm(cmd="drop", guild_id=10, role_id=3001),
    )
    session.add_all(perms)
    session.commit()

    yield perms

    session.rollback()
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

    session.rollback()
    session.query(FortOrder).delete()
    session.commit()


@pytest.fixture
def f_kos(session):
    """
    Fixture to insert some test SheetRows.
    """
    kos_rows = (
        KOS(id=1, cmdr='good_guy', squad="Hudson", reason="Very good", is_friendly=1),
        KOS(id=2, cmdr='good_guy_pvp', squad="Hudson", reason="Very good pvp", is_friendly=1),
        KOS(id=3, cmdr='bad_guy', squad="Hudson", reason="Pretty bad guy", is_friendly=0),
        KOS(id=5, cmdr='BadGuy', squad="Hudson", reason="Pretty badder guy", is_friendly=0),
    )
    session.add_all(kos_rows)
    session.commit()

    yield kos_rows

    session.rollback()
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
        return cls.oid

    def __init__(self, name, id=None):
        if not id:
            id = self.__class__.next_id()
        self.id = id
        self.name = name

    def __repr__(self):
        return f"{self.__class__.__name__}: {self.id} {self.name}"

    def __str__(self):
        return f"{self.__class__.__name__}: {self.name}"


# TODO: Rename Guild.
class Guild(FakeObject):
    def __init__(self, name, id=None):
        super().__init__(name, id)
        self.channels = []
        self.roles = []
        self.emojis = []
        self.mapped = {
            1: Member('User1', [Role('FRC Recruit'), Role("Test")]),
            2: Member('User2', [Role('FRC Member'), Role("Nothing")]),
            3: Member('User3', [Role('FRC Recruit'), Role("Test")]),
        }

    def add(self, channel):
        self.channels.append(channel)

    def get_member(self, id):
        return self.mapped[id]

    @property
    def members(self):
        return list(self.mapped.values())

    def get_channel(self, channel_id):
        try:
            return [channel for channel in self.channels if channel.id == channel_id][0]
        except IndexError:
            return self.channels[0]

    def get_role(self, role_id):
        return [role for role in self.roles if role.id == role_id][0]

    def get_member_named(self, nick):
        """
        Find name of user by string.

        Returns: A member if found at least one (first one) else return None.
        """
        found = [x for x in self.mapped.values() if nick in x.display_name]
        return found[0] if found else None

    # def __repr__(self):
        # channels = "\n  Channels: " + ", ".join([cha.name for cha in self.channels])
        # return super().__repr__() + channels


class Emoji(FakeObject):
    def __str__(self):
        return f"[{self.name}]"


class Channel(FakeObject):
    def __init__(self, name, *, srv=None, id=None):
        super().__init__(name, id)
        self.guild = srv
        self.all_delete_messages = []
        self.sent_messages = []

    # def __repr__(self):
        # return super().__repr__() + ", Server: {}".format(self.server.name)

    @property
    def mention(self):
        return self.name

    async def delete_messages(self, messages):
        for msg in messages:
            msg.is_deleted = True
        self.all_delete_messages += messages

    async def send(self, msg=None, **kwargs):
        self.sent_messages += [[msg, kwargs]]
        return Message(msg, None, self.guild, None)


class Member(FakeObject):
    def __init__(self, name, roles, *, id=None):
        super().__init__(name, id)
        self.discriminator = '12345'
        self.display_name = self.name
        self.roles = roles
        self.display_avatar = aiomock.Mock(url='placeholder')

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
    def __init__(self, content, author, srv, channel, *, id=None,
                 mentions=[], channel_mentions=[], role_mentions=[]):
        super().__init__(None, id)
        self.author = author
        self.channel = channel
        self.content = content
        self.mentions = mentions
        self.channel_mentions = channel_mentions
        self.role_mentions = role_mentions
        self.guild = srv
        self.is_deleted = False
        self.reaction = []

    # def __repr__(self):
        # return super().__repr__() + "\n  Content: {}\n  Author: {}\n  Channel: {}\n  Server: {}".format(
        # self.content, self.author, self.channel, self.server)

    @property
    def created_at(self):
        return datetime.datetime.utcnow()

    @property
    def edited_at(self):
        return datetime.datetime.utcnow()

    async def delete(self):
        self.is_deleted = True

    async def add_reaction(self, emote):
        self.reaction.append(emote)


class Interaction(FakeObject):
    """
    Fake interaction object for discord components.
    """
    def __init__(self, name, *, id=None, user=None, message=None, component=None, button=None, select=None):
        super().__init__(name, id)
        self.message = message
        self.user = user
        self.sent = []

        if button or select:
            component = aiomock.Mock()
            component.label = button if button else select
        self.component = component

        if button:
            self.data = {'custom_id': button}
        if select:
            self.data = {'values': [select]}

        self.response = aiomock.Mock()
        self.response.send_message = self.send

    async def send(self, *args, **kwargs):
        self.sent += args

    async def send_message(self, *args, **kwargs):
        self.sent += args


def fake_servers():
    """ Generate fake discord servers for testing. """
    srv = Guild("Gears' Hideout", id=1)
    channels = [
        Channel("feedback", srv=srv, id=10),
        Channel("live_hudson", srv=srv, id=11),
        Channel("private_dev", srv=srv, id=12),
        Channel("ops_channel", srv=srv, id=cog.util.CONF.channels.ops)
    ]
    for cha in channels:
        srv.add(cha)

    return [srv]


def fake_msg_gears(content, *, mentions=[], channel_mentions=[], role_mentions=[]):
    """ Generate fake message with User1 as author. """
    srv = fake_servers()[0]
    roles = [Role('Cookie Lord', srv), Role('Hudson', srv, id=3001)]
    aut = Member("User1", roles, id=1)
    return Message(content, aut, srv, srv.channels[1], mentions=mentions, channel_mentions=channel_mentions, role_mentions=role_mentions)


def fake_msg_newuser(content, *, mentions=[], channel_mentions=[]):
    """ Generate fake message with NewUser as author. """
    srv = fake_servers()[0]
    roles = [Role('Hudson', srv)]
    aut = Member("NewUser", roles, id=4)
    return Message(content, aut, srv, srv.channels[1], mentions=mentions)


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
    fake_bot.get_member_by_substr.return_value = member
    fake_bot.wait_for.async_return_value = None  # Whenever wait_for needed, put message here.
    fake_bot.emoji.fix = lambda x, y: x
    fake_bot.guilds = fake_servers()
    fake_bot.get_channel_by_name.return_value = 'private_dev'
    fake_bot.get_channel.return_value = Channel('pvp_chan')

    # fake_bot.msgs = []
    #
    # async def send_message_(_, msg):
    #     fake_bot.msgs += msg
    # fake_bot.send_message.async_side_effect = send_message_

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

    async def update_cells_():
        return True

    asheet.batch_get.async_return_value = None
    asheet.batch_update = batch_update_send_
    asheet.batch_get = batch_update_get_
    asheet.init_sheet = init_
    asheet.cells_get_range.async_return_value = None
    asheet.cells_updatea.async_return_value = None
    asheet.values_col = values_col_
    asheet.values_row = values_row_
    asheet.whole_sheet = whole_sheet_
    asheet.update_cells = update_cells_

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
    Return a mocked AsyncGSheet for the umscanner.
    """
    f_asheet.filename = cog.util.rel_to_abs('tests', 'test_input.umscanner.txt')

    yield f_asheet


@pytest.fixture
def f_umformula_values(f_asheet):
    """
    Return the raw formula values of a UM Sheet.
    """
    with open(cog.util.rel_to_abs('tests', 'test_input.umformula.txt'), encoding='utf-8') as fin:
        yield eval(fin.read())


@pytest.fixture
def f_asheet_kos(f_asheet):
    """
    Return a mocked AsyncGSheet for the kosscanner.
    """
    f_asheet.filename = cog.util.rel_to_abs('tests', 'test_input.kos.txt')

    yield f_asheet


@pytest.fixture
def f_asheet_ocrscanner(f_asheet):
    """
    Return a mocked AsyncGSheet for the ocrscanner.
    """
    f_asheet.filename = cog.util.rel_to_abs('tests', 'test_input.ocrscanner.txt')

    yield f_asheet


@pytest.fixture
def f_track_testbed(session):
    """
    Setup the database with dummy data for track tests.
    """
    track_systems = (
        TrackSystem(system="Nanomam", distance=15),
        TrackSystem(system="Tollan", distance=12),
    )
    track_systems_cached = (
        TrackSystemCached(system="44 chi Draconis", overlaps_with="Nanomam"),
        TrackSystemCached(system="Acihaut", overlaps_with="Nanomam"),
        TrackSystemCached(system="Bodedi", overlaps_with="Nanomam, Tollan"),
        TrackSystemCached(system="DX 799", overlaps_with="Nanomam, Tollan"),
        TrackSystemCached(system="G 239-25", overlaps_with="Nanomam"),
        TrackSystemCached(system="Lalande 18115", overlaps_with="Nanomam"),
        TrackSystemCached(system="LFT 880", overlaps_with="Nanomam"),
        TrackSystemCached(system="LHS 1885", overlaps_with="Nanomam, Tollan"),
        TrackSystemCached(system="LHS 215", overlaps_with="Nanomam, Tollan"),
        TrackSystemCached(system="LHS 221", overlaps_with="Nanomam, Tollan"),
        TrackSystemCached(system="LHS 224", overlaps_with="Tollam"),
        TrackSystemCached(system="LHS 2459", overlaps_with="Nanomam"),
        TrackSystemCached(system="LHS 246", overlaps_with="Nanomam, Tollan"),
        TrackSystemCached(system="LHS 250", overlaps_with="Tollam"),
        TrackSystemCached(system="LHS 262", overlaps_with="Nanomam, Tollan"),
        TrackSystemCached(system="LHS 283", overlaps_with="Nanomam, Tollan"),
        TrackSystemCached(system="LHS 6128", overlaps_with="Nanomam, Tollan"),
        TrackSystemCached(system="LP 5-88", overlaps_with="Nanomam"),
        TrackSystemCached(system="LP 64-194", overlaps_with="Nanomam"),
        TrackSystemCached(system="Nang Ta-khian", overlaps_with="Nanomam"),
        TrackSystemCached(system="Nanomam", overlaps_with="Nanomam, Tollan"),
        TrackSystemCached(system="Tollan", overlaps_with="Tollan"),
    )
    date = datetime.datetime(year=2000, month=1, day=10, hour=0, minute=0, second=0, microsecond=0)
    days_2 = datetime.timedelta(days=2)
    track_ids = (
        TrackByID(id="J3J-WVT", squad="CLBF", system="Rana", last_system="Nanomam", updated_at=date),
        TrackByID(id="XNL-3XQ", override=True, squad="CLBF", system="Tollan", updated_at=date),
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


@pytest.fixture
def f_global_testbed(session):
    """
    Setup the database with dummy data for track tests.
    """
    date = datetime.datetime(2021, 8, 25, 2, 33, 0)
    db_globals = (
        Global(id=1, cycle=240, consolidation=77, updated_at=date),
    )
    session.add_all(db_globals)
    session.commit()

    yield db_globals

    session.rollback()
    for cls in (Global,):
        session.query(cls).delete()
    session.commit()


@pytest.fixture
def f_vote_testbed(session):
    """
    Setup the database with dummy data for vote tracker.
    """
    updated_at = datetime.datetime(2021, 8, 25, 2, 33, 0)
    a_min = datetime.timedelta(minutes=1)
    votes = (
        Vote(id=1, vote=EVoteType.cons, amount=1, updated_at=updated_at),
        Vote(id=1, vote=EVoteType.prep, amount=5, updated_at=updated_at + a_min),
        Vote(id=2, vote=EVoteType.cons, amount=3, updated_at=updated_at + a_min + a_min),
        Vote(id=2, vote=EVoteType.prep, amount=2, updated_at=updated_at + a_min + a_min + a_min),
    )
    session.add_all(votes)
    session.commit()

    yield votes

    session.rollback()
    for cls in (Vote,):
        session.query(cls).delete()
    session.commit()


@pytest.fixture
def f_cons_data(session):
    """
    Setup the database with dummy data for consolidation tracker.
    """
    updated_at = datetime.datetime.utcnow()
    delta = datetime.timedelta(hours=1)
    three = datetime.timedelta(hours=3)
    cons = (
        Consolidation(id=1, amount=66, updated_at=updated_at),
        Consolidation(id=2, amount=67, updated_at=updated_at + delta),
        Consolidation(id=3, amount=65, updated_at=updated_at + delta + delta),
        Consolidation(id=4, amount=64, updated_at=updated_at + three),
        Consolidation(id=5, amount=67, updated_at=updated_at + three + delta),
        Consolidation(id=6, amount=68, updated_at=updated_at + three + delta + delta),
    )
    session.add_all(cons)
    session.commit()

    yield cons

    session.rollback()
    for cls in (Consolidation,):
        session.query(cls).delete()
    session.commit()


@pytest.fixture
def f_sheet_records(session):
    records = (
        SheetRecord(id=1, discord_id=1, channel_id=10, command='!fort -s 555:444 Rana', sheet_src='fort'),
        SheetRecord(id=2, discord_id=1, channel_id=10, command='!drop 500 Rana', sheet_src='fort'),
    )
    session.add_all(records)
    session.commit()

    yield records

    session.rollback()
    session.query(SheetRecord).delete()
    session.commit()


@pytest.fixture
def f_pvp_clean():
    yield
    pvp.schema.empty_tables()


@pytest.fixture
def f_pvp_testbed(eddb_session):
    """
    Massive fixture intializes an entire dummy testbed of pvp objects.
    """
    try:
        eddb_session.add_all([
            PVPCmdr(id=1, name='coolGuy', hex='B20000', updated_at=PVP_TIMESTAMP),
            PVPCmdr(id=2, name='shyGuy', hex='B20000', updated_at=PVP_TIMESTAMP),
            PVPCmdr(id=3, name='shootsALot', hex='B20000', updated_at=PVP_TIMESTAMP),
            PVPCmdr(id=4, name='newbie', hex='B20000', updated_at=PVP_TIMESTAMP),
        ])
        eddb_session.commit()
        eddb_session.add_all([
            PVPInaraSquad(id=1, name='cool guys', updated_at=PVP_TIMESTAMP),
        ])
        eddb_session.flush()
        #  system_anja = eddb_session.query(cogdb.eddb.System).filter(System.name == 'Anja').one()
        eddb_session.add_all([
            PVPInara(id=1, squad_id=1, discord_id=1, name='CoolGuyYeah', updated_at=PVP_TIMESTAMP),
            PVPLocation(id=1, cmdr_id=1, system_id=1000, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP),
            PVPLocation(id=2, cmdr_id=1, system_id=1010, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP),
            PVPKill(id=1, cmdr_id=1, system_id=1000, victim_name='LeSuck', victim_rank=3, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP),
            PVPKill(id=2, cmdr_id=1, system_id=1000, victim_name='BadGuy', victim_rank=7, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP + 2),
            PVPKill(id=3, cmdr_id=1, system_id=1001, victim_name='LeSuck', victim_rank=3, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP + 4),
            PVPKill(id=4, cmdr_id=2, victim_name='CanNotShoot', victim_rank=8, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP),

            PVPDeath(id=1, cmdr_id=1, system_id=1000, is_wing_kill=True, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP),
            PVPDeath(id=2, cmdr_id=1, system_id=1001, is_wing_kill=False, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP + 2),
            PVPDeath(id=3, cmdr_id=2, is_wing_kill=False, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP),
            PVPDeathKiller(cmdr_id=1, pvp_death_id=1, name='BadGuyWon', rank=7, ship_id=30, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP),
            PVPDeathKiller(cmdr_id=1, pvp_death_id=1, name='BadGuyHelper', rank=5, ship_id=38, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP),
            PVPDeathKiller(cmdr_id=1, pvp_death_id=2, name='BadGuyWon', rank=7, ship_id=30, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP),
            PVPDeathKiller(cmdr_id=1, pvp_death_id=2, name='BadGuyHelper', rank=7, ship_id=30, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP),
            PVPDeathKiller(cmdr_id=2, pvp_death_id=3, name='BadGuyWon', rank=7, ship_id=30, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP),
            PVPDeathKiller(cmdr_id=2, pvp_death_id=3, name='LeSuck', rank=7, ship_id=22, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP),

            PVPInterdiction(id=1, cmdr_id=1, system_id=1000, is_player=True, is_success=True, survived=False,
                            victim_name="LeSuck", victim_rank=3, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP),
            PVPInterdiction(id=2, cmdr_id=1, system_id=1001, is_player=True, is_success=True, survived=True,
                            victim_name="LeSuck", victim_rank=3, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP + 2),
            PVPInterdiction(id=3, cmdr_id=2, system_id=1001, is_player=True, is_success=True, survived=True,
                            victim_name="CanNotShoot", victim_rank=3, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP + 2),

            PVPInterdicted(id=1, cmdr_id=1, system_id=1000, is_player=True, did_submit=False, survived=False,
                           interdictor_name="BadGuyWon", interdictor_rank=7, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP),
            PVPInterdicted(id=2, cmdr_id=2, is_player=True, did_submit=True, survived=True,
                           interdictor_name="BadGuyWon", interdictor_rank=7, created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP),
            PVPEscapedInterdicted(id=1, cmdr_id=1, system_id=1000, interdictor_name='BadGuyWon', is_player=True,
                                  created_at=PVP_TIMESTAMP, event_at=PVP_TIMESTAMP),
            PVPEscapedInterdicted(id=2, cmdr_id=1, system_id=1000, interdictor_name='BadGuyWon', is_player=True,
                                  created_at=PVP_TIMESTAMP + 2, event_at=PVP_TIMESTAMP + 2),
            PVPLog(id=1, cmdr_id=1, file_hash='hash', filename='first.log', msg_id=1, filtered_msg_id=10, updated_at=PVP_TIMESTAMP),
            PVPLog(id=2, cmdr_id=2, file_hash='hash2', filename='second.log', msg_id=3, filtered_msg_id=12, updated_at=PVP_TIMESTAMP + 2),
            PVPMatch(id=1, discord_channel_id=99, limit=10, state=PVPMatchState.SETUP, created_at=PVP_TIMESTAMP, updated_at=PVP_TIMESTAMP),
            PVPMatch(id=2, discord_channel_id=100, limit=20, state=PVPMatchState.FINISHED, created_at=PVP_TIMESTAMP + 2, updated_at=PVP_TIMESTAMP + 4),
            PVPMatch(id=3, discord_channel_id=101, limit=2, state=PVPMatchState.SETUP, created_at=PVP_TIMESTAMP, updated_at=PVP_TIMESTAMP + 4),
            PVPMatchPlayer(id=1, cmdr_id=1, match_id=1, team=0, won=False, updated_at=PVP_TIMESTAMP),
            PVPMatchPlayer(id=2, cmdr_id=2, match_id=1, team=0, won=False, updated_at=PVP_TIMESTAMP + 2),
            PVPMatchPlayer(id=3, cmdr_id=3, match_id=1, team=0, won=False, updated_at=PVP_TIMESTAMP + 4),
            PVPMatchPlayer(id=4, cmdr_id=1, match_id=3, team=0, won=False, updated_at=PVP_TIMESTAMP),
        ])
        eddb_session.flush()
        eddb_session.add_all([
            PVPInterdictionKill(cmdr_id=1, pvp_interdiction_id=1, pvp_kill_id=1),
            PVPInterdictionDeath(cmdr_id=1, pvp_interdiction_id=2, pvp_death_id=2),
            PVPInterdictedKill(cmdr_id=2, pvp_interdicted_id=2, pvp_kill_id=4),
            PVPInterdictedDeath(cmdr_id=1, pvp_interdicted_id=1, pvp_death_id=1),
        ])
        eddb_session.commit()

        yield
    finally:
        pvp.schema.empty_tables()


@pytest.fixture
def f_plog_file():
    """
    Create a valid log file temporarily and return the location.
    """
    with tempfile.NamedTemporaryFile(prefix='player', suffix='.log', mode='wb', delete=True) as tfile:
        tfile.writelines([
            b'{ "timestamp":"2016-06-10T14:31:00Z", "event":"Fileheader", "part":1, "gameversion":"2.2", "build":"r113684 " }, { "timestamp":"2016-06-10T14:32:03Z", "event":"LoadGame", "Commander":"HRC1", "Ship":"SideWinder", "ShipID":1, "GameMode":"Open", "Credits":600120, "Loan":0 }\n',
            b'{ "timestamp":"2016-06-10T14:50:00Z", "event":"Interdiction", "Success":true, "Interdicted":"cmdr CanNotShoot", "IsPlayer":true, "CombatRank":5 }\n',
            b'{ "timestamp":"2016-06-10T14:55:22Z", "event":"PVPKill", "Victim":"cmdr CanNotShoot", "CombatRank": 5}\n',
            b'{ "timestamp":"2016-06-10T14:35:00Z", "event":"FSDJump", "StarSystem":"Rana", "StarPos":[120.250,40.219,268.594], "JumpDist":36.034 }\n',
            b'{ "timestamp":"2016-06-10T14:36:10Z", "event":"FSDJump", "StarSystem":"Rhea", "StarPos":[120.719,34.188,271.750], "JumpDist":6.823 }\n',
        ])
        tfile.flush()

        yield pathlib.Path(tfile.name)


@pytest.fixture
def f_plog_zip():
    """
    Create a test zip with fake player logs and text files.
    The player logs are valid and can be parsed.
    """
    pat = pathlib.Path(tempfile.mkdtemp())
    archive = pat.parent.joinpath(pat.name + '.zip')
    file_pairs = [
        ['first.log', [
            b'{ "timestamp":"2016-06-10T14:31:00Z", "event":"Fileheader", "part":1, "gameversion":"2.2", "build":"r113684 " }, { "timestamp":"2016-06-10T14:32:03Z", "event":"LoadGame", "Commander":"HRC1", "Ship":"SideWinder", "ShipID":1, "GameMode":"Open", "Credits":600120, "Loan":0 }\n',
            b'{ "timestamp":"2016-06-10T14:50:00Z", "event":"Interdiction", "Success":true, "Interdicted":"cmdr CanNotShoot", "IsPlayer":true, "CombatRank":5 }\n',
            b'{ "timestamp":"2016-06-10T14:55:22Z", "event":"PVPKill", "Victim":"cmdr CanNotShoot", "CombatRank": 5}\n',
            b'{ "timestamp":"2016-06-10T14:35:00Z", "event":"FSDJump", "StarSystem":"Rana", "StarPos":[120.250,40.219,268.594], "JumpDist":36.034 }\n',
            b'{ "timestamp":"2016-06-10T14:36:10Z", "event":"FSDJump", "StarSystem":"Rhea", "StarPos":[120.719,34.188,271.750], "JumpDist":6.823 }\n',
        ]],
        ['second.log', [
            b'{ "timestamp":"2016-06-10T14:31:00Z", "event":"Fileheader", "part":1, "gameversion":"2.2", "build":"r113684 " }, { "timestamp":"2016-06-10T14:32:03Z", "event":"LoadGame", "Commander":"HRC1", "Ship":"SideWinder", "ShipID":1, "GameMode":"Open", "Credits":600120, "Loan":0 }\n',
            b'{ "timestamp":"2016-06-10T14:38:50Z", "event":"Scan", "BodyName":"Praea Euq NW-W b1-3 3", "Description":"Icy body with neon rich atmosphere and major water geysers volcanism" }\n',
            b'{ "timestamp":"2016-06-10T14:39:08Z", "event":"Scan", "BodyName":"Praea Euq NW-W b1-3 3 a", "Description":"Tidally locked Icy body" }\n',
            b'{ "timestamp":"2016-06-10T14:41:29Z", "event":"Docked", "StationName":"Beagle 2 Landing", "StationType":"Coriolis" }\n',
        ]],
        ['third.log', [
            b'{ "timestamp":"2016-06-10T14:31:00Z", "event":"Fileheader", "part":1, "gameversion":"2.2", "build":"r113684 " }, { "timestamp":"2016-06-10T14:32:03Z", "event":"LoadGame", "Commander":"HRC1", "Ship":"SideWinder", "ShipID":1, "GameMode":"Open", "Credits":600120, "Loan":0 }\n',
            b'{ "timestamp":"2016-06-10T14:32:15Z", "event":"Location", "StarSystem":"Asellus Primus", "StarPos":[-23.938,40.875,-1.344] }\n',
            b'{ "timestamp":"2016-06-10T14:35:00Z", "event":"FSDJump", "StarSystem":"HIP 78085", "StarPos":[120.250,40.219,268.594], "JumpDist":36.034 }\n',
        ]],
        ['test.txt', [b'This is a test file, it does nothing.\n']],
        ['works.fine', [b'This is a work file. It should not be read.\n']],
    ]
    try:
        for fname, lines in file_pairs:
            with open(pat.joinpath(fname), 'wb') as fout:
                fout.writelines(lines)
        shutil.make_archive(pat, 'zip', pat.parent, pat.name)

        yield archive
    finally:
        try:
            shutil.rmtree(pat)
        except OSError:
            pass
        try:
            os.remove(archive)
        except OSError:
            pass


@pytest.fixture
def f_plog_filtered(f_plog_zip, f_plog_file):
    """
    Create an example filtered archive, has a log and a zip of logs inside.
    """
    pat = pathlib.Path(tempfile.mkdtemp())
    archive = pat.parent.joinpath(pat.name + '.zip')
    try:
        shutil.copy(f_plog_zip, pat)
        shutil.copy(f_plog_file, pat)
        shutil.make_archive(pat, 'zip', pat.parent, pat.name)

        yield archive
    finally:
        try:
            shutil.rmtree(pat)
        except OSError:
            pass
        try:
            os.remove(archive)
        except OSError:
            pass
