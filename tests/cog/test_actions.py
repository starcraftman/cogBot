"""
Tests against the cog.actions module.
These tests act as integration tests, checking almost the whole path.
Importantly, I have stubbed/mocked everything to do with discord.py and the gsheets calls.

Important Note Regarding DB:
    After executing an action ALWAYS make a new Session(). The old one will still be stale.
"""
from __future__ import absolute_import, print_function
import datetime

import aiomock
import pytest

import cog.actions
import cog.bot
import cog.parse
import cogdb
from cogdb.side import SystemAge
from cogdb.schema import (DUser, SheetCattle, SheetUM,
                          System, SystemUM, Drop, Hold)


# Important, these get auto run to patch things
pytestmark = pytest.mark.usefixtures("patch_pool", "patch_scanners")


@pytest.fixture
def patch_pool():
    """ Patch the pool to silently ignore jobs. """
    old_pool = cog.jobs.POOL
    cog.jobs.POOL = aiomock.Mock()
    cog.jobs.POOL.schedule.return_value = None

    yield

    cog.jobs.POOL = old_pool


@pytest.fixture
def patch_scanners():
    """ Patch the scanners. """
    old_scanners = cog.actions.SCANNERS

    scanner = aiomock.Mock()
    scanner.update_system.return_value = None
    scanner.update_drop.return_value = None
    scanner.update_hold.return_value = None
    scanner.update_sheet_user.return_value = None
    cog.actions.SCANNERS = {'hudson_cattle': scanner, 'hudson_undermine': scanner}

    yield

    cog.actions.SCANNERS = old_scanners


# Fake objects look like discord
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

    yield fake_bot


def action_map(fake_message, fake_bot):
    """
    Test stub of part of CogBot.on_message dispatch.
    Notably, parses commands and returns Action based on parser cmd/subcmd.

    Exceute with Action.execute() coro or schedule on loop
    """
    parser = cog.parse.make_parser("!")
    args = parser.parse_args(fake_message.content.split(" "))
    cls = getattr(cog.actions, args.cmd)

    return cls(args=args, bot=fake_bot, msg=fake_message)


##################################################################
# Actual Tests

# TODO: Admin, some useless subcommands though.


# @pytest.mark.asyncio
# async def test_template(event_loop, f_bot):
    # msg = fake_msg_gears("!cmd")

    # await action_map(msg, f_bot).execute()

    # print(str(f_bot.send_message.call_args).replace("\\n", "\n"))


# General Parse Fails
@pytest.mark.asyncio
async def test_cmd_fail(event_loop, f_bot):
    msg = fake_msg_gears("!cmd")

    with pytest.raises(cog.exc.ArgumentParseError):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_req_help(event_loop, f_bot):
    msg = fake_msg_gears("!fort -h")

    with pytest.raises(cog.exc.ArgumentHelpError):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_invalid_flag(event_loop, f_bot):
    msg = fake_msg_gears("!fort --not_there")

    with pytest.raises(cog.exc.ArgumentParseError):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_bgs_age(side_session, event_loop, f_bot, f_systems):
    systems = [system.name for system in f_systems]
    row = side_session.query(SystemAge).filter(
        SystemAge.control.in_(systems)).order_by(SystemAge.system).first()
    msg = fake_msg_gears("!bgs age " + row.control)

    await action_map(msg, f_bot).execute()

    line = [word.strip() for word in str(f_bot.send_message.call_args).split('\\n')[2].split('|')]
    assert line == [row.control, row.system, str(row.age)]


@pytest.mark.asyncio
async def test_cmd_bgs_inf(side_session, event_loop, f_bot, f_systems):
    msg = fake_msg_gears("!bgs inf Sol")

    await action_map(msg, f_bot).execute()

    assert "Mother Gaia" in str(f_bot.send_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
async def test_cmd_feedback(event_loop, f_bot):
    msg = fake_msg_gears("!feedback Sample bug report.")

    await action_map(msg, f_bot).execute()

    assert str(f_bot.send_message.call_args).split("\\n")[-1][:-2] == 'Sample bug report.'


@pytest.mark.asyncio
async def test_cmd_fort(event_loop, f_bot, f_systems):
    msg = fake_msg_gears("!fort")

    await action_map(msg, f_bot).execute()

    expect = """__Active Targets__
**Nurundere** 5422/8425 :Fortifying:
**Othime**    0/7367 :Fortifying: Priority for S/M ships (no L pads)

__Next Targets__
**LHS 3749** 1850/5974 :Fortifying:
**Alpha Fornacis**    0/6476 :Fortifying:
**Phra Mool**    0/7968 :Fortifying:

__Almost Done__
**Dongkum** 7000/7239 :Fortifying: (239 left)"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_summary(event_loop, f_bot, f_systems):
    msg = fake_msg_gears("!fort --summary")

    await action_map(msg, f_bot).execute()

    expect = """```Cancelled|Fortified|Undermined|Skipped|Left
---------|---------|----------|-------|----
0/10     |1/10     |0/10      |1/10   |8/10```"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_next(event_loop, f_bot, f_systems):
    msg = fake_msg_gears("!fort --next 2")

    await action_map(msg, f_bot).execute()

    expect = """__Next Targets__
**LHS 3749** 1850/5974 :Fortifying:
**Alpha Fornacis**    0/6476 :Fortifying:"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_miss(event_loop, f_bot, f_systems):
    msg = fake_msg_gears("!fort --miss 1000")

    await action_map(msg, f_bot).execute()

    expect = """__Systems Missing 1000 Supplies__
**Dongkum** 7000/7239 :Fortifying: (239 left)"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_search(event_loop, f_bot, f_systems):
    msg = fake_msg_gears("!fort nuru, othime")

    await action_map(msg, f_bot).execute()

    expect = """__Search Results__
**Nurundere** 5422/8425 :Fortifying:
**Othime**    0/7367 :Fortifying: Priority for S/M ships (no L pads)"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_set(event_loop, f_bot, f_systems):
    msg = fake_msg_gears("!fort --set 7000:222 nuru")

    await action_map(msg, f_bot).execute()

    expect = """**Nurundere** 7000/8425 :Fortifying:, 222 :Undermining: (1425 left)"""
    f_bot.send_message.assert_called_with(msg.channel, expect)
    system = cogdb.Session().query(System).filter_by(name='Nurundere').one()
    assert system.fort_status == 7000
    assert system.um_status == 222


@pytest.mark.asyncio
async def test_cmd_fort_set_invalid(event_loop, f_bot, f_systems):
    msg = fake_msg_gears("!fort --set 7000:222 nuru, othime")

    with pytest.raises(cog.exc.InvalidCommandArgs):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_help(event_loop, f_bot):
    msg = fake_msg_gears("!help")

    await action_map(msg, f_bot).execute()

    assert "Here is an overview of my commands." in str(f_bot.send_ttl_message.call_args)


@pytest.mark.asyncio
async def test_cmd_drop_simple(event_loop, f_bot, f_testbed):
    msg = fake_msg_gears("!drop 578 nuru")

    await action_map(msg, f_bot).execute()

    f_bot.send_message.assert_called_with(msg.channel, '**Nurundere** 6000/8425 :Fortifying:')
    session = cogdb.Session()
    system = session.query(System).filter_by(name='Nurundere').one()
    assert system.current_status == 6000
    duser = session.query(DUser).filter_by(id=msg.author.id).one()
    cattle = session.query(SheetCattle).filter_by(name=duser.pref_name).one()
    drop = session.query(Drop).filter_by(user_id=cattle.id, system_id=system.id).one()
    assert drop.amount == 978


@pytest.mark.asyncio
async def test_cmd_drop_negative(event_loop, f_bot, f_testbed):
    msg = fake_msg_gears("!drop -100 nuru")

    await action_map(msg, f_bot).execute()

    f_bot.send_message.assert_called_with(msg.channel, '**Nurundere** 5322/8425 :Fortifying:')
    session = cogdb.Session()
    system = session.query(System).filter_by(name='Nurundere').one()
    assert system.current_status == 5322
    duser = session.query(DUser).filter_by(id=msg.author.id).one()
    cattle = session.query(SheetCattle).filter_by(name=duser.pref_name).one()
    drop = session.query(Drop).filter_by(user_id=cattle.id, system_id=system.id).one()
    assert drop.amount == 300


@pytest.mark.asyncio
async def test_cmd_drop_newuser(event_loop, f_bot, f_testbed):
    msg = fake_msg_newuser("!drop 500 nuru")

    await action_map(msg, f_bot).execute()

    expect = 'Automatically added newuser to cattle sheet. See !user command to change.'
    f_bot.send_message.assert_any_call(msg.channel, expect)
    f_bot.send_message.assert_any_call(msg.channel, '**Nurundere** 5922/8425 :Fortifying:')

    session = cogdb.Session()
    system = session.query(System).filter_by(name='Nurundere').one()
    assert system.current_status == 5922
    duser = session.query(DUser).filter_by(id=msg.author.id).one()
    sheet = session.query(SheetCattle).filter_by(name=duser.pref_name).one()
    drop = session.query(Drop).filter_by(user_id=sheet.id, system_id=system.id).one()
    assert drop.amount == 500


@pytest.mark.asyncio
async def test_cmd_drop_set(event_loop, f_bot, f_testbed):
    msg = fake_msg_gears("!drop 578 nuru --set 6500")

    await action_map(msg, f_bot).execute()

    f_bot.send_message.assert_called_with(msg.channel, '**Nurundere** 6500/8425 :Fortifying:')
    session = cogdb.Session()
    system = session.query(System).filter_by(name='Nurundere').one()
    assert system.current_status == 6500
    duser = session.query(DUser).filter_by(id=msg.author.id).one()
    cattle = session.query(SheetCattle).filter_by(name=duser.pref_name).one()
    drop = session.query(Drop).filter_by(user_id=cattle.id, system_id=system.id).one()
    assert drop.amount == 978


@pytest.mark.asyncio
async def test_cmd_hold_simple(event_loop, f_bot, f_testbed):
    msg = fake_msg_gears("!hold 2000 empt")

    await action_map(msg, f_bot).execute()

    expect = """Control: **Empty**, Security: Medium, Hudson Control: Rana
        Completion: 20%, Missing: 8000
"""
    f_bot.send_message.assert_called_with(msg.channel, expect)
    session = cogdb.Session()
    duser = session.query(DUser).filter_by(id=msg.author.id).one()
    um = session.query(SheetUM).filter_by(name=duser.pref_name).one()
    system = session.query(SystemUM).filter_by(name='Empty').one()
    assert system.missing == 8000
    hold = session.query(Hold).filter_by(user_id=um.id, system_id=system.id).one()
    assert hold.held == 2000
    assert hold.redeemed == 0


@pytest.mark.asyncio
async def test_cmd_hold_newuser(event_loop, f_bot, f_testbed):
    msg = fake_msg_newuser("!hold 1000 empty")

    await action_map(msg, f_bot).execute()

    expect = 'Automatically added newuser to undermine sheet. See !user command to change.'
    f_bot.send_message.assert_any_call(msg.channel, expect)
    expect2 = """Control: **Empty**, Security: Medium, Hudson Control: Rana
        Completion: 10%, Missing: 9000
"""
    f_bot.send_message.assert_any_call(msg.channel, expect2)

    session = cogdb.Session()
    system = session.query(SystemUM).filter_by(name='empty').one()
    assert system.missing == 9000
    duser = session.query(DUser).filter_by(id=msg.author.id).one()
    sheet = session.query(SheetUM).filter_by(name=duser.pref_name).one()
    hold = session.query(Hold).filter_by(user_id=sheet.id, system_id=system.id).one()
    assert hold.held == 1000


@pytest.mark.asyncio
async def test_cmd_hold_redeem(event_loop, f_bot, f_testbed):
    msg = fake_msg_gears("!hold --redeem")

    await action_map(msg, f_bot).execute()

    expect = 'You redeemed 2600 new merits.\nHolding 0, Redeemed 13950'
    f_bot.send_message.assert_called_with(msg.channel, expect)
    session = cogdb.Session()
    duser = session.query(DUser).filter_by(id=msg.author.id).one()
    um = session.query(SheetUM).filter_by(name=duser.pref_name).one()
    system = session.query(SystemUM).filter_by(name='Pequen').one()
    hold = session.query(Hold).filter_by(user_id=um.id, system_id=system.id).one()
    assert hold.held == 0
    assert hold.redeemed == 1950


@pytest.mark.asyncio
async def test_cmd_hold_died(event_loop, f_bot, f_testbed):
    msg = fake_msg_gears("!hold --died")

    await action_map(msg, f_bot).execute()

    f_bot.send_message.assert_called_with(msg.channel, 'Sorry you died :(. Held merits reset.')
    session = cogdb.Session()
    duser = session.query(DUser).filter_by(id=msg.author.id).one()
    um = session.query(SheetUM).filter_by(name=duser.pref_name).one()
    system = session.query(SystemUM).filter_by(name='Pequen').one()
    hold = session.query(Hold).filter_by(user_id=um.id, system_id=system.id).one()
    assert hold.held == 0
    assert hold.redeemed == 1550


@pytest.mark.asyncio
async def test_cmd_status(event_loop, f_bot):
    msg = fake_msg_gears("!status")

    await action_map(msg, f_bot).execute()

    expect = cog.tbl.wrap_markdown(cog.tbl.format_table([
        ['Created By', 'GearsandCogs'],
        ['Uptime', '5'],
        ['Version', '{}'.format(cog.__version__)],
        ['Contributors:', ''],
        ['    Shotwn', 'Inara search'],
    ]))
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_time(event_loop, f_bot):
    msg = fake_msg_gears("!time")

    await action_map(msg, f_bot).execute()

    assert "Game Time:" in str(f_bot.send_message.call_args)


@pytest.mark.asyncio
async def test_cmd_um(event_loop, f_bot, f_testbed):
    msg = fake_msg_gears("!um")

    await action_map(msg, f_bot).execute()

    expect = """__Current UM Targets__

Control: **Pequen**, Security: Anarchy, Hudson Control: Atropos
        Completion: 84%, Missing: 2000

Expanding: **Burr**, Security: Low, Hudson Control: Dongkum
        Behind by 3500%, Missing: 202668

Opposing expansion: **AF Leopris**, Security: Low, Hudson Control: Atropos
        Behind by 169%, Missing: 12138

Control: **Empty**, Security: Medium, Hudson Control: Rana
        Completion: 0%, Missing: 10000
"""

    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_um_search(event_loop, f_bot, f_testbed):
    msg = fake_msg_gears("!um burr")

    await action_map(msg, f_bot).execute()

    expect = """Expanding: **Burr**, Security: Low, Hudson Control: Dongkum
        Behind by 3500%, Missing: 202668
"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_um_set_fail(event_loop, f_bot, f_testbed):
    msg = fake_msg_gears("!um --set 5500")

    with pytest.raises(cog.exc.InvalidCommandArgs):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_um_set_works(session, event_loop, f_bot, f_testbed):
    before = session.query(SystemUM).filter_by(name='Pequen').one()
    msg = fake_msg_gears("!um --set {}:40 {} --offset 600".format(before.progress_us + 1500, before.name))

    await action_map(msg, f_bot).execute()

    expect = """Control: **Pequen**, Security: Anarchy, Hudson Control: Atropos
        Completion: 96%, Missing: 500
"""
    f_bot.send_message.assert_called_with(msg.channel, expect)
    after = cogdb.Session().query(SystemUM).filter_by(name='Pequen').one()
    assert after.progress_us == before.progress_us + 1500
    assert after.progress_them == 0.4
    assert after.map_offset == 600


@pytest.mark.asyncio
async def test_cmd_user(event_loop, f_bot, f_testbed):
    msg = fake_msg_gears("!user")

    await action_map(msg, f_bot).execute()

    expect = """__GearsandCogs__
Sheet Name: GearsandCogs
Default Cry:

__Hudson Cattle__
    Cry: Gears are forting late!
    Total: Dropped 1100
``` System   | Amount
--------- | ------
Frey      | 700
Nurundere | 400```
__Hudson UM__
    Cry: Gears are pew pew!
    Total: Holding 2600, Redeemed 11350
```  System   | Hold | Redeemed
---------- | ---- | --------
Cemplangpa | 0    | 4000
Pequen     | 400  | 1550
Burr       | 2200 | 5800```"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_user_set_name(session, event_loop, f_bot, f_testbed):
    new_name = "NotGears"
    msg = fake_msg_gears("!user --name " + new_name)

    await action_map(msg, f_bot).execute()

    expect = """__GearsandCogs__
Sheet Name: NotGears
Default Cry:

__Hudson Cattle__
    Cry: Gears are forting late!
    Total: Dropped 1100
``` System   | Amount
--------- | ------
Frey      | 700
Nurundere | 400```
__Hudson UM__
    Cry: Gears are pew pew!
    Total: Holding 2600, Redeemed 11350
```  System   | Hold | Redeemed
---------- | ---- | --------
Cemplangpa | 0    | 4000
Pequen     | 400  | 1550
Burr       | 2200 | 5800```"""
    f_bot.send_message.assert_called_with(msg.channel, expect)

    duser = session.query(DUser).filter_by(id=msg.author.id).one()
    assert duser.pref_name == new_name
    for sheet in duser.sheets(session):
        assert sheet.name == new_name


@pytest.mark.asyncio
async def test_cmd_user_set_cry(session, event_loop, f_bot, f_testbed):
    new_cry = "A new cry"
    msg = fake_msg_gears("!user --cry " + new_cry)

    await action_map(msg, f_bot).execute()

    expect = """__GearsandCogs__
Sheet Name: GearsandCogs
Default Cry: A new cry

__Hudson Cattle__
    Cry: A new cry
    Total: Dropped 1100
``` System   | Amount
--------- | ------
Frey      | 700
Nurundere | 400```
__Hudson UM__
    Cry: A new cry
    Total: Holding 2600, Redeemed 11350
```  System   | Hold | Redeemed
---------- | ---- | --------
Cemplangpa | 0    | 4000
Pequen     | 400  | 1550
Burr       | 2200 | 5800```"""

    f_bot.send_message.assert_called_with(msg.channel, expect)

    duser = session.query(DUser).filter_by(id=msg.author.id).one()
    assert duser.pref_cry == new_cry
    for sheet in duser.sheets(session):
        assert sheet.cry == new_cry
