"""
Tests against the cog.actions module.
These tests act as integration tests, checking almost the whole path.
Importantly, I have stubbed/mocked everything to do with discord.py and the gsheets calls.
"""
import os
import re
import shutil

import aiomock
import pytest
import sqlalchemy as sqla

import cog.actions
import cog.bot
import cog.parse
import cogdb
import cogdb.eddb
from cogdb.side import SystemAge
from cogdb.schema import (DiscordUser, FortSystem, FortDrop, FortUser,
                          FortOrder, UMSystem, UMUser, UMHold,
                          AdminPerm, RolePerm, ChannelPerm, Global,
                          TrackSystem, TrackSystemCached, TrackByID)

from tests.conftest import fake_msg_gears, fake_msg_newuser, Interaction
import tests.conftest as tc
from tests.cog.test_inara import INARA_TEST


# Important, any fixtures in here get auto run
pytestmark = pytest.mark.usefixtures("patch_scanners")


@pytest.fixture
def patch_scanners():
    """ Patch the scanners. """
    old_scanners = cog.actions.SCANNERS
    scanner = aiomock.Mock(user_row=5)

    async def send_batch_(payloads, *args, input_opt=''):
        scanner.payloads = payloads

    async def get_batch_(*args):
        return scanner._values

    async def update_cells_():
        return True

    def find_dupe_(_):
        return None, None

    mock_cls = aiomock.Mock()
    mock_cls.update_sheet_user_dict.return_value = [
        {'range': 'A20:B20', 'values': [['test cry', 'test name']]},
    ]
    scanner.__class__ = mock_cls
    scanner.send_batch = send_batch_
    scanner.get_batch = get_batch_
    scanner.find_dupe = find_dupe_
    scanner.update_cells = update_cells_
    cog.actions.SCANNERS = {
        'hudson_carriers': scanner,
        'hudson_cattle': scanner,
        'hudson_kos': scanner,
        'hudson_recruits': scanner,
        'hudson_snipe': scanner,
        'hudson_undermine': scanner,
    }

    yield scanner

    cog.actions.SCANNERS = old_scanners


def action_map(fake_message, fake_bot):
    """
    Test stub of part of CogBot.on_message dispatch.
    Notably, parses commands and returns Action based on parser cmd/subcmd.

    Exceute with Action.execute() coro or schedule on loop
    """
    parser = cog.parse.make_parser("!")
    args = parser.parse_args(fake_message.content.split(" "))
    cls = getattr(cog.actions, args.cmd)

    with cogdb.session_scope(cogdb.Session) as session:
        return cls(args=args, bot=fake_bot, msg=fake_message, session=session)


##################################################################
# Actual Tests

# TODO: Admin, some useless subcommands though.


#  @pytest.mark.asyncio
#  async def test_template(f_bot):
    #  msg = fake_msg_gears("!cmd")

    #  await action_map(msg, f_bot).execute()

    #  print(str(f_bot.send_message.call_args).replace("\\n", "\n"))


class FakeToWrap():
    def __init__(self):
        self.msg = aiomock.Mock(author=None, mentions=["FakeUser"])
        self.log = aiomock.AIOMock()
        self.bot = aiomock.AIOMock()
        self.bot.on_message.async_return_value = True

    @cog.actions.check_mentions
    async def execute(self):
        print("Hello")


@pytest.mark.asyncio
async def test_check_mentions():
    fakeself = FakeToWrap()
    await fakeself.execute()

    assert fakeself.msg.author
    fakeself.bot.on_message.assert_called_with(fakeself.msg)


# General Parse Fails
@pytest.mark.asyncio
async def test_cmd_fail(f_bot):
    msg = fake_msg_gears("!cmd")

    with pytest.raises(cog.exc.ArgumentParseError):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_req_help(f_bot):
    msg = fake_msg_gears("!fort -h")

    with pytest.raises(cog.exc.ArgumentHelpError):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_invalid_flag(f_bot):
    msg = fake_msg_gears("!fort --not_there")

    with pytest.raises(cog.exc.ArgumentParseError):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_admin_add_admin(f_dusers, f_admins, f_bot, db_cleanup, session):
    new_admin = tc.Member(f_dusers[-1].display_name, None, id=f_dusers[-1].id)
    msg = fake_msg_gears("!admin add", mentions=[new_admin])

    await action_map(msg, f_bot).execute()

    session.close()
    last = session.query(AdminPerm).order_by(AdminPerm.id.desc()).limit(1).one()
    assert last.id == 3


@pytest.mark.asyncio
async def test_cmd_admin_remove_admin(f_dusers, f_admins, f_bot, db_cleanup, session):
    new_admin = tc.Member(f_dusers[1].display_name, None, id=f_dusers[1].id)
    msg = fake_msg_gears("!admin remove", mentions=[new_admin])

    await action_map(msg, f_bot).execute()

    session.close()
    with pytest.raises(sqla.exc.NoResultFound):
        session.query(AdminPerm).filter(AdminPerm.id == f_dusers[1].id).limit(1).one()


@pytest.mark.asyncio
async def test_cmd_admin_add_chan(f_dusers, f_admins, f_bot, db_cleanup, session):
    srv = tc.fake_servers()[0]
    chan = srv.channels[1]
    msg = fake_msg_gears("!admin add bgs", channel_mentions=[chan])

    await action_map(msg, f_bot).execute()

    session.close()
    added = session.query(ChannelPerm).filter(ChannelPerm.cmd == 'bgs').one()
    assert added.cmd == "bgs"


@pytest.mark.asyncio
async def test_cmd_admin_add_chan_raises(f_dusers, f_admins, f_bot, db_cleanup, session):
    srv = tc.fake_servers()[0]
    chan = srv.channels[1]
    msg = fake_msg_gears("!admin add Invalid", channel_mentions=[chan])

    with pytest.raises(cog.exc.InvalidCommandArgs):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_admin_remove_chan(f_dusers, f_admins, f_bot, db_cleanup, session):
    srv = tc.fake_servers()[0]
    chan = srv.channels[1]

    msg = fake_msg_gears("!admin add bgs", channel_mentions=[chan])
    await action_map(msg, f_bot).execute()
    msg = fake_msg_gears("!admin remove bgs", channel_mentions=[chan])
    await action_map(msg, f_bot).execute()

    session.close()
    with pytest.raises(sqla.exc.NoResultFound):
        session.query(ChannelPerm).filter(ChannelPerm.cmd == 'bgs').one()


@pytest.mark.asyncio
async def test_cmd_admin_add_role(f_dusers, f_admins, f_bot, db_cleanup, session):
    srv = tc.fake_servers()[0]
    role = srv.get_member(1).roles[0]
    role.id = 90
    msg = fake_msg_gears("!admin add bgs", role_mentions=[role])

    await action_map(msg, f_bot).execute()

    session.close()
    added = session.query(RolePerm).filter(RolePerm.cmd == 'bgs').one()
    assert added.role_id == role.id


@pytest.mark.asyncio
async def test_cmd_admin_add_role_raises(f_dusers, f_admins, f_bot, db_cleanup, session):
    srv = tc.fake_servers()[0]
    role = srv.get_member(1).roles[0]
    role.id = 90
    msg = fake_msg_gears("!admin add Invalid", role_mentions=[role])

    with pytest.raises(cog.exc.InvalidCommandArgs):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_admin_remove_role(f_dusers, f_admins, f_bot, db_cleanup, session):
    srv = tc.fake_servers()[0]
    role = srv.get_member(1).roles[0]
    role.id = 90

    msg = fake_msg_gears("!admin add bgs", role_mentions=[role])
    await action_map(msg, f_bot).execute()
    msg = fake_msg_gears("!admin remove bgs", role_mentions=[role])
    await action_map(msg, f_bot).execute()

    session.close()
    with pytest.raises(sqla.exc.NoResultFound):
        session.query(ChannelPerm).filter(ChannelPerm.cmd == 'bgs').one()


@pytest.mark.asyncio
async def test_cmd_admin_removeum_fail(f_admins, f_bot, db_cleanup):
    msg = fake_msg_gears("!admin removeum Sol")

    await action_map(msg, f_bot).execute()

    expect = 'System Sol is not in the UM sheet.'
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_admin_removeum(f_bot, f_dusers, f_admins, f_um_testbed,
                                  f_umformula_values, patch_scanners, db_cleanup):
    fake_um = cog.actions.SCANNERS['hudson_undermine']
    fake_um._values = [f_umformula_values[4:]]

    msg = fake_msg_gears("!admin removeum Pequen")

    await action_map(msg, f_bot).execute()

    expected = [
        'Cemplangpa',
        '',
        'Albisiyatae',
        '',
        'Control System Template',
        '',
        'Expansion Template',
        '',
        '',
        ''
    ]
    assert fake_um.payloads[0]['values'][8] == expected

    expect = 'System Pequen removed from the UM sheet.'
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_admin_addum_fail(f_admins, f_bot, db_cleanup):
    msg = fake_msg_gears("!admin addum Cubeo")

    await action_map(msg, f_bot).execute()

    expect = 'All systems asked are already in the sheet or are invalid'
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_admin_addum(f_admins, f_bot, f_asheet_umscanner, patch_scanners, db_cleanup):
    fake_um = cog.actions.SCANNERS['hudson_undermine']
    fake_cols = await f_asheet_umscanner.whole_sheet()
    fake_cols = fake_cols[:13]
    fake_cols = [[columns[i] for columns in fake_cols] for i in range(17)]
    fake_cols = [fake_cols[3:]]
    fake_um._values = fake_cols

    msg = fake_msg_gears("!admin addum Kappa")

    await action_map(msg, f_bot).execute()

    expected_payloads = [{'range': 'N1:13', 'values': [
        ['', '', '', '', 'Opp. trigger', '% safety margin'],
        ['', '', '', '', '', '50%'],
        ['0', '', '0', '', '#DIV/0!', ''],
        [9790, '', '1,000', '', '0', ''],
        ['0', '', '0', '', '0', ''],
        ['1,000', '', '1,000', '', '0', ''],
        ['Sec: N/A', 'Zemina Torval', 'Sec: N/A', '', 'Sec: N/A', ''],
        ['', 'Normal', '', '', '', ''],
        ['Kappa', '', 'Control System Template', '', 'Expansion Template', ''],
        [0, '', '', '', '', ''], [0, '', '', '', '', ''],
        ['Held merits', 'Redeemed merits', 'Held merits', 'Redeemed merits', 'Held merits', 'Redeemed merits'],
        ['', '', '', '', '', '']]}]
    assert fake_um.payloads == expected_payloads

    expect = 'Systems added to the UM sheet.'
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_admin_top(f_dusers, f_admins, f_bot, f_fort_testbed, f_um_testbed):
    msg = fake_msg_gears("!admin top 3")

    await action_map(msg, f_bot).execute()

    expected = """
```Top 5 Recruits | Merits | Top 5 Members | Merits
-------------- | ------ | ------------- | ------
User1          | 15050  | User2         | 8050
User3          | 1800   |               |
               |        |               |```

```Top 5 Fort Recruits | Merits | Top Fort 5 Members | Merits
------------------- | ------ | ------------------ | ------
User3               | 1800   | User2              | 2000
User1               | 1100   |                    |
                    |        |                    |```

```Top 5 UM Recruits | Merits | Top UM 5 Members | Merits
----------------- | ------ | ---------------- | ------
User1             | 13950  | User2            | 6050
                  |        |                  |
                  |        |                  |```
"""
    assert expected in str(f_bot.send_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
async def test_cmd_bgs_age(side_session, eddb_session, f_bot, f_dusers, f_fort_testbed):
    row = side_session.query(SystemAge).\
        first()
    msg = fake_msg_gears("!bgs age " + row.control)

    await action_map(msg, f_bot).execute()

    line = [word.strip().replace("```')", '') for word in
            str(f_bot.send_message.call_args).split('\\n')[2].split('|')]
    assert line == [row.control, row.system, str(row.age)]


@pytest.mark.asyncio
async def test_cmd_bgs_dash(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!bgs dash Othime")

    await action_map(msg, f_bot).execute()

    assert 'Chelgit' in str(f_bot.send_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
async def test_cmd_bgs_edmc(side_session, f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!bgs edmc rana, frey")

    await action_map(msg, f_bot).execute()

    cap = str(f_bot.send_message.call_args).replace("\\n", "\n")
    assert "__Rana__" in cap
    assert "__Frey" in cap


@pytest.mark.asyncio
async def test_cmd_bgs_exp(f_bot):
    msg = fake_msg_gears("!bgs exp rana")

    f_bot.wait_for.async_return_value = fake_msg_gears('0')
    await action_map(msg, f_bot).execute()

    actual = str(f_bot.send_message.call_args).replace("\\n", "\n")[:-2]
    actual = re.sub(r'.*live_hudson, ["\']', '', actual)
    actual = actual.split('\n')
    for line in actual[6:]:
        parts = re.split(r'\s+\|\s+', line)
        assert float(parts[1]) <= 20


@pytest.mark.asyncio
async def test_cmd_bgs_expto(f_bot):
    msg = fake_msg_gears("!bgs expto rana")

    await action_map(msg, f_bot).execute()

    actual = str(f_bot.send_message.call_args).replace("\\n", "\n")[:-2]
    actual = re.sub(r'.*live_hudson, ["\']', '', actual)
    actual = actual.split('\n')

    for line in actual[4:]:
        if line == "```":
            continue

        parts = re.split(r'\s+\|\s+', line)
        assert float(parts[1]) <= 20


@pytest.mark.asyncio
async def test_cmd_bgs_find(f_bot):
    msg = fake_msg_gears("!bgs find nurundere")

    await action_map(msg, f_bot).execute()

    actual = str(f_bot.send_message.call_args).replace("\\n", "\n")[:-2]
    actual = re.sub(r'.*live_hudson, ["\']', '', actual)
    assert actual.split('\n')[4].index("Monarchy of Orisala") != -1


@pytest.mark.asyncio
async def test_cmd_bgs_inf(side_session, f_bot):
    msg = fake_msg_gears("!bgs inf Sol")

    await action_map(msg, f_bot).execute()

    assert "Mother Gaia" in str(f_bot.send_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
async def test_cmd_bgs_sys(side_session, f_bot):
    msg = fake_msg_gears("!bgs sys Sol")

    await action_map(msg, f_bot).execute()
    reply = str(f_bot.send_message.call_args).replace("\\n", "\n")

    assert '**Sol**: ' in reply
    assert r'```autohotkey' in reply
    assert '-> Demo | Mother Gaia:' in reply
    assert 'Owns: Abraham Lincoln (L)' in reply


@pytest.mark.asyncio
async def test_cmd_feedback(f_bot):
    msg = fake_msg_gears("!feedback Sample bug report.")

    await action_map(msg, f_bot).execute()

    assert str(f_bot.send_message.call_args).split("\\n")[-1][:-2] == 'Sample bug report.'


@pytest.mark.asyncio
async def test_cmd_fort(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!fort")

    await action_map(msg, f_bot).execute()

    expect = """__Active Targets__
Prep: **Rhea** 5100/10000 :Fortifying: Atropos - 65.55Ly
**Nurundere** 5422/8425 :Fortifying: - 99.51Ly

__Next Targets__
**LHS 3749** 1850/5974 :Fortifying: - 55.72Ly
**Alpha Fornacis**    0/6476 :Fortifying: - 67.27Ly
**WW Piscis Austrini**    0/8563 :Fortifying:, :Undermined: - 101.38Ly

__Priority Systems__
**Othime**    0/7367 :Fortifying: Priority for S/M ships (no L pads) - 83.68Ly

__Almost Done__
**Dongkum** 7000/7239 :Fortifying: (239 left) - 81.54Ly"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_summary(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!fort --summary")

    await action_map(msg, f_bot).execute()

    expect = """```Cancelled|Fortified|Undermined|Skipped|Left|Almost_done
---------|---------|----------|-------|----|-----------
0/10     |1/10     |2/10      |2/10   |4/10|1/10```"""

    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_next(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!fort --next 2")

    await action_map(msg, f_bot).execute()

    expect = """__Next Targets__
**LHS 3749** 1850/5974 :Fortifying: - 55.72Ly
**Alpha Fornacis**    0/6476 :Fortifying: - 67.27Ly"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_miss(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!fort --miss 1000")

    await action_map(msg, f_bot).execute()

    expect = """__Systems Missing 1000 Supplies__
**Dongkum** 7000/7239 :Fortifying: (239 left) - 81.54Ly"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_search(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!fort nuru, othime")

    await action_map(msg, f_bot).execute()

    expect = """__Search Results__
**Nurundere** 5422/8425 :Fortifying: - 99.51Ly
**Othime**    0/7367 :Fortifying: Priority for S/M ships (no L pads) - 83.68Ly"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_set(session, f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!fort --set 7000:222 nuru")

    await action_map(msg, f_bot).execute()

    expect = """**Nurundere** 7000/8425 :Fortifying:, 222 :Undermining: (1425 left) - 99.51Ly"""
    f_bot.send_message.assert_called_with(msg.channel, expect)

    session.commit()
    system = session.query(FortSystem).filter_by(name='Nurundere').one()
    assert system.fort_status == 7000
    assert system.um_status == 222

    expect = [
        {'range': 'H6:H7', 'values': [[7000], [222]]},
    ]
    assert cog.actions.SCANNERS['hudson_cattle'].payloads == expect


@pytest.mark.asyncio
async def test_cmd_fort_details(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!fort -d frey")

    await action_map(msg, f_bot).execute()

    expect = """**Frey**
```Completion  | 100.0%
CMDR Merits | 3700/4910
Fort Status | 4910/4910
UM Status   | 0 (0.00%)
Notes       |```
```CMDR Name | Merits
--------- | ------
User3     | 1800
User2     | 1200
User1     | 700```"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_details_invalid(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!fort -d")

    with pytest.raises(cog.exc.InvalidCommandArgs):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_fort_set_invalid(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!fort --set 7000:222 nuru, othime")

    with pytest.raises(cog.exc.InvalidCommandArgs):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_fort_order(session, f_bot, f_dusers, f_fort_testbed, f_fortorders):
    msg = fake_msg_gears("!fort --order lpm 229, nuru, frey")
    await action_map(msg, f_bot).execute()

    systems = [sys.system_name for sys in session.query(FortOrder).order_by(FortOrder.order)]
    assert systems == ['LPM 229', 'Nurundere', 'Frey']

    msg2 = fake_msg_gears("!fort")
    await action_map(msg2, f_bot).execute()

    expect = """__Active Targets (Manual Order)__
**LPM 229**    0/9479 :Fortifying:, :Undermined: - 112.98Ly

__Next Targets__
**Nurundere** 5422/8425 :Fortifying: - 99.51Ly

__Priority Systems__
**Othime**    0/7367 :Fortifying: Priority for S/M ships (no L pads) - 83.68Ly

__Almost Done__
**Dongkum** 7000/7239 :Fortifying: (239 left) - 81.54Ly"""
    f_bot.send_message.assert_called_with(msg2.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_order_next(session, f_bot, f_dusers, f_fort_testbed, f_fortorders):
    msg = fake_msg_gears("!fort --order sol, nuru, frey")
    await action_map(msg, f_bot).execute()

    systems = [sys.system_name for sys in session.query(FortOrder).order_by(FortOrder.order)]
    assert systems == ['Sol', 'Nurundere', 'Frey']

    msg2 = fake_msg_gears("!fort --next 2")
    await action_map(msg2, f_bot).execute()

    expect = """__Next Targets (Manual Order)__
**Nurundere** 5422/8425 :Fortifying: - 99.51Ly"""
    f_bot.send_message.assert_called_with(msg2.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_unset(session, f_bot, f_dusers, f_fort_testbed, f_fortorders):
    msg = fake_msg_gears("!fort --order sol, nuru, frey")
    await action_map(msg, f_bot).execute()

    systems = [sys.system_name for sys in session.query(FortOrder).order_by(FortOrder.order)]
    assert systems == ['Sol', 'Nurundere', 'Frey']

    msg2 = fake_msg_gears("!fort --order")
    await action_map(msg2, f_bot).execute()

    session.commit()
    systems = [sys.system_name for sys in session.query(FortOrder).order_by(FortOrder.order)]
    assert systems == []


@pytest.mark.asyncio
async def test_cmd_help(f_bot):
    msg = fake_msg_gears("!help")

    await action_map(msg, f_bot).execute()

    assert "Here is an overview of my commands." in str(f_bot.send_ttl_message.call_args)
    assert msg.is_deleted


@pytest.mark.asyncio
async def test_cmd_drop_simple(session, f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!drop 578 nuru")

    await action_map(msg, f_bot).execute()

    f_bot.send_message.assert_called_with(msg.channel, '**Nurundere** 6000/8425 :Fortifying: - 99.51Ly')

    system = session.query(FortSystem).filter_by(name='Nurundere').one()
    assert system.current_status == 6000
    duser = session.query(DiscordUser).filter_by(id=msg.author.id).one()
    cattle = session.query(FortUser).filter_by(name=duser.pref_name).one()
    drop = session.query(FortDrop).filter_by(user_id=cattle.id, system_id=system.id).one()
    assert drop.amount == 978

    expect = [
        {'range': 'H6:H7', 'values': [[6000], [0]]},
        {'range': 'H15:H15', 'values': [[978]]},
    ]
    assert cog.actions.SCANNERS['hudson_cattle'].payloads == expect


@pytest.mark.asyncio
async def test_cmd_drop_negative(session, f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!drop -100 nuru")

    await action_map(msg, f_bot).execute()

    f_bot.send_message.assert_called_with(msg.channel, '**Nurundere** 5322/8425 :Fortifying: - 99.51Ly')

    system = session.query(FortSystem).filter_by(name='Nurundere').one()
    assert system.current_status == 5322
    duser = session.query(DiscordUser).filter_by(id=msg.author.id).one()
    cattle = session.query(FortUser).filter_by(name=duser.pref_name).one()
    drop = session.query(FortDrop).filter_by(user_id=cattle.id, system_id=system.id).one()
    assert drop.amount == 300

    expect = [
        {'range': 'H6:H7', 'values': [[5322], [0]]},
        {'range': 'H15:H15', 'values': [[300]]},
    ]
    assert cog.actions.SCANNERS['hudson_cattle'].payloads == expect


@pytest.mark.asyncio
async def test_cmd_drop_newuser(session, f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_newuser("!drop 500 nuru")

    await action_map(msg, f_bot).execute()

    expect = 'Will automatically add NewUser to sheet. See !user command to change.'
    f_bot.send_message.assert_any_call(msg.channel, expect)
    f_bot.send_message.assert_any_call(msg.channel, '**Nurundere** 5922/8425 :Fortifying: - 99.51Ly')

    system = session.query(FortSystem).filter_by(name='Nurundere').one()
    system = session.query(FortSystem).filter_by(name='Nurundere').one()
    assert system.current_status == 5922
    duser = session.query(DiscordUser).filter_by(id=msg.author.id).one()
    sheet = session.query(FortUser).filter_by(name=duser.pref_name).one()
    drop = session.query(FortDrop).filter_by(user_id=sheet.id, system_id=system.id).one()
    assert drop.amount == 500

    expect = [
        {'range': 'A20:B20', 'values': [['test cry', 'test name']]},
        {'range': 'H6:H7', 'values': [[5922], [0]]},
        {'range': 'H18:H18', 'values': [[500]]},
    ]
    assert cog.actions.SCANNERS['hudson_cattle'].payloads == expect


@pytest.mark.asyncio
async def test_cmd_drop_set(session, f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!drop 578 nuru --set 6500")

    await action_map(msg, f_bot).execute()

    f_bot.send_message.assert_called_with(msg.channel, '**Nurundere** 6500/8425 :Fortifying: - 99.51Ly')

    system = session.query(FortSystem).filter_by(name='Nurundere').one()
    assert system.current_status == 6500
    duser = session.query(DiscordUser).filter_by(id=msg.author.id).one()
    cattle = session.query(FortUser).filter_by(name=duser.pref_name).one()
    drop = session.query(FortDrop).filter_by(user_id=cattle.id, system_id=system.id).one()
    assert drop.amount == 978

    expect = [
        {'range': 'H6:H7', 'values': [[6500], [0]]},
        {'range': 'H15:H15', 'values': [[978]]},
    ]
    assert cog.actions.SCANNERS['hudson_cattle'].payloads == expect


@pytest.mark.asyncio
async def test_cmd_drop_finished(session, f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!drop 400 dong")

    await action_map(msg, f_bot).execute()
    expected = """**Dongkum** 7400/7239 :Fortified: - 81.54Ly

__Next Fort Target__:
**Nurundere** 5422/8425 :Fortifying: - 99.51Ly

**User1** Have a :cookie: for completing Dongkum
Bonus for highest contribution:
    :cookie: for **User1** with 400 supplies"""
    f_bot.send_message.assert_called_with(msg.channel, expected)

    system = session.query(FortSystem).filter_by(name='Dongkum').one()
    assert system.current_status == 7400
    duser = session.query(DiscordUser).filter_by(id=msg.author.id).one()
    cattle = session.query(FortUser).filter_by(name=duser.pref_name).one()
    drop = session.query(FortDrop).filter_by(user_id=cattle.id, system_id=system.id).one()
    assert drop.amount == 400

    expect = [
        {'range': 'K6:K7', 'values': [[7400], [0]]},
        {'range': 'K15:K15', 'values': [[400]]},
    ]
    assert cog.actions.SCANNERS['hudson_cattle'].payloads == expect


@pytest.mark.asyncio
async def test_cmd_hold_simple(session, f_bot, f_dusers, f_um_testbed):
    msg = fake_msg_gears("!hold 2000 empt")

    await action_map(msg, f_bot).execute()

    expect = """```Control        | Empty [M sec]
20%            | Merits Missing 8000
Our Progress 0 | Enemy Progress 0%
Nearest Hudson | Rana
Priority       | Low
Power          |```"""
    f_bot.send_message.assert_called_with(msg.channel, expect)

    duser = session.query(DiscordUser).filter_by(id=msg.author.id).one()
    um = session.query(UMUser).filter_by(name=duser.pref_name).one()
    system = session.query(UMSystem).filter_by(name='Empty').one()
    assert system.missing == 8000
    hold = session.query(UMHold).filter_by(user_id=um.id, system_id=system.id).one()
    assert hold.held == 2000
    assert hold.redeemed == 0

    expect = [
        {'range': 'K18:L18', 'values': [[2000, 0]]},
    ]
    assert cog.actions.SCANNERS['hudson_undermine'].payloads == expect


@pytest.mark.asyncio
async def test_cmd_hold_newuser(session, f_bot, f_dusers, f_um_testbed):
    msg = fake_msg_newuser("!hold 1000 empty")

    await action_map(msg, f_bot).execute()

    expect = 'Will automatically add NewUser to sheet. See !user command to change.'
    f_bot.send_message.assert_any_call(msg.channel, expect)
    expect2 = """```Control        | Empty [M sec]
10%            | Merits Missing 9000
Our Progress 0 | Enemy Progress 0%
Nearest Hudson | Rana
Priority       | Low
Power          |```"""
    f_bot.send_message.assert_any_call(msg.channel, expect2)

    system = session.query(UMSystem).filter_by(name='empty').one()
    assert system.missing == 9000
    duser = session.query(DiscordUser).filter_by(id=msg.author.id).one()
    sheet = session.query(UMUser).filter_by(name=duser.pref_name).one()
    hold = session.query(UMHold).filter_by(user_id=sheet.id, system_id=system.id).one()
    assert hold.held == 1000

    expect = [
        {'range': 'A20:B20', 'values': [['test cry', 'test name']]},
        {'range': 'K20:L20', 'values': [[1000, 0]]},
    ]
    assert cog.actions.SCANNERS['hudson_undermine'].payloads == expect


@pytest.mark.asyncio
async def test_cmd_hold_redeem(session, f_bot, f_dusers, f_um_testbed):
    msg = fake_msg_gears("!hold --redeem")

    await action_map(msg, f_bot).execute()

    expect = """**Redeemed Now** 2600

__Cycle Summary__
```  System   | Hold | Redeemed
---------- | ---- | --------
Cemplangpa | 0    | 4000
Pequen     | 0    | 1950
Burr       | 0    | 8000```"""
    f_bot.send_message.assert_called_with(msg.channel, expect)

    duser = session.query(DiscordUser).filter_by(id=msg.author.id).one()
    um = session.query(UMUser).filter_by(name=duser.pref_name).one()
    system = session.query(UMSystem).filter_by(name='Pequen').one()
    hold = session.query(UMHold).filter_by(user_id=um.id, system_id=system.id).one()
    assert hold.held == 0
    assert hold.redeemed == 1950

    expect = [
        {'range': 'D18:E18', 'values': [[0, 4000]]},
        {'range': 'F18:G18', 'values': [[0, 1950]]},
        {'range': 'H18:I18', 'values': [[0, 8000]]},
    ]
    assert cog.actions.SCANNERS['hudson_undermine'].payloads == expect


@pytest.mark.asyncio
async def test_cmd_hold_redeem_finished(session, f_bot, f_dusers, f_um_testbed):
    msg = fake_msg_gears("!hold 10000 empty")

    await action_map(msg, f_bot).execute()

    expect = """```Control        | Empty [M sec]
100%           | Merits Leading 0
Our Progress 0 | Enemy Progress 0%
Nearest Hudson | Rana
Priority       | Low
Power          |```
\nSystem is finished with held merits. Type `!um` for more targets.
\n**User1** Have a :skull: for completing Empty. Don\'t forget to redeem."""
    f_bot.send_message.assert_any_call(msg.channel, expect)

    duser = session.query(DiscordUser).filter_by(id=msg.author.id).one()
    um = session.query(UMUser).filter_by(name=duser.pref_name).one()
    system = session.query(UMSystem).filter_by(name='Empty').one()
    assert system.missing == 0
    hold = session.query(UMHold).filter_by(user_id=um.id, system_id=system.id).one()
    assert hold.held == 10000
    assert hold.redeemed == 0

    expect = [
        {'range': 'K18:L18', 'values': [[10000, 0]]},
    ]
    assert cog.actions.SCANNERS['hudson_undermine'].payloads == expect


@pytest.mark.asyncio
async def test_cmd_hold_died(session, f_bot, f_dusers, f_um_testbed):
    msg = fake_msg_gears("!hold --died")

    await action_map(msg, f_bot).execute()

    f_bot.send_message.assert_called_with(msg.channel, 'Sorry you died :(. Held merits reset.')

    duser = session.query(DiscordUser).filter_by(id=msg.author.id).one()
    um = session.query(UMUser).filter_by(name=duser.pref_name).one()
    system = session.query(UMSystem).filter_by(name='Pequen').one()
    hold = session.query(UMHold).filter_by(user_id=um.id, system_id=system.id).one()
    assert hold.held == 0
    assert hold.redeemed == 1550

    expect = [
        {'range': 'D18:E18', 'values': [[0, 4000]]},
        {'range': 'F18:G18', 'values': [[0, 1550]]},
        {'range': 'H18:I18', 'values': [[0, 5800]]},
    ]
    assert cog.actions.SCANNERS['hudson_undermine'].payloads == expect


@pytest.mark.asyncio
async def test_cmd_hold_system(session, f_bot, f_dusers, f_um_testbed):
    msg = fake_msg_gears("!hold --redeem-systems Pequen")

    await action_map(msg, f_bot).execute()

    expect = """**Redeemed Now** 400

__Cycle Summary__
```  System   | Hold | Redeemed
---------- | ---- | --------
Cemplangpa | 0    | 4000
Pequen     | 0    | 1950
Burr       | 2200 | 5800```"""

    f_bot.send_message.assert_called_with(msg.channel, expect)

    duser = session.query(DiscordUser).filter_by(id=msg.author.id).one()
    um_user = session.query(UMUser).filter_by(name=duser.pref_name).one()
    p_system = session.query(UMSystem).filter_by(name='Pequen').one()
    b_system = session.query(UMSystem).filter_by(name='Burr').one()

    hold = session.query(UMHold).filter_by(user_id=um_user.id, system_id=p_system.id).one()
    assert hold.held == 0
    assert hold.redeemed == 1950
    hold2 = session.query(UMHold).filter_by(user_id=um_user.id, system_id=b_system.id).one()
    assert hold2.held == 2200
    assert hold2.redeemed == 5800

    expect = [
        {'range': 'F18:G18', 'values': [[0, 1950]]}
    ]
    assert cog.actions.SCANNERS['hudson_undermine'].payloads == expect


@pytest.mark.asyncio
async def test_cmd_ocr_preps(session, f_bot, f_dusers, f_fort_testbed, f_ocr_testbed):
    msg = fake_msg_gears("!ocr preps")

    await action_map(msg, f_bot).execute()
    expected = """__Hudson Preps Report__

Current Consolidation: 0%

Rhea: 0, updated at 2021-08-25 02:33:00"""
    f_bot.send_message.assert_called_with(msg.channel, expected)


@pytest.mark.asyncio
async def test_cmd_pin(session, f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!pin")

    await action_map(msg, f_bot).execute()
    expected = """```:Fortifying: Rhea **Atropos**
:Fortifying: Othime **Priority for S/M ships (no L pads)**
:Fortifying: Dongkum
:Fortifying: Nurundere
:Fortifying: LHS 3749
:Fortifying: Alpha Fornacis
:Fortifying: WW Piscis Austrini
:Fortifying: LPM 229
:Fortifying: The things in the list after that```"""
    f_bot.send_message.assert_called_with(msg.channel, expected)


@pytest.mark.asyncio
async def test_cmd_recruits_not_admin(f_bot, db_cleanup):
    msg = fake_msg_gears("!recruits")

    with pytest.raises(cog.exc.InvalidPerms):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_recruits_similar_cmdr(f_bot, f_admins, f_dusers, db_cleanup, patch_scanners):
    patch_scanners.cells_col_major = [
        [
            "CMDR Names",
            "CMDR 1",
            "CMDR 2",
            "CMDR Cookies",
        ],
        [
            "Discord Names",
            "Discord CMDR 1",
            "",
            "",
        ],
    ]
    msg = fake_msg_gears("!recruits add CMDR Cookie")

    await action_map(msg, f_bot).execute()
    expected = "CMDR CMDR Cookie is similar to CMDR Cookies in row 4"
    result = str(f_bot.send_message.call_args).replace("\\n", "\n")
    assert expected in result


@pytest.mark.asyncio
async def test_cmd_recruits_similar_cmdr_force(f_bot, f_admins, f_dusers, db_cleanup, patch_scanners):
    patch_scanners.first_free = 200
    patch_scanners.cells_col_major = [
        [
            "CMDR Names",
            "CMDR 1",
            "CMDR 2",
            "CMDR Cookies",
        ],
        [
            "Discord Names",
            "Discord CMDR 1",
            "",
            "",
        ],
    ]
    msg = fake_msg_gears("!recruits add CMDR Cookie --force")

    await action_map(msg, f_bot).execute()
    expected = "CMDR CMDR Cookie has been added to row: 199"
    result = str(f_bot.send_message.call_args).replace("\\n", "\n")
    assert expected in result


@pytest.mark.asyncio
async def test_cmd_recruits_unique_cmdr(f_bot, f_admins, f_dusers, db_cleanup, patch_scanners):
    patch_scanners.first_free = 200
    patch_scanners.cells_col_major = [
        [
            "CMDR Names",
            "CMDR 1",
            "CMDR 2",
            "CMDR Cookies",
        ],
        [
            "Discord Names",
            "Discord CMDR 1",
            "",
            "",
        ],
    ]
    msg = fake_msg_gears("!recruits add CMDR Not There -d Discord Name -p 1 -r R")

    await action_map(msg, f_bot).execute()
    expected = "CMDR CMDR Not There has been added to row: 199"
    assert expected in str(f_bot.send_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
async def test_cmd_repair(f_bot):
    msg = fake_msg_gears("!repair rana")

    await action_map(msg, f_bot).execute()

    actual = str(f_bot.send_message.call_args).replace("\\n", "\n")
    assert "Ali Hub" in actual
    assert "Meucci Port" in actual


@pytest.mark.asyncio
async def test_cmd_route(f_bot):
    msg = fake_msg_gears("!route nanomam, rana, sol, frey, arnemil")

    await action_map(msg, f_bot).execute()

    expect = """__Route Plotted__
Total Distance: **277**ly

Nanomam
Sol
Rana
Arnemil
Frey"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_route_start(f_bot):
    msg = fake_msg_gears("!route nanomam, rana, sol, frey, arnemil --optimum")

    await action_map(msg, f_bot).execute()

    expect = """__Route Plotted__
Total Distance: **246**ly

Arnemil
Nanomam
Sol
Rana
Frey"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_route_too_few(f_bot):
    msg = fake_msg_gears("!route rana")

    with pytest.raises(cog.exc.InvalidCommandArgs):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_route_bad_system(f_bot):
    msg = fake_msg_gears("!route ranaaaaa")

    with pytest.raises(cog.exc.InvalidCommandArgs):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_scout_rnd(f_bot):
    msg = fake_msg_gears("!scout -r 1")

    f_bot.wait_for_message.async_return_value = fake_msg_gears('stop')
    await action_map(msg, f_bot).execute()

    expect = """
If you are running more than one system, do them in this order and you'll not ricochet the whole galaxy. Also let us know if you want to be a member of the FRC Scout Squad!
@here @FRC Scout

:Exploration: Epsilon Scorpii
:Exploration: Mulachi
:Exploration: Parutis
:Exploration: 39 Serpentis
:Exploration: Venetic
:Exploration: BD+42 3917
:Exploration: LHS 6427
:Exploration: LP 580-33
:Exploration: WW Piscis Austrini
:Exploration: Aornum
:Exploration: LHS 142
:Exploration: Kaushpoos

:o7:```"""
    assert expect in str(f_bot.send_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
async def test_cmd_scout_custom(f_bot):
    msg = fake_msg_gears("!scout -c rana, nurundere, frey")

    await action_map(msg, f_bot).execute()

    expect = """
If you are running more than one system, do them in this order and you'll not ricochet the whole galaxy. Also let us know if you want to be a member of the FRC Scout Squad!
@here @FRC Scout

:Exploration: Nurundere
:Exploration: Rana
:Exploration: Frey

:o7:```"""
    assert expect in str(f_bot.send_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
async def test_cmd_scout_bad_args(f_bot):
    msg = fake_msg_gears("!scout -c raaaaaaaaaa, zzzzzzzzz")

    with pytest.raises(cog.exc.InvalidCommandArgs):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_scout_no_args(f_bot):
    msg = fake_msg_gears("!scout")

    with pytest.raises(cog.exc.InvalidCommandArgs):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_status(f_bot):
    msg = fake_msg_gears("!status")

    await action_map(msg, f_bot).execute()

    expect = cog.tbl.format_table([
        ['Created By', 'GearsandCogs'],
        ['Uptime', '5'],
        ['Version', '{}'.format(cog.__version__)],
        ['Contributors:', ''],
        ['    Shotwn', 'Inara search'],
        ['    Prozer', 'Various Contributions'],
    ])[0]
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_time(f_bot):
    msg = fake_msg_gears("!time")

    await action_map(msg, f_bot).execute()

    assert "Game Time:" in str(f_bot.send_message.call_args)


@pytest.mark.asyncio
async def test_cmd_track_add(session, f_bot, f_dusers, f_admins, f_track_testbed):
    msg = fake_msg_gears("!track add -d 10 Kappa")

    await action_map(msg, f_bot).execute()

    expected = """__Systems Added To Tracking__

Systems added: 9 First few follow ...

HIP 33799, Hyades Sector IC-K b9-4, Kappa, Ocshongzi, Pang, Suttora, Tjiwang, Tyerremon, Udegobo"""
    f_bot.send_message.assert_called_with(msg.channel, expected)

    assert session.query(TrackSystem).filter(TrackSystem.system == "Kappa").one().distance == 10
    assert session.query(TrackSystemCached).filter(TrackSystemCached.system == "Pang").one()


@pytest.mark.asyncio
async def test_cmd_track_remove(session, f_bot, f_dusers, f_admins, f_track_testbed):
    msg = fake_msg_gears("!track remove Tollan")

    await action_map(msg, f_bot).execute()

    expected = """__Systems Removed From Tracking__

Systems added: 1 First few follow ...

Tollan"""
    f_bot.send_message.assert_called_with(msg.channel, expected)

    assert session.query(TrackSystem).filter(TrackSystem.system == "Rhea").all() == []
    assert session.query(TrackSystemCached).filter(TrackSystemCached.system == "Santal").all() == []


@pytest.mark.asyncio
async def test_cmd_track_show(f_bot, f_dusers, f_admins, f_track_testbed):
    msg = fake_msg_gears("!track show")

    await action_map(msg, f_bot).execute()

    expected = """__Tracking System Rules__

    Tracking systems <= 15ly from Nanomam
    Tracking systems <= 12ly from Tollan"""
    f_bot.send_message.assert_called_with(msg.channel, expected)


@pytest.mark.asyncio
async def test_cmd_track_ids_add(session, f_bot, f_dusers, f_admins, f_track_testbed):
    msg = fake_msg_gears("!track ids -a ZZZ-111")

    await action_map(msg, f_bot).execute()

    expected = "Carrier IDs added successfully to tracking."
    f_bot.send_message.assert_called_with(msg.channel, expected)

    assert session.query(TrackByID).filter(TrackByID.id == "ZZZ-111").one().override


@pytest.mark.asyncio
async def test_cmd_track_ids_remove(session, f_bot, f_dusers, f_admins, f_track_testbed):
    msg = fake_msg_gears("!track ids -r J3N-53B")

    await action_map(msg, f_bot).execute()

    expected = "Carrier IDs removed successfully from tracking."
    f_bot.send_message.assert_called_with(msg.channel, expected)

    assert session.query(TrackByID).filter(TrackByID.id == "J3N-53B").all() == []


@pytest.mark.asyncio
async def test_cmd_track_ids_show(f_bot, f_dusers, f_admins, f_track_testbed):
    msg = fake_msg_gears("!track ids -s")

    await action_map(msg, f_bot).execute()

    expected = """__Tracking IDs__

J3J-WVT [CLBF] jumped **Nanomam** => **Rana**.
J3N-53B [CLBF] jumped **No Info** => **No Info**.
OVE-111 [Manual] jumped **No Info** => **No Info**.
XNL-3XQ [CLBF] jumped **No Info** => **Tollan**, near Tollan."""
    f_bot.send_message.assert_called_with(msg.channel, expected)


@pytest.mark.asyncio
async def test_cmd_trigger(f_bot):
    msg = fake_msg_gears("!trigger rana")

    await action_map(msg, f_bot).execute()

    expect = """__Predicted Triggers__
Power: Zachary Hudson
Power HQ: Nanomam

```System       | Rana
Distance     | 46.1
Upkeep       | 22.1
Fort Trigger | 5620
UM Trigger   | 13786```"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_trigger_as_power(f_bot):
    msg = fake_msg_gears("!trigger rana -p grom")

    await action_map(msg, f_bot).execute()

    expect = """__Predicted Triggers__
Power: Yuri Grom
Power HQ: Clayakarma

```System       | Rana
Distance     | 86.3
Upkeep       | 27.4
Fort Trigger | 7545
UM Trigger   | 8432```"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_um(f_bot, f_testbed):
    msg = fake_msg_gears("!um")

    await action_map(msg, f_bot).execute()

    expect = """__Current Combat / Undermining Targets__

```Control            | Pequen [A sec]
84%                | Merits Missing 2000
Our Progress 10500 | Enemy Progress 50%
Nearest Hudson     | Atropos
Priority           | Low
Power              |```
```Expand              | Burr [L sec]
Behind by 3500%     | Merits Missing 202668
Our Progress 161630 | Enemy Progress 3500%
Nearest Hudson      | Dongkum
Priority            | Medium
Power               |```
```Opposing expansion | AF Leopris [L sec]
Behind by 169%     | Merits Missing 12138
Our Progress 47739 | Enemy Progress 169%
Nearest Hudson     | Atropos
Priority           | low
Power              |```
```Control        | Empty [M sec]
0%             | Merits Missing 10000
Our Progress 0 | Enemy Progress 0%
Nearest Hudson | Rana
Priority       | Low
Power          |```"""

    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_um_search(f_bot, f_testbed):
    msg = fake_msg_gears("!um burr")

    await action_map(msg, f_bot).execute()

    expect = """```Expand              | Burr [L sec]
Behind by 3500%     | Merits Missing 202668
Our Progress 161630 | Enemy Progress 3500%
Nearest Hudson      | Dongkum
Priority            | Medium
Power               |```"""

    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_um_set_fail(f_bot, f_testbed):
    msg = fake_msg_gears("!um --set 5500")

    with pytest.raises(cog.exc.InvalidCommandArgs):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_um_set_works(session, f_bot, f_testbed):
    before = session.query(UMSystem).filter_by(name='Pequen').one()
    msg = fake_msg_gears("!um --set {}:40 {} --offset 600".format(before.progress_us + 1500, before.name))

    await action_map(msg, f_bot).execute()

    expect = """```Control            | Pequen [A sec]
96%                | Merits Missing 500
Our Progress 12000 | Enemy Progress 40%
Nearest Hudson     | Atropos
Priority           | Low
Power              |```"""

    f_bot.send_message.assert_called_with(msg.channel, expect)

    session.close()  # Force refresh
    after = session.query(UMSystem).filter_by(name='Pequen').one()
    assert after.progress_us == before.progress_us + 1500
    assert after.progress_them == 0.4
    assert after.map_offset == 600

    expect = [
        {'range': 'F10:F13', 'values': [[12000], [0.4], ['Hold Merits'], [600]]}
    ]
    assert cog.actions.SCANNERS['hudson_undermine'].payloads == expect


@pytest.mark.asyncio
async def test_cmd_um_list_held(f_bot, f_testbed):
    msg = fake_msg_gears("!um --list")

    await action_map(msg, f_bot).execute()

    expect = """**Held Merits**

```CMDR  | Cemplangpa | Pequen | Burr | AF Leopris | Empty
----- | ---------- | ------ | ---- | ---------- | -----
User2 | 450        | 2400   | 0    | 0          | 0
User1 | 0          | 400    | 2200 | 0          | 0```"""

    actual = str(f_bot.send_message.call_args).replace("\\n", "\n")[:-2]
    actual = re.sub(r'.*live_hudson, ["\']', '', actual)
    actual = actual.split("\n")
    actual = "\n".join(actual[:2] + actual[3:])
    assert actual == expect


@pytest.mark.asyncio
async def test_cmd_um_npcs(f_bot):
    msg = fake_msg_gears("!um --npcs")

    await action_map(msg, f_bot).execute()

    call_args_list = f_bot.send_message.call_args_list
    assert len(call_args_list) == 2  # Only two messages were sent
    for call_args in call_args_list:  # Each sends an embed to the right channel
        assert call_args[0][0] == msg.channel
        assert call_args[1]['embed'].__module__ == 'discord.embeds'
    # And the content is different
    assert call_args_list[0][1]['embed'] is not call_args_list[1][1]['embed']


@pytest.mark.asyncio
async def test_cmd_user(f_bot, f_testbed):
    msg = fake_msg_gears("!user")

    await action_map(msg, f_bot).execute()

    expect = """__User1__
Sheet Name: User1
Default Cry:

__Fortification__
    Cry: User1 are forting late!
    Total: Dropped 1100
``` System   | Amount
--------- | ------
Frey      | 700
Nurundere | 400```
__Undermining__
    Cry: We go pew pew!
    Total: Holding 2600, Redeemed 11350
```  System   | Hold | Redeemed
---------- | ---- | --------
Cemplangpa | 0    | 4000
Pequen     | 400  | 1550
Burr       | 2200 | 5800```"""

    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_user_set_name(session, f_bot, f_testbed):
    new_name = "NotUser1"
    msg = fake_msg_gears("!user --name " + new_name)

    await action_map(msg, f_bot).execute()

    expect = """__User1__
Sheet Name: NotUser1
Default Cry:

__Fortification__
    Cry: User1 are forting late!
    Total: Dropped 1100
``` System   | Amount
--------- | ------
Frey      | 700
Nurundere | 400```
__Undermining__
    Cry: We go pew pew!
    Total: Holding 2600, Redeemed 11350
```  System   | Hold | Redeemed
---------- | ---- | --------
Cemplangpa | 0    | 4000
Pequen     | 400  | 1550
Burr       | 2200 | 5800```"""

    f_bot.send_message.assert_called_with(msg.channel, expect)

    duser = session.query(DiscordUser).filter_by(id=msg.author.id).one()
    assert duser.pref_name == new_name
    assert duser.fort_user.name == new_name
    assert duser.um_user.name == new_name

    expect = [
        {'range': 'A15:B15', 'values': [['User1 are forting late!', 'NotUser1']]},
        {'range': 'A18:B18', 'values': [['We go pew pew!', 'NotUser1']]}
    ]
    assert cog.actions.SCANNERS['hudson_cattle'].payloads == expect


@pytest.mark.asyncio
async def test_cmd_user_set_cry(session, f_bot, f_testbed):
    new_cry = "A new cry"
    msg = fake_msg_gears("!user --cry " + new_cry)

    await action_map(msg, f_bot).execute()

    expect = """__User1__
Sheet Name: User1
Default Cry: A new cry

__Fortification__
    Cry: A new cry
    Total: Dropped 1100
``` System   | Amount
--------- | ------
Frey      | 700
Nurundere | 400```
__Undermining__
    Cry: A new cry
    Total: Holding 2600, Redeemed 11350
```  System   | Hold | Redeemed
---------- | ---- | --------
Cemplangpa | 0    | 4000
Pequen     | 400  | 1550
Burr       | 2200 | 5800```"""

    f_bot.send_message.assert_called_with(msg.channel, expect)

    duser = session.query(DiscordUser).filter_by(id=msg.author.id).one()
    assert duser.pref_cry == new_cry
    assert duser.fort_user.cry == new_cry
    assert duser.um_user.cry == new_cry

    expect = [
        {'range': 'A15:B15', 'values': [['A new cry', 'User1']]},
        {'range': 'A18:B18', 'values': [['A new cry', 'User1']]}
    ]
    assert cog.actions.SCANNERS['hudson_cattle'].payloads == expect


@pytest.mark.asyncio
async def test_cmd_dist(f_bot):
    msg = fake_msg_gears("!dist sol, frey, adeo")

    await action_map(msg, f_bot).execute()

    expect = """Distances From: **Sol**

```Adeo | 91.59ly
Frey | 108.97ly```"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_dist_partial(f_bot):
    msg = fake_msg_gears("!dist sol, adeo")

    await action_map(msg, f_bot).execute()

    expect = """Distances From: **Sol**

```Adeo | 91.59ly```"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_dist_invalid_args(f_bot):
    msg = fake_msg_gears("!dist sol")
    with pytest.raises(cog.exc.InvalidCommandArgs):
        await action_map(msg, f_bot).execute()

    msg = fake_msg_gears("!dist solllll, frey")
    with pytest.raises(cog.exc.InvalidCommandArgs):
        await action_map(msg, f_bot).execute()

    msg = fake_msg_gears("!dist sol, freyyyyy")
    with pytest.raises(cog.exc.InvalidCommandArgs):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_donate(f_bot):
    expected = "This is the donation message.\n"
    d_file = cog.util.CONF.paths.donate
    try:
        if os.path.exists(d_file):
            shutil.copyfile(d_file, d_file + '.bak')
        with open(d_file, 'w') as fout:
            fout.write(expected)

        msg = fake_msg_gears("!donate")
        await action_map(msg, f_bot).execute()

        f_bot.send_message.assert_called_with(msg.channel, expected)
    finally:
        try:
            os.rename(d_file + '.bak', d_file)
        except (OSError, FileNotFoundError):
            os.remove(d_file)


@pytest.mark.asyncio
async def test_cmd_kos_search(f_bot, f_kos):
    msg = fake_msg_gears("!kos search bad")

    await action_map(msg, f_bot).execute()

    expect = "bad_guy   | Hudson  | KILL         | Pretty bad guy"
    assert expect in str(f_bot.send_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
async def test_cmd_kos_pull(f_bot, patch_scanners):
    msg = fake_msg_gears("!kos pull")

    await action_map(msg, f_bot).execute()

    # FIXME: Improve
    #  assert patch_scanners.parse_sheet.assert_called()
    f_bot.send_message.assert_called_with(msg.channel, "KOS list refreshed from sheet.")


@pytest.mark.asyncio
async def test_cmd_near_control(f_bot):
    msg = fake_msg_gears("!near control winters rana")

    await action_map(msg, f_bot).execute()

    expect = "Momoirent  | 65.38"
    assert expect in str(f_bot.send_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
async def test_cmd_near_if(f_bot):
    msg = fake_msg_gears("!near if sol")

    await action_map(msg, f_bot).execute()

    actual = str(f_bot.send_message.call_args).replace("\\n", "\n")
    assert "Lacaille 9352" in actual


@pytest.mark.asyncio
async def test_cmd_vote_prep(f_bot, f_dusers, f_global_testbed, f_vote_testbed):
    msg = fake_msg_gears("!vote prep 1")

    await action_map(msg, f_bot).execute()
    f_bot.send_message.assert_called_with(msg.channel, "**User1**: voted 1 Prep.")


@pytest.mark.asyncio
async def test_cmd_vote_cons(f_bot, f_dusers, f_global_testbed, f_vote_testbed):
    msg = fake_msg_gears("!vote 5 cons")

    await action_map(msg, f_bot).execute()
    f_bot.send_message.assert_called_with(msg.channel, "**User1**: voted 6 Cons.")


@pytest.mark.asyncio
async def test_cmd_vote_near(f_bot, f_global_testbed, f_vote_testbed, db_cleanup):
    with cogdb.session_scope(cogdb.Session) as session:
        globe = session.query(Global).one()
        globe.show_vote_goal = True
        globe.consolidation = 25
        globe.vote_goal = 26

    msg = fake_msg_gears("!vote")
    await action_map(msg, f_bot).execute()

    assert "Hold your vote (<=1%" in str(f_bot.send_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
async def test_cmd_vote(f_bot, f_global_testbed, f_vote_testbed, db_cleanup):
    msg = fake_msg_gears("!vote")

    await action_map(msg, f_bot).execute()

    possibles = [
        "Current vote goal is 0%, current consolidation 77%, please **vote Preparation**.",
        "Please hold your vote for now. A ping will be sent once we have a final decision.",
    ]
    actual = str(f_bot.send_message.call_args).replace("\\n", "\n")
    actual = re.sub(r'call\(Channel: \d+ live_hudson, \'', '', actual)[:-2]
    assert actual in possibles


@pytest.mark.asyncio
async def test_cmd_vote_set_goal(f_bot, f_admins, f_dusers, f_global_testbed, f_vote_testbed, db_cleanup):
    msg = fake_msg_gears("!vote -s 75")

    await action_map(msg, f_bot).execute()
    f_bot.send_message.assert_called_with(msg.channel, 'New vote goal is **75%**, current vote is 77%.')


@pytest.mark.asyncio
async def test_cmd_vote_display(f_bot, f_admins, f_dusers, f_global_testbed, f_vote_testbed, db_cleanup):
    msg = fake_msg_gears("!vote --display")

    await action_map(msg, f_bot).execute()
    f_bot.send_message.assert_called_with(msg.channel, 'Will now SHOW the vote goal.')


@INARA_TEST
@pytest.mark.asyncio
async def test_cmd_whois_cancelled(f_bot, f_asheet_kos, f_kos):
    msg = fake_msg_gears("!whois Prozer")
    f_bot.wait_for.async_side_effect = (
        Interaction('cmd_who_friendly', message=msg, user=msg.author, comp_label=cog.inara.BUT_CANCEL),
        Interaction('cmd_who_friendly', message=msg, user=msg.author, comp_label=cog.inara.BUT_DENY),
    )

    await action_map(msg, f_bot).execute()
    assert "Should the CMDR Prozer be added as friendly or hostile?" in str(f_bot.send_message.call_args).replace("\\n", "\n")


@INARA_TEST
@pytest.mark.asyncio
async def test_cmd_whois_friendly(f_bot, f_asheet_kos, f_kos):
    msg = fake_msg_gears('!whois Prozer')
    f_bot.wait_for.async_side_effect = (
        Interaction('cmd_who_friendly', message=msg, user=msg.author, comp_label=cog.inara.BUT_FRIENDLY),
        Interaction('cmd_who_friendly', message=msg, user=msg.author, comp_label=cog.inara.BUT_DENY),
    )

    await action_map(msg, f_bot).execute()
    assert "Should the CMDR Prozer be added as friendly or hostile?" in str(f_bot.send_message.call_args).replace("\\n", "\n")


@INARA_TEST
@pytest.mark.asyncio
async def test_cmd_whois_hostile(f_bot, f_asheet_kos, f_kos, patch_scanners):
    msg = fake_msg_gears("!whois Prozer")
    f_bot.wait_for.async_side_effect = (
        Interaction('cmd_who_friendly', message=msg, user=msg.author, comp_label=cog.inara.BUT_FRIENDLY),
        Interaction('cmd_who_friendly', message=msg, user=msg.author, comp_label=cog.inara.BUT_APPROVE),
    )

    await action_map(msg, f_bot).execute()
    assert "Should the CMDR Prozer be added as friendly or hostile?" in str(f_bot.send_message.call_args).replace("\\n", "\n")


def test_process_system_args():
    args = ['This  ,  ', 'is ,   ', '   an,', ' example.']
    results = cog.actions.process_system_args(args)
    assert results == ['this', 'is', 'an', 'example.']


def test_filter_top_dusers(session, f_dusers, f_fort_testbed, f_um_testbed):
    top_fort = cogdb.query.users_with_fort_merits(session)
    mapped = {
        1: tc.Member(top_fort[2][0].pref_name, roles=[tc.Role("FRC Recruit"), tc.Role("Something")]),
        2: tc.Member(top_fort[0][0].pref_name, roles=[tc.Role("FRC Member"), tc.Role("Something")]),
        3: tc.Member(top_fort[1][0].pref_name, roles=[tc.Role("FRC Recruit"), tc.Role("Something")]),
    }

    def get_member_(did):
        return mapped[did]

    guild = aiomock.Mock()
    guild.get_member = get_member_

    expected_rec = [
        ('User3', 1800),
        ('User1', 1100),
        ('', ''),
        ('', ''),
        ('', '')
    ]
    expected_mem = [
        ('User2', 2000),
        ('', ''),
        ('', ''),
        ('', ''),
        ('', '')
    ]
    rec, mem = cog.actions.filter_top_dusers(guild, top_fort, [])
    assert rec == expected_rec
    assert mem == expected_mem


def test_filter_top_dusers_exclude(session, f_dusers, f_fort_testbed, f_um_testbed):
    top_fort = cogdb.query.users_with_fort_merits(session)
    mapped = {
        1: tc.Member(top_fort[2][0].pref_name, roles=[tc.Role("FRC Recruit"), tc.Role("Filter")]),
        2: tc.Member(top_fort[0][0].pref_name, roles=[tc.Role("FRC Member"), tc.Role("Something")]),
        3: tc.Member(top_fort[1][0].pref_name, roles=[tc.Role("FRC Recruit"), tc.Role("Filter")]),
    }

    def get_member_(did):
        return mapped[did]

    guild = aiomock.Mock()
    guild.get_member = get_member_

    expected_rec = [
        ('', ''),
        ('', '')
    ]
    expected_mem = [
        ('User2', 2000),
        ('', ''),
    ]
    rec, mem = cog.actions.filter_top_dusers(guild, top_fort, ["Filter"], 2)
    assert rec == expected_rec
    assert mem == expected_mem


def test_route_systems(session, f_dusers, f_fort_testbed):
    systems = f_fort_testbed[1]

    expected = [
        '**Sol** 2500/5211 :Fortifying:, 2250 :Undermining: Leave For Grinders - 28.94Ly',
        '**Alpha Fornacis**    0/6476 :Fortifying: - 67.27Ly',
        '**Dongkum** 7000/7239 :Fortifying: (239 left) - 81.54Ly',
        '**Nurundere** 5422/8425 :Fortifying: - 99.51Ly',
        '**LHS 3749** 1850/5974 :Fortifying: - 55.72Ly',
        '**Frey** 4910/4910 :Fortified: - 116.99Ly'
    ]
    assert cog.actions.route_systems(systems[:6]) == expected


def test_route_systems_less_two(session, f_dusers, f_fort_testbed):
    systems = f_fort_testbed[1]

    expected = ['**Frey** 4910/4910 :Fortified: - 116.99Ly']
    assert cog.actions.route_systems(systems[:1]) == expected
