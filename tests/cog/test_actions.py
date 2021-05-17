"""
Tests against the cog.actions module.
These tests act as integration tests, checking almost the whole path.
Importantly, I have stubbed/mocked everything to do with discord.py and the gsheets calls.

Important Note Regarding DB:
    After executing an action ALWAYS make a new Session(). The old one will still be stale.
"""
import re

import aiomock
import pytest

import cog.actions
import cog.bot
import cog.parse
import cogdb
from cogdb.eddb import HUDSON_CONTROLS
from cogdb.side import SystemAge
from cogdb.schema import (DiscordUser, FortSystem, FortDrop, FortUser,
                          FortOrder, UMSystem, UMUser, UMHold)

from tests.conftest import fake_msg_gears, fake_msg_newuser


# Important, any fixtures in here get auto run
pytestmark = pytest.mark.usefixtures("patch_scanners")


@pytest.fixture
def patch_scanners():
    """ Patch the scanners. """
    old_scanners = cog.actions.SCANNERS
    scanner = aiomock.Mock(user_row=5)

    async def send_batch_(payloads, input_opt='', *args):
        scanner.payloads = payloads

    async def get_batch_(*args):
        return scanner._values

    mock_cls = aiomock.Mock()
    mock_cls.update_sheet_user_dict.return_value = [
        {'range': 'A20:B20', 'values': [['test cry', 'test name']]},
    ]
    scanner.__class__ = mock_cls
    scanner.send_batch = send_batch_
    scanner.get_batch = get_batch_
    cog.actions.SCANNERS = {
        'hudson_cattle': scanner,
        'hudson_kos': scanner,
        'hudson_undermine': scanner,
    }

    yield

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

    return cls(args=args, bot=fake_bot, msg=fake_message)


##################################################################
# Actual Tests

# TODO: Admin, some useless subcommands though.


#  @pytest.mark.asyncio
#  async def test_template(f_bot):
    #  msg = fake_msg_gears("!cmd")

    #  await action_map(msg, f_bot).execute()

    #  print(str(f_bot.send_message.call_args).replace("\\n", "\n"))


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
async def test_cmd_admin_addum_fail(f_admins, f_bot, db_cleanup):
    msg = fake_msg_gears("!admin addum Cubeo")

    await action_map(msg, f_bot).execute()

    expect = 'All systems asked are already in the sheet or are invalid'
    f_bot.send_long_message.assert_called_with(msg.channel, expect)


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
    f_bot.send_long_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_bgs_age(side_session, f_bot, f_dusers, f_fort_testbed):
    row = side_session.query(SystemAge).filter(
        SystemAge.control.in_(HUDSON_CONTROLS)).order_by(SystemAge.system.asc()).first()
    msg = fake_msg_gears("!bgs age " + row.control)

    await action_map(msg, f_bot).execute()

    line = [word.strip().replace("```')", '') for word in
            str(f_bot.send_long_message.call_args).split('\\n')[2].split('|')]
    assert line == [row.control, row.system, str(row.age)]


@pytest.mark.asyncio
async def test_cmd_bgs_dash(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!bgs dash Othime")

    await action_map(msg, f_bot).execute()

    assert 'Chelgit' in str(f_bot.send_long_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
async def test_cmd_bgs_exp(f_bot):
    msg = fake_msg_gears("!bgs exp rana")

    f_bot.wait_for.async_return_value = fake_msg_gears('0')
    await action_map(msg, f_bot).execute()

    actual = str(f_bot.send_long_message.call_args).replace("\\n", "\n")[:-2]
    actual = re.sub(r'.*live_hudson, ["\']', '', actual)
    actual = actual.split('\n')
    for line in actual[6:]:
        parts = re.split(r'\s+\|\s+', line)
        assert float(parts[1]) <= 20


@pytest.mark.asyncio
async def test_cmd_bgs_expto(f_bot):
    msg = fake_msg_gears("!bgs expto rana")

    await action_map(msg, f_bot).execute()

    actual = str(f_bot.send_long_message.call_args).replace("\\n", "\n")[:-2]
    actual = re.sub(r'.*live_hudson, ["\']', '', actual)
    actual = actual.split('\n')
    for line in actual[4:]:
        parts = re.split(r'\s+\|\s+', line)
        assert float(parts[1]) <= 20


@pytest.mark.asyncio
async def test_cmd_bgs_find(f_bot):
    msg = fake_msg_gears("!bgs find nurundere")

    await action_map(msg, f_bot).execute()

    actual = str(f_bot.send_long_message.call_args).replace("\\n", "\n")[:-2]
    actual = re.sub(r'.*live_hudson, ["\']', '', actual)
    assert actual.split('\n')[4].index("Monarchy of Orisala") != -1


@pytest.mark.asyncio
async def test_cmd_bgs_inf(side_session, f_bot):
    msg = fake_msg_gears("!bgs inf Sol")

    await action_map(msg, f_bot).execute()

    assert "Mother Gaia" in str(f_bot.send_long_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
async def test_cmd_bgs_sys(side_session, f_bot):
    msg = fake_msg_gears("!bgs sys Sol")

    await action_map(msg, f_bot).execute()
    reply = str(f_bot.send_long_message.call_args).replace("\\n", "\n")

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
**Nurundere** 5422/8425 :Fortifying:
**Othime**    0/7367 :Fortifying: Priority for S/M ships (no L pads)
Prep: **Rhea** 5100/10000 :Fortifying: Atropos

__Next Targets__
**LHS 3749** 1850/5974 :Fortifying:
**Alpha Fornacis**    0/6476 :Fortifying:
**Othime**    0/7367 :Fortifying: Priority for S/M ships (no L pads)

__Almost Done__
**Dongkum** 7000/7239 :Fortifying: (239 left)"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_summary(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!fort --summary")

    await action_map(msg, f_bot).execute()

    expect = """```Cancelled|Fortified|Undermined|Skipped|Left
---------|---------|----------|-------|----
0/10     |1/10     |2/10      |2/10   |7/10```"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_next(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!fort --next 2")

    await action_map(msg, f_bot).execute()

    expect = """__Next Targets__
**LHS 3749** 1850/5974 :Fortifying:
**Alpha Fornacis**    0/6476 :Fortifying:"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_miss(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!fort --miss 1000")

    await action_map(msg, f_bot).execute()

    expect = """__Systems Missing 1000 Supplies__
**Dongkum** 7000/7239 :Fortifying: (239 left)"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_search(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!fort nuru, othime")

    await action_map(msg, f_bot).execute()

    expect = """__Search Results__
**Nurundere** 5422/8425 :Fortifying:
**Othime**    0/7367 :Fortifying: Priority for S/M ships (no L pads)"""
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_fort_set(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!fort --set 7000:222 nuru")

    await action_map(msg, f_bot).execute()

    expect = """**Nurundere** 7000/8425 :Fortifying:, 222 :Undermining: (1425 left)"""
    f_bot.send_message.assert_called_with(msg.channel, expect)
    system = cogdb.Session().query(FortSystem).filter_by(name='Nurundere').one()
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
async def test_cmd_fort_order(session, f_bot, f_dusers, f_fort_testbed):
    try:
        msg = fake_msg_gears("!fort --order sol, nuru, frey")
        await action_map(msg, f_bot).execute()

        systems = [sys.system_name for sys in session.query(FortOrder).order_by(FortOrder.order)]
        assert systems == ['Sol', 'Nurundere', 'Frey']

        msg2 = fake_msg_gears("!fort")
        await action_map(msg2, f_bot).execute()

        expect = """__Active Targets (Manual Order)__
**Sol** 2500/5211 :Fortifying:, 2250 :Undermining: Leave For Grinders

__Next Targets__
**Nurundere** 5422/8425 :Fortifying:

__Almost Done__
**Dongkum** 7000/7239 :Fortifying: (239 left)"""
        f_bot.send_message.assert_called_with(msg2.channel, expect)
    finally:
        dsession = cogdb.Session()
        for order in dsession.query(FortOrder):
            dsession.delete(order)
        dsession.commit()


@pytest.mark.asyncio
async def test_cmd_fort_order_next(session, f_bot, f_dusers, f_fort_testbed):
    try:
        msg = fake_msg_gears("!fort --order sol, nuru, frey")
        await action_map(msg, f_bot).execute()

        systems = [sys.system_name for sys in session.query(FortOrder).order_by(FortOrder.order)]
        assert systems == ['Sol', 'Nurundere', 'Frey']

        msg2 = fake_msg_gears("!fort --next 2")
        await action_map(msg2, f_bot).execute()

        expect = """__Next Targets (Manual Order)__
**Nurundere** 5422/8425 :Fortifying:"""
        f_bot.send_message.assert_called_with(msg2.channel, expect)
    finally:
        dsession = cogdb.Session()
        for order in dsession.query(FortOrder):
            dsession.delete(order)
        dsession.commit()


@pytest.mark.asyncio
async def test_cmd_fort_unset(session, f_bot, f_dusers, f_fort_testbed):
    try:
        msg = fake_msg_gears("!fort --order sol, nuru, frey")
        await action_map(msg, f_bot).execute()

        systems = [sys.system_name for sys in session.query(FortOrder).order_by(FortOrder.order)]
        assert systems == ['Sol', 'Nurundere', 'Frey']

        msg2 = fake_msg_gears("!fort --order")
        await action_map(msg2, f_bot).execute()

        session = cogdb.Session()
        systems = [sys.system_name for sys in session.query(FortOrder).order_by(FortOrder.order)]
        assert systems == []
    finally:
        dsession = cogdb.Session()
        for order in dsession.query(FortOrder):
            dsession.delete(order)
        dsession.commit()


@pytest.mark.asyncio
async def test_cmd_help(f_bot):
    msg = fake_msg_gears("!help")

    await action_map(msg, f_bot).execute()

    assert "Here is an overview of my commands." in str(f_bot.send_ttl_message.call_args)
    assert msg.is_deleted


@pytest.mark.asyncio
async def test_cmd_drop_simple(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!drop 578 nuru")

    await action_map(msg, f_bot).execute()

    f_bot.send_message.assert_called_with(msg.channel, '**Nurundere** 6000/8425 :Fortifying:')
    session = cogdb.Session()
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
async def test_cmd_drop_negative(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!drop -100 nuru")

    await action_map(msg, f_bot).execute()

    f_bot.send_message.assert_called_with(msg.channel, '**Nurundere** 5322/8425 :Fortifying:')
    session = cogdb.Session()
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
async def test_cmd_drop_newuser(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_newuser("!drop 500 nuru")

    await action_map(msg, f_bot).execute()

    expect = 'Will automatically add NewUser to sheet. See !user command to change.'
    f_bot.send_message.assert_any_call(msg.channel, expect)
    f_bot.send_message.assert_any_call(msg.channel, '**Nurundere** 5922/8425 :Fortifying:')

    session = cogdb.Session()
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
async def test_cmd_drop_set(f_bot, f_dusers, f_fort_testbed):
    msg = fake_msg_gears("!drop 578 nuru --set 6500")

    await action_map(msg, f_bot).execute()

    f_bot.send_message.assert_called_with(msg.channel, '**Nurundere** 6500/8425 :Fortifying:')
    session = cogdb.Session()
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
async def test_cmd_hold_simple(f_bot, f_dusers, f_um_testbed):
    msg = fake_msg_gears("!hold 2000 empt")

    await action_map(msg, f_bot).execute()

    expect = """```Control        | Empty [M sec]
20%            | Merits Missing 8000
Our Progress 0 | Enemy Progress 0%
Nearest Hudson | Rana
Priority       | Low```"""
    f_bot.send_message.assert_called_with(msg.channel, expect)
    session = cogdb.Session()
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
async def test_cmd_hold_newuser(f_bot, f_dusers, f_um_testbed):
    msg = fake_msg_newuser("!hold 1000 empty")

    await action_map(msg, f_bot).execute()

    expect = 'Will automatically add NewUser to sheet. See !user command to change.'
    f_bot.send_message.assert_any_call(msg.channel, expect)
    expect2 = """```Control        | Empty [M sec]
10%            | Merits Missing 9000
Our Progress 0 | Enemy Progress 0%
Nearest Hudson | Rana
Priority       | Low```"""
    f_bot.send_message.assert_any_call(msg.channel, expect2)

    session = cogdb.Session()
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
async def test_cmd_hold_redeem(f_bot, f_dusers, f_um_testbed):
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
    session = cogdb.Session()
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
async def test_cmd_hold_redeem_finished(f_bot, f_dusers, f_um_testbed):
    msg = fake_msg_gears("!hold 10000 empty")

    await action_map(msg, f_bot).execute()

    expect = """```Control        | Empty [M sec]
100%           | Merits Leading 0
Our Progress 0 | Enemy Progress 0%
Nearest Hudson | Rana
Priority       | Low```
\nSystem is finished with held merits. Type `!um` for more targets.
\n**User1** Have a :skull: for completing Empty. Don\'t forget to redeem."""
    f_bot.send_message.assert_any_call(msg.channel, expect)
    session = cogdb.Session()
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
async def test_cmd_hold_died(f_bot, f_dusers, f_um_testbed):
    msg = fake_msg_gears("!hold --died")

    await action_map(msg, f_bot).execute()

    f_bot.send_message.assert_called_with(msg.channel, 'Sorry you died :(. Held merits reset.')
    session = cogdb.Session()
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
async def test_cmd_hold_system(f_bot, f_dusers, f_um_testbed):
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

    session = cogdb.Session()
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
async def test_cmd_repair(f_bot):
    msg = fake_msg_gears("!repair rana")

    await action_map(msg, f_bot).execute()

    actual = str(f_bot.send_long_message.call_args).replace("\\n", "\n")
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

    expect = cog.tbl.wrap_markdown(cog.tbl.format_table([
        ['Created By', 'GearsandCogs'],
        ['Uptime', '5'],
        ['Version', '{}'.format(cog.__version__)],
        ['Contributors:', ''],
        ['    Shotwn', 'Inara search'],
    ]))
    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_time(f_bot):
    msg = fake_msg_gears("!time")

    await action_map(msg, f_bot).execute()

    assert "Game Time:" in str(f_bot.send_message.call_args)


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

    expect = """__Current UM Targets__

```Control            | Pequen [A sec]
84%                | Merits Missing 2000
Our Progress 10500 | Enemy Progress 50%
Nearest Hudson     | Atropos
Priority           | Low```
```Expand              | Burr [L sec]
Behind by 3500%     | Merits Missing 202668
Our Progress 161630 | Enemy Progress 3500%
Nearest Hudson      | Dongkum
Priority            | Medium```
```Opposing expansion | AF Leopris [L sec]
Behind by 169%     | Merits Missing 12138
Our Progress 47739 | Enemy Progress 169%
Nearest Hudson     | Atropos
Priority           | low```
```Control        | Empty [M sec]
0%             | Merits Missing 10000
Our Progress 0 | Enemy Progress 0%
Nearest Hudson | Rana
Priority       | Low```"""

    f_bot.send_message.assert_called_with(msg.channel, expect)


@pytest.mark.asyncio
async def test_cmd_um_search(f_bot, f_testbed):
    msg = fake_msg_gears("!um burr")

    await action_map(msg, f_bot).execute()

    expect = """```Expand              | Burr [L sec]
Behind by 3500%     | Merits Missing 202668
Our Progress 161630 | Enemy Progress 3500%
Nearest Hudson      | Dongkum
Priority            | Medium```"""

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
Priority           | Low```"""

    f_bot.send_message.assert_called_with(msg.channel, expect)
    after = cogdb.Session().query(UMSystem).filter_by(name='Pequen').one()
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
    assert(len(call_args_list) == 2)  # Only two messages were sent
    for call_args in call_args_list:  # Each sends an embed to the right channel
        assert(call_args[0][0] == msg.channel)
        assert(call_args[1]['embed'].__module__ == 'discord.embeds')
    # And the content is different
    assert(call_args_list[0][1]['embed'] is not call_args_list[1][1]['embed'])


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

    nsession = cogdb.Session()
    duser = nsession.query(DiscordUser).filter_by(id=msg.author.id).one()
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

    nsession = cogdb.Session()
    duser = nsession.query(DiscordUser).filter_by(id=msg.author.id).one()
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
    assert "LHS 397        | 19.32    | [L] Zillig Depot" in actual
