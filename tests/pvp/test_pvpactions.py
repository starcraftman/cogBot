# pylint: disable=redefined-outer-name,missing-function-docstring,unused-argument
"""
Tests for pvp.actions
"""
import tempfile

import pytest
import sqlalchemy as sqla

import cog.exc
import cogdb
from cogdb.schema import AdminPerm
import pvp.actions
import pvp.parse
import pvp.schema
from pvp.schema import PVPLog
from tests.conftest import fake_msg_gears, Member
from tests.pvp.test_pvpjournal import JOURNAL_PATH


def action_map(fake_message, fake_bot):
    """
    Test stub of part of CogBot.on_message dispatch.
    Notably, parses commands and returns Action based on parser cmd/subcmd.

    Exceute with Action.execute() coro or schedule on loop
    """
    parser = pvp.parse.make_parser("!")
    args = parser.parse_args(fake_message.content.split(" "))
    cls = getattr(pvp.actions, args.cmd)

    with cogdb.session_scope(cogdb.Session) as session,\
            cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        return cls(args=args, bot=fake_bot, msg=fake_message, session=session, eddb_session=eddb_session)


#  @pytest.mark.asyncio
#  async def test_template(f_bot):
    #  msg = fake_msg_gears("!cmd")

    #  await action_map(msg, f_bot).execute()

    #  print(str(f_bot.send_message.call_args).replace("\\n", "\n"))


@pytest.mark.asyncio
async def test_cmd_admin_add_admin(f_dusers, f_admins, f_bot, db_cleanup, session):
    new_admin = Member(f_dusers[-1].display_name, None, id=f_dusers[-1].id)
    msg = fake_msg_gears("!admin add", mentions=[new_admin])

    await action_map(msg, f_bot).execute()

    session.close()
    last = session.query(AdminPerm).order_by(AdminPerm.id.desc()).limit(1).one()
    assert last.id == 3


@pytest.mark.asyncio
async def test_cmd_admin_remove_admin(f_dusers, f_admins, f_bot, db_cleanup, session):
    new_admin = Member(f_dusers[1].display_name, None, id=f_dusers[1].id)
    msg = fake_msg_gears("!admin remove", mentions=[new_admin])

    await action_map(msg, f_bot).execute()

    session.close()
    with pytest.raises(sqla.exc.NoResultFound):
        session.query(AdminPerm).filter(AdminPerm.id == f_dusers[1].id).limit(1).one()


@pytest.mark.asyncio
async def test_cmd_help(f_bot, f_pvp_clean):
    msg = fake_msg_gears("!help")
    expect = """Here is an overview of my commands.

For more information do: `!Command -h`
       Example: `!drop -h`

``` Command  |                           Effect
--------- | ----------------------------------------------------------
!admin    | The admininstrative commands
!dist     | Determine the distance from the first system to all others
!donate   | Information on supporting the dev.
!feedback | Give feedback or report a bug
!help     | This help command.
!log      | Show recent PVP events parsed
!near     | Find things near you.
!repair   | Show the nearest orbitals with shipyards
!route    | Plot the shortest route between these systems
!stats    | PVP statistics for you or another CMDR
!status   | Info about this bot
!time     | Show game time and time to ticks
!trigger  | Calculate fort and um triggers for systems
!whois    | Search for commander on inara.cz```"""

    await action_map(msg, f_bot).execute()

    assert expect in str(f_bot.send_ttl_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
async def test_cmd_log(f_bot, f_pvp_testbed):
    msg = fake_msg_gears("!log")

    await action_map(msg, f_bot).execute()
    assert "CMDR coolGuy killed CMDR LeSuck" in str(f_bot.send_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
async def test_cmd_stats(f_bot, f_pvp_testbed):
    msg = fake_msg_gears("!stats")

    await action_map(msg, f_bot).execute()
    _, kwargs = f_bot.send_message.call_args_list[0]
    assert '3' == kwargs['embed'].fields[0].value


@pytest.mark.asyncio
async def test_cmd_stats_name(f_bot, f_pvp_testbed):
    msg = fake_msg_gears("!stats coolGuy")

    await action_map(msg, f_bot).execute()
    _, kwargs = f_bot.send_message.call_args_list[0]
    assert '3' == kwargs['embed'].fields[0].value


@pytest.mark.asyncio
async def test_cmd_stats_help(f_bot, f_pvp_clean):
    msg = fake_msg_gears("!stats -h")

    with pytest.raises(cog.exc.ArgumentHelpError):
        await action_map(msg, f_bot).execute()


@pytest.mark.asyncio
async def test_cmd_file_upload(f_bot, f_spy_ships, f_pvp_testbed):
    cls = getattr(pvp.actions, 'FileUpload')
    msg = fake_msg_gears("upload")  # Not real command

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        assert len(eddb_session.query(pvp.schema.PVPDeath).all()) == 3

    with cogdb.session_scope(cogdb.Session) as session,\
            cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        await cls(args=None, bot=f_bot, msg=msg, fname=JOURNAL_PATH, session=session, eddb_session=eddb_session).execute()

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        assert len(eddb_session.query(pvp.schema.PVPDeath).all()) == 5


def test_filename_for_upload():
    fname = pvp.actions.filename_for_upload('<coolGuy{32}>', id_num=2)
    assert fname.startswith('coolGuy32_2')
    assert fname.endswith('.log')


def test_parse_in_process(f_pvp_testbed):
    first = 'CMDR coolGuy located in Asellus Primus'
    last = 'CMDR coolGuy was killed by: [CMDR Assisting (Python)'
    events = pvp.actions.parse_in_process(JOURNAL_PATH, discord_id=1)
    assert first in events[0]
    assert last in events[-1]


@pytest.mark.asyncio
async def test_parse_logs(f_bot, f_pvp_testbed, eddb_session):
    msg = fake_msg_gears('This is a fake file upload.')
    await pvp.actions.process_logs(eddb_session, [JOURNAL_PATH, JOURNAL_PATH],
                                   client=f_bot, msg=msg, archive=JOURNAL_PATH, orig_filename='player.log')
    msg, kwargs = msg.channel.sent_messages[0]

    assert 'Logs parsed. Detailed per file summary attached.' == msg
    assert 'parsed.log' == kwargs['file'].filename


@pytest.mark.asyncio
async def test_archive_log(f_bot, f_pvp_testbed, eddb_session):
    save_bot = cog.util.BOT
    try:
        cog.util.BOT = f_bot
        with tempfile.NamedTemporaryFile(mode='w') as tfile:
            tfile.write('This is a sample log file.')
            tfile.flush()
            cmdr = pvp.schema.get_pvp_cmdr(eddb_session, cmdr_id=1)
            msg = fake_msg_gears('This is a fake file upload.')

            await pvp.actions.archive_log(eddb_session, msg=msg, cmdr=cmdr, fname=tfile.name)
            latest_log = eddb_session.query(PVPLog).order_by(PVPLog.id.desc()).limit(1).one()
            expect_hash = 'efacef55cc78da2ce5cac8f50104e28d616c3bde9c27b1cdfb4dd8aa6e5d6a46e4'\
                          'b6873b06c88b7b4c031400459a75366207dcb98e29623a170997da5aedb539'
            assert expect_hash == latest_log.file_hash
            assert 1 == latest_log.cmdr_id
    finally:
        cog.util.BOT = save_bot
