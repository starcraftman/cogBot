"""
Tests for pvp.actions
"""
import pytest

import cog.exc
import cogdb
import pvp.actions
import pvp.parse
import pvp.schema
from tests.conftest import fake_msg_gears, fake_msg_newuser, Interaction
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
async def test_cmd_req_help(f_bot, f_pvp_clean):
    msg = fake_msg_gears("!stats -h")

    with pytest.raises(cog.exc.ArgumentHelpError):
        await action_map(msg, f_bot).execute()


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
