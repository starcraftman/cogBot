# pylint: disable=redefined-outer-name,missing-function-docstring,unused-argument
"""
Tests for pvp.actions
"""
import concurrent.futures as cfut
import functools
import tempfile
import zipfile

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


def action_map(fake_message, fake_bot):
    """
    Test stub of part of CogBot.on_message dispatch.
    Notably, parses commands and returns Action based on parser cmd/subcmd.

    Exceute with Action.execute() coro or schedule on loop
    """
    parser = pvp.parse.make_parser("!")
    args = parser.parse_args(fake_message.content.split(" "))
    cls = getattr(pvp.actions, args.cmd)

    with cogdb.session_scope(cogdb.Session) as session, cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
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
!cmdr     | Setup and modify your cmdr settings.
!donate   | Information on supporting the dev.
!feedback | Give feedback or report a bug
!help     | This help command.
!log      | Show recent PVP events parsed
!match    | Create and manage pvp matches.
!near     | Find things near you.
!privacy  | Display the privacy explanation.
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
    dfile = f_bot.send_message.call_args[1]['file']
    assert dfile.filename.endswith('file_01.txt')


@pytest.mark.asyncio
async def test_cmd_log_event(f_bot, f_pvp_testbed):
    msg = fake_msg_gears("!log kills locations")

    await action_map(msg, f_bot).execute()
    dfile = f_bot.send_message.call_args[1]['file']
    assert dfile.filename.endswith('file_01.txt')


@pytest.mark.asyncio
async def test_cmd_log_invalid(f_bot, f_pvp_testbed):
    msg = fake_msg_gears("!log invalid")

    await action_map(msg, f_bot).execute()
    assert 'Invalid log event, choose from:' in str(f_bot.send_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
async def test_cmd_log_limit(f_bot, f_pvp_testbed):
    msg = fake_msg_gears("!log --limit 4")

    await action_map(msg, f_bot).execute()
    dfile = f_bot.send_message.call_args[1]['file']
    assert dfile.filename.endswith('file_01.txt')


@pytest.mark.asyncio
async def test_cmd_log_after(f_bot, f_pvp_testbed):
    msg = fake_msg_gears("!log --after 2016-06-10T14:32:38")

    await action_map(msg, f_bot).execute()
    dfile = f_bot.send_message.call_args[1]['file']
    assert dfile.filename.endswith('file_01.txt')


@pytest.mark.asyncio
async def test_cmd_log_after_invalid(f_bot, f_pvp_testbed):
    msg = fake_msg_gears("!log --after invalid")

    await action_map(msg, f_bot).execute()
    assert 'Invalid after format. Write dates like:' in str(f_bot.send_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
class TestCMDMatch:
    """ Tests for the `!match` command of pvp bot. """
    async def test_start_nomatch(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match start")

        await action_map(msg, f_bot).execute()
        assert 'No pending match.' in f_bot.send_message.call_args[0][-1]

    async def test_start_channel(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match start")
        msg.channel.id = 99

        await action_map(msg, f_bot).execute()
        assert 'The current match has been started.' in f_bot.send_message.call_args[0][-1]
        dembed = f_bot.send_message.call_args[1]['embed'].to_dict()
        assert dembed['title'] == 'PVP Match: 3/10'
        assert dembed['fields'][0]['value'] == 'Started'
        assert dembed['fields'][-2]['name'] == 'Team 1'
        assert dembed['fields'][-1]['name'] == 'Team 2'

    async def test_cancel_nomatch(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match cancel")

        await action_map(msg, f_bot).execute()
        assert 'No pending match.' in f_bot.send_message.call_args[0][-1]

    async def test_cancel_channel(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match cancel")
        msg.channel.id = 99

        await action_map(msg, f_bot).execute()
        assert 'The current match has been cancelled.' == f_bot.send_message.call_args[0][-1]

    async def test_add_nomatch(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match add")

        await action_map(msg, f_bot).execute()
        assert 'No pending match.' in f_bot.send_message.call_args[0][-1]

    async def test_add_channel(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match add newbie")
        msg.channel.id = 99

        await action_map(msg, f_bot).execute()
        assert 'CMDR newbie added to match!\n' in f_bot.send_message.call_args[0][-1]

    async def test_add_channel_start(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match add newbie")
        msg.channel.id = 101

        await action_map(msg, f_bot).execute()

        assert 'CMDR newbie added to match!\n' in f_bot.send_message.call_args[0][-1]
        assert 'match has been started' in f_bot.send_message.call_args[0][-1]

        dembed = f_bot.send_message.call_args[1]['embed'].to_dict()
        assert dembed['title'] == 'PVP Match: 2/2'
        assert dembed['fields'][0]['value'] == 'Started'
        assert dembed['fields'][-2]['name'] == 'Team 1'
        assert dembed['fields'][-1]['name'] == 'Team 2'

    async def test_remove_nomatch(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match remove")

        await action_map(msg, f_bot).execute()
        assert 'No pending match.' in f_bot.send_message.call_args[0][-1]

    async def test_remove_channel(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match remove coolguy")
        msg.channel.id = 99

        await action_map(msg, f_bot).execute()
        assert 'CMDR coolGuy removed from match!' in f_bot.send_message.call_args[0][-1]

    async def test_join_nomatch(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match join")

        await action_map(msg, f_bot).execute()
        assert 'No pending match.' in f_bot.send_message.call_args[0][-1]

    async def test_join_channel(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match join")
        msg.channel.id = 99
        msg.author.id = 4

        await action_map(msg, f_bot).execute()
        assert 'CMDR newbie added to match!\n' == f_bot.send_message.call_args[0][-1]

    async def test_leave_nomatch(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match leave")

        await action_map(msg, f_bot).execute()
        assert 'No pending match.' in f_bot.send_message.call_args[0][-1]

    async def test_leave_channel(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match leave")
        msg.channel.id = 99
        msg.author.id = 1

        await action_map(msg, f_bot).execute()
        assert 'CMDR coolGuy removed from match!\n' == f_bot.send_message.call_args[0][-1]

    async def test_setup_nomatch(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match setup")

        await action_map(msg, f_bot).execute()
        dembed = f_bot.send_message.call_args[1]['embed'].to_dict()
        assert dembed['title'] == 'PVP Match: 0/20'
        assert dembed['fields'][0]['value'] == 'Setup'

    async def test_setup_channel(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match setup")
        msg.channel.id = 99

        await action_map(msg, f_bot).execute()
        assert 'A match is already pending.' in f_bot.send_message.call_args[0][-1]

    async def test_reroll_nomatch(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match reroll")

        await action_map(msg, f_bot).execute()
        assert 'No pending match.' in f_bot.send_message.call_args[0][-1]

    async def test_reroll_channel(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match reroll")
        msg.channel.id = 99

        await action_map(msg, f_bot).execute()
        dembed = f_bot.send_message.call_args[1]['embed'].to_dict()
        assert dembed['title'] == 'PVP Match: 3/10'
        assert dembed['fields'][-2]['name'] == 'Team 1'
        assert dembed['fields'][-1]['name'] == 'Team 2'

    async def test_win_nomatch(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match win")

        await action_map(msg, f_bot).execute()
        assert 'No pending match.' in f_bot.send_message.call_args[0][-1]

    @pytest.mark.skip(reason='Difficult to test atm.')
    async def test_win_channel(self, f_bot, f_pvp_testbed):
        pass

    # The same as using the !match show command, won't test it twice.
    async def test_nocmd_nomatch(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match")

        await action_map(msg, f_bot).execute()
        assert "No pending match." in str(f_bot.send_message.call_args).replace("\\n", "\n")

    async def test_nocmd_channel(self, f_bot, f_pvp_testbed):
        msg = fake_msg_gears("!match")
        msg.channel.id = 99

        await action_map(msg, f_bot).execute()
        dembed = f_bot.send_message.call_args[1]['embed'].to_dict()
        assert "coolGuy\nshootsALot\nshyGuy" == dembed['fields'][-1]['value']


@pytest.mark.asyncio
async def test_cmd_privacy(f_bot, f_pvp_testbed):
    expect = """This bot will take uploads of logs from you and process them to derive statistics.
On first upload your in game name will be read from logs and linked to your Discord ID.
This bot will link all this information to your discord id so you can retrieve it by command.
Beyond this bot having access to your logs, uploading files to the bot naturally means
that discord the company has a copy."""
    msg = fake_msg_gears("!privacy")

    await action_map(msg, f_bot).execute()
    assert expect in str(f_bot.send_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
async def test_cmd_privacy_version_none(f_bot, f_pvp_testbed):
    expect = 'versions: 1.0, 2.0'
    msg = fake_msg_gears("!privacy version")

    await action_map(msg, f_bot).execute()
    assert expect in str(f_bot.send_message.call_args).replace("\\n", "\n")


@pytest.mark.asyncio
async def test_cmd_privacy_version_selected(f_bot, f_pvp_testbed):
    expect = 'Version 1.0'
    msg = fake_msg_gears("!privacy version 1.0")

    await action_map(msg, f_bot).execute()
    assert expect in str(f_bot.send_message.call_args).replace("\\n", "\n")


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


def test_filename_for_upload():
    fname = pvp.actions.filename_for_upload('<coolGuy{32}>', id_num=2)
    assert fname.startswith('coolGuy32_2')
    assert fname.endswith('.log')


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


def test_process_log(f_pvp_testbed, f_plog_file, eddb_session):
    found = pvp.actions.process_log(fname=f_plog_file, cmdr_id=3, eddb_session=eddb_session)
    assert 'CMDR shootsALot killed CMDR CanNotShoot at 2016-06-10 14:55:22' in found


def test_process_log_fails(f_pvp_testbed, f_plog_zip, eddb_session):
    found = pvp.actions.process_log(fname=f_plog_zip, cmdr_id=3, eddb_session=eddb_session)
    assert not found


def test_process_log_noeddb(f_pvp_testbed, f_plog_file, eddb_session):
    found = pvp.actions.process_log(fname=f_plog_file, cmdr_id=3, eddb_session=eddb_session)
    assert 'CMDR shootsALot killed CMDR CanNotShoot at 2016-06-10 14:55:22' in found


def test_process_archive(f_pvp_testbed, f_plog_zip, eddb_session):
    found = pvp.actions.process_archive(fname=f_plog_zip, cmdr_id=3, attach_fname='/tmp/original.zip', eddb_session=eddb_session)
    assert 'CMDR shootsALot killed CMDR CanNotShoot at 2016-06-10 14:55:22' in found


def test_process_archive_fails(f_pvp_testbed, f_plog_file, eddb_session):
    with pytest.raises(zipfile.BadZipfile):
        pvp.actions.process_archive(fname=f_plog_file, cmdr_id=3, attach_fname='/tmp/original.zip', eddb_session=eddb_session)


@pytest.mark.asyncio
async def test_process_tempfile_log(f_pvp_testbed, f_plog_file, eddb_session, event_loop):
    info = [{'attach_fname': '/tmp/original.log', 'fname': f_plog_file}]
    with cfut.ProcessPoolExecutor(1) as pool:
        coro = event_loop.run_in_executor(
            pool, functools.partial(pvp.actions.process_cmdr_tempfiles, cmdr_id=3, info=info)
        )
        await coro
        assert 'CMDR shootsALot killed CMDR CanNotShoot at 2016-06-10 14:55:22' in coro.result()


@pytest.mark.asyncio
async def test_process_tempfile_archive(f_pvp_testbed, f_plog_zip, eddb_session, event_loop):
    info = [{'attach_fname': '/tmp/original.log', 'fname': f_plog_zip}]
    with cfut.ProcessPoolExecutor(1) as pool:
        coro = event_loop.run_in_executor(
            pool, functools.partial(pvp.actions.process_cmdr_tempfiles, cmdr_id=3, info=info)
        )
        await coro
        assert 'CMDR shootsALot killed CMDR CanNotShoot at 2016-06-10 14:55:22' in coro.result()


@pytest.mark.asyncio
async def test_process_tempfile_fails(f_pvp_testbed, f_plog_zip, eddb_session, event_loop):
    with tempfile.NamedTemporaryFile() as tfile, cfut.ProcessPoolExecutor(1) as pool:
        tfile.write(b"Bad file.")
        tfile.flush()

        info = [{'attach_fname': '/tmp/original.log', 'fname': tfile.name}]
        coro = event_loop.run_in_executor(
            pool, functools.partial(pvp.actions.process_cmdr_tempfiles, cmdr_id=3, info=info)
        )

        await coro
        assert 'Unsupported file type: /tmp/original.log' in coro.result()


def test_get_privacy_versions():
    expect = [1.0, 2.0]
    found = pvp.actions.get_privacy_versions(cog.util.CONF.paths.privacy)
    for item in expect:
        assert item in found


@pytest.mark.asyncio
async def test_get_privacy_stmt():
    assert "Version 2.0" in await pvp.actions.get_privacy_stmt(cog.util.CONF.paths.privacy, version=2.0)


@pytest.mark.asyncio
async def test_get_privacy_stmt_fails():
    with pytest.raises(cog.exc.InvalidCommandArgs):
        await pvp.actions.get_privacy_stmt(cog.util.CONF.paths.privacy, version='bad')


def test_process_filtered_archive(f_pvp_testbed, f_plog_filtered):
    with tempfile.NamedTemporaryFile() as log:
        pvp.actions.process_filtered_archive(fname=f_plog_filtered, cmdr_id=1, attach_fname='fixture_00.zip', log_fname=log.name)
        with open(log.name, 'r', encoding='utf-8') as fin:
            assert fin.read()


def test_compute_all_stats(f_pvp_testbed, eddb_session):
    stat = eddb_session.query(pvp.schema.PVPStat).filter(pvp.schema.PVPStat.cmdr_id == 1).one()
    stat.kills = 0
    stat.deaths = 0
    eddb_session.commit()
    eddb_session.close()

    pvp.actions.compute_all_stats()

    found = eddb_session.query(pvp.schema.PVPStat).filter(pvp.schema.PVPStat.cmdr_id == 1).one()
    assert found.kills == 3
