"""
Specific actions for pvp bot
"""
import asyncio
import concurrent.futures as cfut
import datetime
import tempfile
import functools
import logging
import traceback
import zipfile

import aiofiles
import discord
import discord.ui as dui

import cogdb
from cogdb.eddb import LEN as EDDB_LEN
import cog.tbl
import pvp
import pvp.journal
# Make available reused actions
from cog.actions import (Action, Dist, Donate, Feedback, Near,
                         Repair, Route, Time, Trigger, WhoIs)  # noqa: F401 pylint: disable=unused-import

DISCLAIMER = """
This bot will store information derived from logs uploaded and link it to your Discord ID.
Your in game name will be read from logs and linked to your Discord ID as well (if present).

Do you consent to this use of your data?
"""


class PVPAction(Action):
    """
    Top level action, contains shared logic for all PVP actions.
    All actions will require an eddb_session by design.
    When file needed can be set, otherwise will be None.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.eddb_session = kwargs['eddb_session']
        self.fname = kwargs.get('fname')


class Admin(PVPAction):
    """
    Admin command console. For knowledgeable users only.
    """
    async def add(self):
        """
        Takes one of the following actions:
            1) Add 1 or more admins
            2) Add a single channel rule
            3) Add a single role rule
        """
        if not self.args.rule_cmds and self.msg.mentions:
            for member in self.msg.mentions:
                cogdb.query.add_admin(self.session, member)
            response = "Admins added:\n\n" + '\n'.join([member.name for member in self.msg.mentions])

        return response

    async def remove(self, admin):
        """
        Takes one of the following actions:
            1) Remove 1 or more admins
            2) Remove a single channel rule
            3) Remove a single role rule
        """
        if not self.args.rule_cmds and self.msg.mentions:
            for member in self.msg.mentions:
                admin.remove(self.session, cogdb.query.get_admin(self.session, member))
            response = "Admins removed:\n\n" + '\n'.join([member.name for member in self.msg.mentions])

        return response

    async def regenerate(self):  # pragma: no cover, don't hit the guild repeatedly
        """
        Regenerate the PVP database by reparsing logs.
        This will:
            - Drop and recreate all tables that aren't pvp_cmdrs.
            - Process all logs in order of upload to archive channel on server.
            - Import and parse selected events.
            - Once done, regenerate all stats for each CMDR at the end.
        """
        await self.bot.send_message(self.msg.channel, "All uploads will be denied while regeneration underway. Starting now.")
        response = "Regenerating PVP database has completed."
        pvp.schema.recreate_tables(keep_cmdrs=True)

        # Channel archiving pvp logs
        log_chan = self.msg.guild.get_channel(cog.util.CONF.channels.pvp_log)
        events = []
        try:
            async for msg in log_chan.history(limit=1000000, oldest_first=True):
                if not msg.content.startswith('Discord ID'):
                    continue

                discord_id = int(msg.content.split('\n')[0].replace('Discord ID: ', ''))
                cmdr = pvp.schema.get_pvp_cmdr(self.eddb_session, cmdr_id=discord_id)
                if not cmdr:  # If you don't have the pvp_cmdr, fill it
                    cmdr_name = msg.content.split('\n')[1].replace('CMDR: ', '')
                    pvp.schema.add_pvp_cmdr(self.eddb_session, discord_id, cmdr_name, None)

                with tempfile.NamedTemporaryFile() as tfile, cfut.ProcessPoolExecutor(max_workers=1) as pool:
                    for attach in msg.attachments:
                        await attach.save(tfile.name)
                        print(f'Parsing: {attach.filename}')
                        if await cog.util.is_zipfile_async(tfile.name):
                            try:
                                async with cog.util.extracted_archive_async(tfile.name) as logs:
                                    for log in logs:
                                        events += [f'\nFile: {log.name}']
                                        events += await asyncio.get_event_loop().run_in_executor(
                                            pool, parse_in_process, log, discord_id,
                                        )
                            except zipfile.BadZipfile:
                                await self.send_message(msg.channel, f'Error unzipping {attach.filename}, please check archive.')

                        else:
                            events += [f'\nFile: {attach.filename}']
                            events += await asyncio.get_event_loop().run_in_executor(
                                pool, parse_in_process, tfile.name, discord_id,
                            )

            with tempfile.NamedTemporaryFile(mode='w') as tfile:
                async with aiofiles.open(tfile.name, 'w', encoding='utf-8') as aout:
                    await aout.write("__Parsed Events__\n" + '\n'.join(events))
                    await aout.flush()

                await self.msg.channel.send('Logs parsed. Summary attached.',
                                            file=discord.File(fp=tfile.name, filename='parsed.log'))

        except discord.Forbidden as exc:
            logging.getLogger(__name__).error('Discord error: %s', exc)
            response = f"Missing message history permission on: {log_chan.name} on {log_chan.guild.name}"
        except discord.HTTPException:
            response = "Received HTTP Error, may be intermittent issue. Investigate."

        return response

    async def stats(self):  # pragma: no cover, don't hit the guild repeatedly
        """
        Regenerate just the stats given the current set of events.
        """
        with cfut.ProcessPoolExecutor(max_workers=1) as pool:
            await asyncio.get_event_loop().run_in_executor(pool, compute_all_stats)

        return "Stats for all CMDRs have been redone."

    async def execute(self):
        try:
            admin = cogdb.query.get_admin(self.session, self.duser)
        except cog.exc.NoMatch as exc:
            raise cog.exc.InvalidPerms(f"{self.msg.author.mention} You are not an admin!") from exc

        try:
            func = getattr(self, self.args.subcmd)
            if self.args.subcmd == "remove":
                response = await func(admin)
            elif self.args.subcmd == "regenerate":
                try:
                    self.deny_commands = True
                    response = await func()
                finally:
                    self.deny_commands = False
            else:
                response = await func()
            if response:
                await self.bot.send_message(self.msg.channel, response)
        except (AttributeError, TypeError) as exc:
            traceback.print_exc()
            raise cog.exc.InvalidCommandArgs("Bad subcommand of `!admin`, see `!admin -h` for help.") from exc


def compute_all_stats():
    """
    Recreate the PVPStat table only.
    Then compute the PVPStats for all existing commanders again.
    """
    pvp.schema.PVPStat.__table__.drop(cogdb.eddb_engine)
    pvp.schema.Base.metadata.create_all(cogdb.eddb_engine, tables=[pvp.schema.PVPStat])

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        for cmdr in eddb_session.query(pvp.schema.PVPCmdr):
            pvp.schema.update_pvp_stats(eddb_session, cmdr_id=cmdr.id)


class FileUpload(PVPAction):
    """
    Handle a file upload and scan for pvp information.
    """
    async def parse_log(self, *, discord_id):
        """
        Helper to allow usage internally.

        Args:
            discord_id: The CMDR's ID.
        """
        with cfut.ProcessPoolExecutor(max_workers=1) as pool:
            events = await asyncio.get_event_loop().run_in_executor(
                pool, parse_in_process, self.fname, discord_id,
            )

        return events

    async def execute(self):
        events = await self.parse_log(discord_id=self.msg.author.id)

        with tempfile.NamedTemporaryFile(mode='w') as tfile:
            async with aiofiles.open(tfile.name, 'w', encoding='utf-8') as aout:
                await aout.write("__Parsed Events__\n" + '\n'.join(events))
                await aout.flush()
            await self.msg.channel.send('Logs parsed. Summary attached.',
                                        file=discord.File(fp=tfile.name, filename='parsed.log'))


class Help(PVPAction):
    """
    Provide an overview of help.
    """
    async def execute(self):
        prefix = self.bot.prefix
        overview = '\n'.join([
            'Here is an overview of my commands.',
            '',
            f'For more information do: `{prefix}Command -h`',
            f'       Example: `{prefix}drop -h`'
        ])
        lines = [
            ['Command', 'Effect'],
            [f'{prefix}admin', 'The admininstrative commands'],
            [f'{prefix}dist', 'Determine the distance from the first system to all others'],
            [f'{prefix}donate', 'Information on supporting the dev.'],
            [f'{prefix}feedback', 'Give feedback or report a bug'],
            [f'{prefix}help', 'This help command.'],
            [f'{prefix}log', 'Show recent PVP events parsed'],
            [f'{prefix}near', 'Find things near you.'],
            [f'{prefix}repair', 'Show the nearest orbitals with shipyards'],
            [f'{prefix}route', 'Plot the shortest route between these systems'],
            [f'{prefix}stats', 'PVP statistics for you or another CMDR'],
            [f'{prefix}status', 'Info about this bot'],
            [f'{prefix}time', 'Show game time and time to ticks'],
            [f'{prefix}trigger', 'Calculate fort and um triggers for systems'],
            [f'{prefix}whois', 'Search for commander on inara.cz'],
        ]
        response = overview + '\n\n' + cog.tbl.format_table(lines, header=True)[0]
        print(response)
        await self.bot.send_ttl_message(self.msg.channel, response)
        try:
            await self.msg.delete()
        except discord.HTTPException:
            pass


class Log(PVPAction):
    """
    Display the most recent parsed events.
    """
    async def execute(self):
        events = pvp.schema.get_pvp_events(self.eddb_session, cmdr_id=self.msg.author.id)
        msg = '__Most Recent Events__\n\n' + '\n'.join([str(x) for x in events])
        await self.bot.send_message(self.msg.channel, msg)


class Stats(PVPAction):
    """
    Display statistics based on file uploads to bot.
    """
    async def execute(self):
        if self.args.name:
            cmdr_name = ' '.join(self.args.name)
            cmdr = pvp.schema.get_pvp_cmdr(self.eddb_session, cmdr_name=cmdr_name)
            msg = f"__CMDR Statistics__\n\nCMDR {cmdr_name} not found!"
        else:
            cmdr = pvp.schema.get_pvp_cmdr(self.eddb_session, cmdr_id=self.msg.author.id)
            msg = "__CMDR Statistics__\n\nNo recorded events."
        if not cmdr:
            await self.bot.send_message(self.msg.channel, msg)
            return

        embed = None
        stats = pvp.schema.get_pvp_stats(self.eddb_session, cmdr.id)
        if stats:
            msg = None
            avatar = self.bot.user.display_avatar.url
            cmdr_member = self.msg.guild.get_member(cmdr.id)
            if cmdr_member:
                avatar = cmdr_member.display_avatar.url
            embed = discord.Embed.from_dict({
                'color': cmdr.hex_value,
                'author': {
                    'name': 'PVP Statistics',
                    'icon_url': self.bot.user.display_avatar.url,
                },
                'provider': {
                    'name': 'FedCAT',
                },
                'thumbnail': {
                    'url': avatar
                },
                'title': f"CMDR {stats.cmdr.name}",
                "fields": stats.embed_values,
            })

        await self.bot.send_message(self.msg.channel, msg, embed=embed)


class Status(PVPAction):
    """
    Display the status of this bot.
    """
    async def execute(self):
        lines = [
            ['Created By', 'GearsandCogs'],
            ['Uptime', self.bot.uptime],
            ['Version', f'{pvp.__version__}'],
            ['Contributors:', ''],
        ]

        await self.bot.send_message(self.msg.channel, cog.tbl.format_table(lines)[0])


class CMDRRegistration(dui.Modal, title='CMDR Registration'):  # pragma: no cover
    """ Register a cmdr by getting name via a modal input. """
    hex = dui.TextInput(label='Hex colour for embeds. Red: B20000', min_length=6, max_length=6,
                        style=discord.TextStyle.short, placeholder="B20000", required=True)
    name = None

    async def on_submit(self, interaction: discord.Interaction):
        if not self.name:
            self.name = pvp.journal.clean_cmdr_name(self.children[-1].value)
        await interaction.response.send_message(f'You are now registered, CMDR {self.name}!', ephemeral=True)


async def cmdr_setup(eddb_session, client, msg, *, cmdr_name=None):  # pragma: no cover, heavily dependent on discord.py
    """
    Perform first time cmdr setup via interactive questions.
    """
    def check_button(orig_author, sent, inter):
        """
        Check the interaction is for same message and author.
        """
        return inter.message == sent and inter.user == orig_author

    view = dui.View().\
        add_item(dui.Button(label="Yes", custom_id="Yes", style=discord.ButtonStyle.green)).\
        add_item(dui.Button(label="No", custom_id="No", style=discord.ButtonStyle.red))

    sent = await msg.channel.send(DISCLAIMER, view=view)
    inter = await client.wait_for('interaction', check=functools.partial(check_button, msg.author, sent))

    if inter.data['custom_id'] == "No":
        await inter.response.send_message('Aborting registration. No data stored.', ephemeral=True)
        return False

    modal = CMDRRegistration()
    modal.name = cmdr_name
    if not modal.name:
        modal.add_item(
            dui.TextInput(
                label='In Game CMDR Name', min_length=5, max_length=EDDB_LEN['pvp_name'],
                style=discord.TextStyle.short, placeholder="Your in game name", required=True
            )
        )

    await inter.response.send_modal(modal)
    await modal.wait()

    return pvp.schema.add_pvp_cmdr(eddb_session, msg.author.id, modal.name, modal.hex)


def filename_for_upload(cmdr_name, *, id_num=1, archive=False):
    """
    Create a filename for archiving a file based on a simple naming format including:
    - CMDR name
    - integer discriminator
    - the current date.
    - correct extension based on type

    Args:
        cmdr_name: The name of the commander.
        cnt: A simple integer discriminator.
        archive: True if a zip, otherwise a log.

    Returns: A potential filename that is cleaned for discord.
    """
    now = datetime.datetime.utcnow().strftime(cog.util.TIME_STRP)
    id_num = id_num % (4 * 2 ** 10)
    ext = 'zip' if archive else 'log'
    fname = f'{cmdr_name}_{id_num}_{now}.{ext}'

    return cog.util.clean_fname(fname, replacement='', extras=['-'])


def parse_in_process(fname, discord_id):
    """
    Helper to run the parsing in a separate process.

    Args:
        fname: The filename of the journal fragment.
        discord_id: The discord id of the CMDR.

    Returns: The results of the parse operation.
    """
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        parser = pvp.journal.Parser(fname=fname, cmdr_id=discord_id, eddb_session=eddb_session)
        parser.load()
        # Do not return unbound db objects.
        return [str(x) for x in parser.parse()]


async def archive_log(eddb_session, *, msg, cmdr, fname):
    """
    Upload a copy of the log file or zip to the archiving channel set for server.

    Args:
        eddb_session: A session onto the EDDB db.
        msg: The msg that initiated the DM upload.
        log_chan: The disocrd.TextChannel that contains all log backups.
        cmdr: The PVPCmdr object.
        fname: The filename of the log OR zip file.

    Returns: True IFF the log is new and was uploaded.
    """
    # Files are hashed to check for dupes on add.
    pvp_log = await pvp.schema.add_pvp_log(eddb_session, fname=fname, cmdr_id=cmdr.id)
    if pvp_log.msg_id:
        await msg.channel.send(f'Log already uploaded at {pvp_log.updated_date}.',)
        return False

    pvp_log.filename = filename_for_upload(cmdr.name, id_num=pvp_log.id, archive=cog.util.is_zipfile(fname))
    with open(fname, 'rb') as fin:
        log_chan = cog.util.BOT.get_channel(cog.util.CONF.channels.pvp_log)
        msg = await log_chan.send(f"Discord ID: {cmdr.id}\nCMDR: {cmdr.name}",
                                  file=discord.File(fin, filename=pvp_log.filename))
        pvp_log.msg_id = msg.id

    return True


async def process_logs(eddb_session, logs, *, client, msg, archive, orig_filename=None):
    """
    Process all logs received from a single upload to the bot.

    Args:
        eddb_session: A session onto the EDDB db.
        logs: A list of filenames to process.
        client: A instance of the bot.
        msg: The original discord.Message received by bot.
        archive: The original file received, be it zip or log file.
    """
    if not logs:
        return

    cmdr = pvp.schema.get_pvp_cmdr(eddb_session, cmdr_id=msg.author.id)
    if not cmdr:
        cmdr_name = await pvp.journal.find_cmdr_name(logs[0])
        cmdr = await cmdr_setup(eddb_session, client, msg, cmdr_name=cmdr_name)

    if await archive_log(eddb_session, msg=msg, cmdr=cmdr, fname=archive):
        events = []
        for log in logs:
            events += [f'\nFile: {orig_filename if orig_filename else log.name}']
            events += await FileUpload(
                args=None, bot=client, msg=msg, session=None,
                eddb_session=eddb_session, fname=log
            ).parse_log(discord_id=cmdr.id)

        with tempfile.NamedTemporaryFile(mode='w') as tfile:
            async with aiofiles.open(tfile.name, 'w', encoding='utf-8') as aout:
                await aout.write("__Parsed Events__\n" + '\n'.join(events))
                await aout.flush()

            await msg.channel.send('Logs parsed. Detailed per file summary attached.',
                                   file=discord.File(fp=tfile.name, filename='parsed.log'))
