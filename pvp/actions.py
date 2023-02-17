"""
Specific actions for pvp bot
"""
import asyncio
import concurrent.futures as cfut
import datetime
import tempfile
import functools
import logging
import pathlib
import shutil
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
This bot will take uploads of logs from you and process them to derive statistics.
On first upload your in game name will be read from logs and linked to your Discord ID.
This bot will link all this information to your discord id so you can retrieve it by command.
Beyond this bot having access to your logs, uploading files to the bot naturally means
that discord the company has a copy.

The bot will ...
    - Upload an archival copy of your log to a private channel accesible only by devs
    - Read through your log and select events of interest for statistics, i.e. kills, deaths, locations
    - Store parsed information in the bot's database on the server hosting the bots.
    - Store derived statistics from these events
    - The bot will store information about matches and outcomes.

This bot is not intended to spy. It is for entertainment and amusement.
Data collected and processed will not be shared with anyone.
Only the server owner and the dev team have access.

This is not a legal notice, the author is not a lawayer. If you have questions contact the devs.
This privacy statement may be updated as new features are added, the version will be incremented when it is.
Version 1.0 """
DISCLAIMER_QUERY = """

Do you consent to this use of your data?"""
DELETION_WARNING = """WARNING: This is FINAL and IRREVOCABLE.
Proceding will purge all uploaded logs and all information in the database associated
with your discord id.

Do you wish to delete all your information? yes/no
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

    async def prune_bulk(self):  # pragma: no cover, requires deletions
        """
        Prune in bulk chunks of 50 the messages in channel until it is empty.
        """
        for chan in self.msg.channel_mentions:
            await self.msg.channel.send(f"Please confirm channel pruning {chan.name}. Y/N")
            resp = await self.bot.wait_for(
                'message',
                check=lambda m: m.author == self.msg.author and m.channel == self.msg.channel,
                timeout=30
            )

            if resp.content.lower().startswith('y'):
                await self.msg.channel.send(f"Carrying out prune, limiting deletions to 1 batch per {cog.util.DISCORD_RATE_LIMIT}s.")
                msgs = True
                try:
                    while msgs:
                        msgs = await chan.purge(limit=50, reason='Prune requested by admin.')
                        await asyncio.sleep(cog.util.DISCORD_RATE_LIMIT)
                except discord.Forbidden:
                    await self.msg.channel.send("Likely missing perms for channel, need manage messages and read message history.")
                except discord.NotFound:
                    pass

        await self.msg.channel.send("All prune operations completed.")

    async def prune(self):  # pragma: no cover, requires deletions
        """
        Prune all messages from a mentioned channel.
        """
        for chan in self.msg.channel_mentions:
            await self.msg.channel.send(f"Please confirm channel pruning {chan.name}. Y/N")
            resp = await self.bot.wait_for(
                'message',
                check=lambda m: m.author == self.msg.author and m.channel == self.msg.channel,
                timeout=30
            )

            if resp.content.lower().startswith('y'):
                await self.msg.channel.send("Carrying out prune, limiting deletions to 1 per 2s.")
                try:
                    async for msg in chan.history(limit=100000):
                        await msg.delete()
                        await asyncio.sleep(cog.util.DISCORD_RATE_LIMIT)
                except discord.Forbidden:
                    await self.msg.channel.send("Likely missing perms for channel, need manage messages and read history.")
                except discord.NotFound:
                    pass

        await self.msg.channel.send("All prune operations completed.")

    async def filter(self):  # pragma: no cover, too costly to run live
        """
        Regenerate just the stats given the current set of events.
        """
        filter_dir = pathlib.Path(tempfile.mkdtemp(suffix='filter'))
        down_dir = tempfile.mkdtemp(suffix='downs')
        print('Temp downloads and destination.', down_dir, filter_dir)

        log_chan = self.msg.guild.get_channel(cog.util.CONF.channels.pvp_log)
        coros, upload_stage = [], []
        if self.args.attachment:
            self.args.attachment = ' '.join(self.args.attachment)
            await self.bot.send_message(self.msg.channel, f"Resuming filter generation after: {self.args.attachment}")

        try:
            with cfut.ProcessPoolExecutor(max_workers=10) as pool:
                async for msg in log_chan.history(limit=1000000, oldest_first=True):
                    attach_names = {x.filename for x in msg.attachments}
                    if not msg.content.startswith('Discord ID') or \
                            (self.args.attachment and self.args.attachment not in attach_names):
                        continue
                    if self.args.attachment and self.args.attachment in attach_names:
                        self.args.attachment = None  # Start reading next one
                        continue

                    discord_id = int(msg.content.split('\n')[0].replace('Discord ID: ', ''))
                    cmdr = pvp.schema.get_pvp_cmdr(self.eddb_session, cmdr_id=discord_id)
                    if not cmdr:  # If you don't have the pvp_cmdr, fill it
                        cmdr_name = msg.content.split('\n')[1].replace('CMDR: ', '')
                        cmdr = pvp.schema.update_pvp_cmdr(self.eddb_session, discord_id,
                                                          name=cmdr_name, hex_colour=None)

                    for attach in msg.attachments:
                        with tempfile.NamedTemporaryFile(dir=down_dir, delete=False) as tfile:
                            await attach.save(tfile.name)
                            tfile.close()
                            print(f'Filtering: {attach.filename}')

                            # Ensure log exists
                            pvp_log = await pvp.schema.add_pvp_log(self.eddb_session, tfile.name, cmdr_id=cmdr.id)
                            pvp_log.filename = attach.filename
                            pvp_log.msg_id = msg.id

                            try:
                                coro = await pvp.journal.filter_tempfile(
                                    pool=pool, dest_dir=filter_dir,
                                    fname=tfile.name, attach_fname=attach.filename
                                )
                                coros += [coro]
                                upload_stage += [[cmdr, pvp_log, coro]]
                            except pvp.journal.ParserError as exc:
                                await self.msg.channel.send(str(exc))

                self.eddb_session.commit()
                await asyncio.wait(coros)

                filter_chan = self.bot.get_channel(cog.util.CONF.channels.pvp_filter)
                await pvp.journal.upload_filtered_logs(upload_stage, log_chan=filter_chan)

        finally:
            shutil.rmtree(down_dir)
            shutil.rmtree(filter_dir)

        return f"Filtered: {len(coros)} files in {filter_dir}."

    async def regenerate(self):  # pragma: no cover, don't hit the guild repeatedly
        """
        Regenerate the PVP database by reparsing logs.
        This will:
            - Drop and recreate all tables that aren't pvp_cmdrs.
            - Process all logs in order of upload to archive channel on server.
            - Import and parse selected events.
            - Once done, regenerate all stats for each CMDR at the end.
        """
        await self.bot.send_message(self.msg.channel, "All uploads will be denied while regeneration underway.")
        response = "Regenerating PVP database has completed."
        pvp.schema.recreate_tables(keep_cmdrs=True)

        down_dir = tempfile.mkdtemp(suffix='downs')
        print(f"Regeneration down dir: {down_dir}")
        coros = []
        try:
            # Channel archiving filtered pvp_logs
            filter_chan = self.bot.get_channel(cog.util.CONF.channels.pvp_filter)
            with cfut.ProcessPoolExecutor(max_workers=10) as pool:
                for pvp_log in pvp.schema.get_filtered_pvp_logs(self.eddb_session):
                    msg = await filter_chan.fetch_message(pvp_log.filtered_msg_id)
                    if not msg.content.startswith('Discord ID'):
                        continue

                    discord_id = int(msg.content.split('\n')[0].replace('Discord ID: ', ''))
                    cmdr = pvp.schema.get_pvp_cmdr(self.eddb_session, cmdr_id=discord_id)
                    if not cmdr:  # If you don't have the pvp_cmdr, fill it
                        cmdr_name = msg.content.split('\n')[1].replace('CMDR: ', '')
                        cmdr = pvp.schema.update_pvp_cmdr(self.eddb_session, discord_id,
                                                          name=cmdr_name, hex_colour=None)

                    for attach in msg.attachments:
                        with tempfile.NamedTemporaryFile(dir=down_dir, delete=False) as tfile:
                            await attach.save(tfile.name)
                            tfile.close()
                            print(f'Parsing: {attach.filename}')

                            try:
                                coro = await process_tempfile(
                                    pool=pool, cmdr_id=discord_id,
                                    fname=tfile.name, attach_fname=attach.filename
                                )
                                coros += [coro]
                                await coro  # TODO: Possible dupes if parallel.
                            except (pvp.journal.ParserError, zipfile.BadZipfile) as exc:
                                await self.msg.channel.send(str(exc))

                if not coros:
                    await self.msg.channel.send('No PVPLogs set, please import database from a source.')
                    return

                await asyncio.wait(coros)
                events = functools.reduce(lambda x, y: x + y, [x.result() for x in coros])

            with tempfile.NamedTemporaryFile(mode='w') as tfile:
                async with aiofiles.open(tfile.name, 'w', encoding='utf-8') as aout:
                    await aout.write("__Parsed Events__\n" + '\n'.join(events))
                    await aout.flush()

                await self.msg.channel.send('Logs parsed. Summary attached.',
                                            file=discord.File(fp=tfile.name, filename='parsed.log'))

        except discord.Forbidden as exc:
            logging.getLogger(__name__).error('Discord error: %s', exc)
            response = f"Missing message history permission on: {filter_chan.name} on {filter_chan.guild.name}"
        except discord.HTTPException:
            response = "Received HTTP Error, may be intermittent issue. Investigate."
        finally:
            shutil.rmtree(down_dir)

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


class FileUpload(PVPAction):  # pragma: no cover
    """
    Handle a file upload and scan for pvp information.
    """
    async def file_upload(self, *, pool, filter_dir):
        """
        Handle the file upload. Parts that are long will offload to the pool.
        This involves several steps now for each attachment:
        - Upload to archive if new.
        - Filter the file and upload a filtered version to filter channel.
        - Process all events in the file and return a message to user with processed information.

        Args:
            pool: An instance of ProcessPoolExecutor.
            filter_dir: The directory to put all filtered files.

        Returns: A list of events parsed from the log uploaded.
        """
        filter_chan = self.bot.get_channel(cog.util.CONF.channels.pvp_filter)

        for attach in self.msg.attachments:
            if attach.size > cog.util.DISCORD_FILE_LIMIT:
                await self.bot.send_message(self.msg.channel, f'Rejecting file {attach.filename}, please upload files < 8MB')
                continue

            with tempfile.NamedTemporaryFile(dir=filter_dir, delete=False) as tfile:
                await attach.save(tfile.name)
                tfile.close()

                cmdr = await ensure_cmdr_exists(self.eddb_session, client=self.bot, msg=self.msg, fname=tfile.name)
                pvp_log = await archive_log(self.eddb_session, msg=self.msg, cmdr=cmdr, fname=tfile.name)
                if not pvp_log:
                    await self.bot.send_message(self.msg.channel, f'Log {attach.filename}, has already been uploaded. Ignoring.')
                    continue

                try:
                    # Filter first
                    filter_coro = await pvp.journal.filter_tempfile(
                        pool=pool, dest_dir=filter_dir,
                        fname=tfile.name, output_fname=pvp_log.filtered_filename, attach_fname=attach.filename
                    )
                    upload_stage = [[cmdr, pvp_log, filter_coro]]
                    await filter_coro
                    await pvp.journal.upload_filtered_logs(upload_stage, log_chan=filter_chan)

                    # Process the log that was filtered
                    process_coro = await process_tempfile(
                        pool=pool, cmdr_id=self.msg.author.id,
                        fname=filter_coro.result(), attach_fname=attach.filename
                    )
                    await process_coro
                    return process_coro.result()

                except (pvp.journal.ParserError, zipfile.BadZipfile):
                    await self.bot.send_message(self.msg.channel, f'Error with {attach.filename}. We only support zips and text files.')

    async def execute(self):
        filter_dir = pathlib.Path(tempfile.mkdtemp(suffix='filter'))
        try:
            with cfut.ProcessPoolExecutor(max_workers=1) as pool:
                events = await self.file_upload(pool=pool, filter_dir=filter_dir)

                with tempfile.NamedTemporaryFile(mode='w') as tfile:
                    async with aiofiles.open(tfile.name, 'w', encoding='utf-8') as aout:
                        await aout.write("__Parsed Events__\n" + '\n'.join(events))
                        await aout.flush()
                    await self.msg.channel.send('Logs parsed. Summary attached.',
                                                file=discord.File(fp=tfile.name, filename='parsed.log'))
        finally:
            shutil.rmtree(filter_dir)


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
            [f'{prefix}cmdr', 'Setup and modify your cmdr settings.'],
            [f'{prefix}donate', 'Information on supporting the dev.'],
            [f'{prefix}feedback', 'Give feedback or report a bug'],
            [f'{prefix}help', 'This help command.'],
            [f'{prefix}log', 'Show recent PVP events parsed'],
            [f'{prefix}match', 'Create and manage pvp matches.'],
            [f'{prefix}near', 'Find things near you.'],
            [f'{prefix}privacy', 'Display the privacy explanation.'],
            [f'{prefix}repair', 'Show the nearest orbitals with shipyards'],
            [f'{prefix}route', 'Plot the shortest route between these systems'],
            [f'{prefix}stats', 'PVP statistics for you or another CMDR'],
            [f'{prefix}status', 'Info about this bot'],
            [f'{prefix}time', 'Show game time and time to ticks'],
            [f'{prefix}trigger', 'Calculate fort and um triggers for systems'],
            [f'{prefix}whois', 'Search for commander on inara.cz'],
        ]
        response = overview + '\n\n' + cog.tbl.format_table(lines, header=True)[0]
        await self.bot.send_ttl_message(self.msg.channel, response)
        try:
            await self.msg.delete()
        except discord.HTTPException:
            pass


class Cmdr(PVPAction):
    """
    Provide a management interface for cmdr settings.
    """
    async def execute(self):
        await cmdr_setup(self.eddb_session, self.bot, self.msg, cmdr_name=None)


LOG_MAP = {
    'kills': 'PVPKill',
    'deaths': 'PVPDeath',
    'interdictions': 'PVPInterdiction',
    'interdicteds': 'PVPInterdicted',
    'escapes': 'PVPEscapedInterdicted',
    'locations': 'PVPLocation',
}


class Log(PVPAction):
    """
    Display the most recent parsed events.
    """
    async def execute(self):
        try:
            events = None
            if self.args.events:
                events = [getattr(pvp.schema, LOG_MAP[x]) for x in self.args.events]
            if self.args.after:
                self.args.after = datetime.datetime.strptime(
                    self.args.after, cog.util.TIME_STRP[:-1]
                ).replace(tzinfo=datetime.timezone.utc).timestamp()

            async with pvp.schema.create_log_of_events(self.eddb_session, cmdr_id=self.msg.author.id,
                                                       events=events, last_n=self.args.limit,
                                                       after=self.args.after) as log_files:
                if not log_files:
                    await self.bot.send_message(self.msg.channel, 'No recorded PVP events for CMDR.')

                for ind, fname in enumerate(log_files, start=1):
                    await self.bot.send_message(self.msg.channel, f'Part {ind} of logs requested.',
                                                file=discord.File(fp=fname))

        except KeyError:
            await self.bot.send_message(self.msg.channel, f'Invalid log event, choose from: {list(LOG_MAP.keys())}')

        except ValueError:
            await self.bot.send_message(self.msg.channel, 'Invalid after format. Write dates like: 2023-04-30T20:10:44')


class Match(PVPAction):
    """
    Prepare a match between players.
    """
    def _validate_cmdrs(self):
        """
        Validate the CMDRs mentioned or listed as arguments.

        Raises:
            ValueError: One or more CMDRs could not be found.

        Returns: A list of PVPCmdrs to process.
        """
        cmdrs, failed = set(), []

        for mention in self.msg.mentions:
            cmdr = pvp.schema.get_pvp_cmdr(self.eddb_session, cmdr_id=mention.id)
            if cmdr:
                cmdrs.add(cmdr)
            else:
                failed += [f"CMDR not found for: {mention.mention}. Please register yourself with: {self.bot.prefix}cmdr"]

        for name in self.args.players:
            cmdr = pvp.schema.get_pvp_cmdr(self.eddb_session, cmdr_name=name)
            if cmdr:
                cmdrs.add(cmdr)
            else:
                failed += [f"CMDR not found for: {name}. Please register yourself with: {self.bot.prefix}cmdr"]

        if failed:
            raise ValueError('\n'.join(failed))

        return cmdrs

    async def start(self, match):
        """
        Start the Match. Will roll teams if there hasn't been a roll.

        Args:
            match: PVPMatch that tracks the current match.

        Returns: response, embed - A response message and embed, if needed they will have values otherwise None.
        """
        response = "The current match has been started."
        match.state = pvp.schema.PVPMatchState.STARTED

        if len(match.teams_dict().keys()) == 1:
            response += "\nTeams have been rolled and the match has started."
            response += "\nTo finish match use: `{self.bot.prefix}match win` or `{self.bot.prefix}match cancel`"
            match.roll_teams()

        return response, discord.Embed.from_dict(match.embed_dict())

    async def cancel(self, match):
        """
        Cancel a Match. Deletes the match completely.

        Args:
            match: PVPMatch that tracks the current match.

        Returns: response, embed - A response message and embed, if needed they will have values otherwise None.
        """
        self.eddb_session.delete(match)
        response = "The current match has been cancelled."

        return response, None

    async def add(self, match):
        """
        Add player to the Match.

        Args:
            match: PVPMatch that tracks the current match.

        Returns: response, embed - A response message and embed, if needed they will have values otherwise None.
        """
        response, embed = "", None

        for cmdr in self._validate_cmdrs():
            player = match.add_player(self.eddb_session, cmdr_id=cmdr.id)
            if player:
                response += f"CMDR {cmdr.name} added to match!\n"
            else:
                response += f"CMDR {cmdr.name} already in match!\n"

            if len(match.players) == match.limit:
                temp_resp, embed = await self.start(match)
                response += temp_resp
                break

        return response, embed

    async def remove(self, match):
        """
        Remove player from Match.

        Args:
            match: PVPMatch that tracks the current match.

        Returns: response, embed - A response message and embed, if needed they will have values otherwise None.
        """
        cmdrs = self._validate_cmdrs()
        pvp.schema.remove_players_from_match(
            self.eddb_session, match_id=match.id, cmdr_ids=[cmdr.id for cmdr in cmdrs]
        )
        response = '\n'.join([f'CMDR {cmdr.name} removed from match!' for cmdr in cmdrs])

        return response, None

    async def join(self, match):
        """
        Join yourself to a match. A shortcut to using add with self mention.

        Args:
            match: PVPMatch that tracks the current match.

        Returns: response, embed - A response message and embed, if needed they will have values otherwise None.
        """
        response, embed = "", None

        cmdr = pvp.schema.get_pvp_cmdr(self.eddb_session, cmdr_id=self.msg.author.id)
        if not cmdr:
            response += f"CMDR not found for: {self.msg.author.mention}. Please register yourself with: {self.bot.prefix}cmdr"

        else:
            player = match.add_player(self.eddb_session, cmdr_id=cmdr.id)
            if player:
                response += f"CMDR {cmdr.name} added to match!\n"
            else:
                response += f"CMDR {cmdr.name} already in match!\n"

            if len(match.players) == match.limit:
                temp_resp, embed = await self.start(match)
                response += f'\n{temp_resp}'

        return response, embed

    async def leave(self, match):
        """
        Leave a match in the channel.

        Args:
            match: PVPMatch that tracks the current match.

        Returns: response, embed - A response message and embed, if needed they will have values otherwise None.
        """
        response = None

        cmdr = pvp.schema.get_pvp_cmdr(self.eddb_session, cmdr_id=self.msg.author.id)
        if cmdr:
            response = f"CMDR {cmdr.name} removed from match!\n"
            pvp.schema.remove_players_from_match(
                self.eddb_session, match_id=match.id, cmdr_ids=[cmdr.id]
            )

        return response, None

    async def show(self, match):
        """
        Display the current Match info.

        Args:
            match: PVPMatch that tracks the current match.

        Returns: response, embed - A response message and embed, if needed they will have values otherwise None.
        """
        response, embed = "", None

        if match:
            embed = discord.Embed.from_dict(match.embed_dict())
        else:
            response = f"No pending match.\nPlease setup one using `{self.bot.prefix}match setup`"

        return response, embed

    async def setup(self, match):
        """
        Setup a new Match.

        Args:
            match: PVPMatch that tracks the current match.

        Returns: response, embed - A response message and embed, if needed they will have values otherwise None.
        """
        response, embed = "", None

        if match:
            response = "A match is already pending.\nPlease start it or cancel it before creating a new one."
        else:
            match = pvp.schema.add_pvp_match(self.eddb_session, discord_channel_id=self.msg.channel.id, limit=self.args.limit)
            embed = discord.Embed.from_dict(match.embed_dict())

        return response, embed

    async def reroll(self, match):
        """
        Reroll teams.

        Args:
            match: PVPMatch that tracks the current match.

        Returns: response, embed - A response message and embed, if needed they will have values otherwise None.
        """
        match.roll_teams()
        embed = discord.Embed.from_dict(match.embed_dict())

        return None, embed

    async def win(self, match):
        """
        Conclude a match with victory for the mentioned cmdr's team.

        Args:
            match: PVPMatch that tracks the current match.

        Returns: response, embed - A response message and embed, if needed they will have values otherwise None.
        """
        response, embed = "", None

        for cmdr in self._validate_cmdrs():
            player = match.get_player(cmdr_id=cmdr.id)
            if player:
                match.finish(winning_team=player.team)
                break

        question = "Do you want to rematch with existing teams? Yes/No"
        await self.bot.send_message(self.msg.channel, question)
        answer = await self.bot.wait_for(
            'message',
            check=lambda m: m.author == self.msg.author and m.channel == self.msg.channel,
            timeout=30
        )
        if answer.content.lower().startswith('y'):
            match.clone(self.eddb_session)
            response += f"A new match has been created with existing teams. To randomize teams: {self.bot.prefix}match reroll\n"

        response += "Team {player.team} has won!\n\nCongrats: "
        response += ", ".join([f"CMDR {x.cmdr.name}" for x in match.winners])

        return response, embed

    async def execute(self):
        """
        All methods will take the match in question. Only create one when requested via setup.
        """
        match = pvp.schema.get_pvp_match(self.eddb_session, discord_channel_id=self.msg.channel.id)
        try:
            if not match and self.args.subcmd not in {'setup'}:
                await self.bot.send_message(
                    self.msg.channel,
                    f"No pending match.\nPlease setup one using `{self.bot.prefix}match setup`"
                )
                return

            if match and self.args.subcmd not in {'add', 'remove', 'join', 'leave'} and match.state == pvp.schema.PVPMatchState.STARTED:
                await self.bot.send_message(
                    self.msg.channel,
                    f"Please finish started match with: `{self.bot.prefix}match win` or `{self.bot.prefix}match cancel"
                )
                return

            func = getattr(self, self.args.subcmd)
            response, embed = await func(match)
            if response or embed:
                await self.bot.send_message(self.msg.channel, response, embed=embed)

        except AttributeError as exc:
            traceback.print_exc()
            raise cog.exc.InvalidCommandArgs("Bad subcommand of `!match`, see `!match -h` for help.") from exc

        except TypeError:  # Default case, no subcmd set
            response, embed = await self.show(match)
            await self.bot.send_message(self.msg.channel, response, embed=embed)

        except ValueError as exc:  # _validate_cmdrs has failed to validate the mentioned or named cmdrs.
            response = str(exc)
            await self.bot.send_message(self.msg.channel, response)


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


class Privacy(PVPAction):  # Pragma no cover, very destructive test.
    """
    Manage all privacy related functions of the bot, including disclosure and information deletion.
    """
    async def delete(self):
        """
        Handle deleting of a users data.
        """
        await self.bot.send_message(self.msg.channel, "This may take a while due to rate limiting. We will ping you when finished.")

        cmdr_id = self.msg.author.id
        log_chan = self.msg.guild.get_channel(cog.util.CONF.channels.pvp_log)
        filter_chan = self.bot.get_channel(cog.util.CONF.channels.pvp_filter)
        await pvp.journal.purge_uploaded_logs(log_chan=log_chan, cmdr_id=cmdr_id)
        if filter_chan != log_chan:
            await pvp.journal.purge_uploaded_logs(log_chan=filter_chan, cmdr_id=cmdr_id)

        pvp.schema.purge_cmdr(self.eddb_session, cmdr_id=cmdr_id)

        return f"{self.msg.author.mention} All your information has been deleted. Have a nice day."

    async def execute(self):
        response = DISCLAIMER

        if self.args.delete:
            await self.bot.send_message(self.msg.channel, DELETION_WARNING)
            resp = await self.bot.wait_for(
                'message',
                check=lambda m: m.author == self.msg.author and m.channel == self.msg.channel,
                timeout=60
            )

            response = "Aborting data deletion."
            if resp.content.lower().startswith('y'):
                response = await self.delete()

        await self.bot.send_message(self.msg.channel, response)


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
                        style=discord.TextStyle.short, default="B20000", placeholder="B20000", required=True)
    name = dui.TextInput(label='In Game CMDR Name', min_length=5, max_length=EDDB_LEN['pvp_name'],
                         style=discord.TextStyle.short, placeholder="Your in game name", required=True)

    def __init__(self, *args, existing, cmdr_name=None, **kwargs):
        if existing:
            self.hex.default = existing.hex
            self.name.default = existing.name
            self.title = 'Update CMDR Registration'
        if cmdr_name:
            self.name.default = cmdr_name

        super().__init__(*args, **kwargs)

    async def on_submit(self, interaction: discord.Interaction):
        if self.name:
            self.name = pvp.journal.clean_cmdr_name(str(self.name))

        int(str(self.hex), 16)  # Validate the hex here
        await interaction.response.send_message(f'You are now registered, CMDR {self.name}!', ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error):
        await interaction.response.send_message('Registration failed, please try again.', ephemeral=True)


async def cmdr_setup(eddb_session, client, msg, *, cmdr_name=None):  # pragma: no cover, heavily dependent on discord.py
    """
    Perform cmdr setup via interactive questions. If exists, update answers.
    """
    def check_button(orig_author, sent, inter):
        """
        Check the interaction is for same message and author.
        """
        return inter.message == sent and inter.user == orig_author

    view = dui.View().\
        add_item(dui.Button(label="Yes", custom_id="Yes", style=discord.ButtonStyle.green)).\
        add_item(dui.Button(label="No", custom_id="No", style=discord.ButtonStyle.red))

    sent = await msg.channel.send(DISCLAIMER + DISCLAIMER_QUERY, view=view)
    inter = await client.wait_for('interaction', check=functools.partial(check_button, msg.author, sent))

    if inter.data['custom_id'] == "No":
        await inter.response.send_message('Aborting registration. No data stored.', ephemeral=True)
        return False

    existing = pvp.schema.get_pvp_cmdr(eddb_session, cmdr_id=msg.author.id)
    modal = CMDRRegistration(existing=existing, cmdr_name=cmdr_name, timeout=cog.util.DISCORD_TIMEOUT)

    await inter.response.send_modal(modal)
    if await modal.wait():
        await msg.channel.send('Timeout on registration, please try again.')
        return False

    return pvp.schema.update_pvp_cmdr(eddb_session, msg.author.id, name=str(modal.name), hex_colour=str(modal.hex))


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


async def archive_log(eddb_session, *, msg, cmdr, fname):
    """
    Upload a copy of the log file or zip to the archiving channel set for server.

    Args:
        eddb_session: A session onto the EDDB db.
        msg: The msg that initiated the DM upload.
        log_chan: The disocrd.TextChannel that contains all log backups.
        cmdr: The PVPCmdr object.
        fname: The filename of the log OR zip file.

    Returns: The PVPLog if it is new and was uploaded. Otherwise None
    """
    logging.getLogger(__name__).error("Archive FNAME: %s", fname)
    # Files are hashed to check for dupes on add.
    pvp_log = await pvp.schema.add_pvp_log(eddb_session, fname=fname, cmdr_id=cmdr.id)
    if pvp_log.msg_id:
        await msg.channel.send(f'Log already uploaded at {pvp_log.updated_date}.',)
        return None

    pvp_log.filename = filename_for_upload(cmdr.name, id_num=pvp_log.id, archive=cog.util.is_zipfile(fname))
    log_chan = cog.util.BOT.get_channel(cog.util.CONF.channels.pvp_log)
    msg = await log_chan.send(pvp.journal.upload_text(cmdr),
                              file=discord.File(fp=fname, filename=pvp_log.filename))
    pvp_log.msg_id = msg.id

    return pvp_log


def process_archive(fname, *, attach_fname, cmdr_id):
    """
    Parse an entire archive and return events parsed as a list of strings.

    Args:
        fname: The filename of the journal fragment.
        attach_fname: The original discord.Attachment.filename.
        cmdr_id: The discord id of the CMDR.

    Returns: A list of strings of the events parsed.
    """
    events = [f'Archive: {fname}']
    try:
        with cog.util.extracted_archive(fname) as logs, cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            for log in logs:
                events += process_log(log, cmdr_id=cmdr_id, eddb_session=eddb_session)
    except zipfile.BadZipfile:
        msg = f'ERROR unzipping {attach_fname}, please check archive.'
        logging.getLogger(__name__).error(msg)
        raise

    return events


def process_log(fname, *, cmdr_id, eddb_session=None):
    """
    Parse a single log file and return events parsed as a list of strings.

    Args:
        fname: The filename of the journal fragment.
        cmdr_id: The discord id of the CMDR.
        eddb_session: Optionally pass in the session onto EDDB to use. If not passed, will create a new session.

    Returns: A list of strings of the events parsed.
    """
    if not cog.util.is_log_file(fname):  # Ignore non valid logs
        return []

    if eddb_session:
        parser = pvp.journal.Parser(fname=fname, cmdr_id=cmdr_id, eddb_session=eddb_session)
        parser.load()

        # Do not return unbound db objects.
        return [f'\nFile: {fname}'] + [str(x) for x in parser.parse()]

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session_new:
        parser = pvp.journal.Parser(fname=fname, cmdr_id=cmdr_id, eddb_session=eddb_session_new)
        parser.load()

        # Do not return unbound db objects.
        return [f'\nFile: {fname}'] + [str(x) for x in parser.parse()]


async def process_tempfile(*, attach_fname, fname, pool, cmdr_id):
    """
    High level process a tempfile. Handle parsing contents if it is a log or zip.

    Args:
        attach_fname: The original name of the discord.Attachment.
        file: The filename of the local file.
        pool: An instance of cfut.ProcessPoolExecutor.
        cmdr_id: The id of the cmdr who is associated with log.

    Raises:
        pvp.journal.ParserError: Unsupported file type.

    Returns: The events found in the tempfile, a list of strings.
    """
    if await cog.util.is_log_file_async(fname):
        func = functools.partial(
            process_log, fname, cmdr_id=cmdr_id
        )

    elif await cog.util.is_zipfile_async(fname):
        func = functools.partial(
            process_archive, fname, attach_fname=attach_fname, cmdr_id=cmdr_id
        )

    else:
        raise pvp.journal.ParserError("Unsupported file type.")

    return asyncio.get_event_loop().run_in_executor(pool, func)


async def ensure_cmdr_exists(eddb_session, *, client, msg, fname):  # pragma: no cover, relies on underlying interactive cmdr_setup
    """
    Ensure a PVPCmdr exists for the author of the msg.
    In the case it doesn't, peek in the log file or archive and find cmdr name.
    Then run the cmdr registration.

    Args:
        eddb_session: A session onto the EDDB db.
        client: A instance of the bot.
        msg: The originating message.
        fname: The filename where the attachment was saved.

    Returns: The PVPCmdr that has the discord id of the msg.
    """
    cmdr = pvp.schema.get_pvp_cmdr(eddb_session, cmdr_id=msg.author.id)
    if not cmdr:
        if await cog.util.is_log_file_async(fname):
            cmdr_name = await pvp.journal.find_cmdr_name(fname)

        elif await cog.util.is_zipfile_async(fname):
            async with cog.util.extracted_archive_async(fname) as logs:
                for log in logs:
                    if await cog.util.is_log_file_async(log):
                        cmdr_name = await pvp.journal.find_cmdr_name(fname)
                        break
        cmdr = await cmdr_setup(eddb_session, client, msg, cmdr_name=cmdr_name)

    return cmdr
