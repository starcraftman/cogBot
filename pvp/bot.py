"""
This is the PVP bot. Everything is started upon main() execution. To invoke from root:
    python -m pvp.bot

See cog.bot for more information.
"""
import asyncio
import datetime
import logging
import os
import pathlib
import random
import re
import sys
import tempfile
import time
import zipfile

import aiofiles
import apiclient
import discord
import websockets.exceptions
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.get_event_loop().set_debug(True)
finally:
    print("Default event loop:", asyncio.get_event_loop())

import cog.exc
import cog.util
import cogdb
import cogdb.query
from cog.bot import CogBot
import pvp.parse
import pvp.actions


SYNC_NOTICE = """Synchronizing sheet changes.

Your command will resume after a short delay of about {} seconds. Thank you for waiting."""
SYNC_RESUME = """{} Resuming your command:
    **{}**"""
MAX_FILE_SIZE = 8 * 1024 * 1024


class PVPBot(CogBot):  # pragma: no cover
    """
    The main bot, hooks onto on_message primarily and waits for commands.
    """
    def __init__(self, prefix, **kwargs):
        super().__init__(prefix, **kwargs)
        # Replace the parser with pvp one.
        self.parser = pvp.parse.make_parser(prefix)
        self.once = False

    async def on_ready(self):
        """
        Event triggered when connection established to discord and bot ready.
        """
        log = logging.getLogger(__name__)
        log.info('Logged in as: %s', self.user.name)
        log.info('Available on following guilds:')
        for guild in self.guilds:
            log.info('  "%s" with id %s', guild.name, guild.id)

        self.emoji.update(self.guilds)

        # This block is effectively a one time setup.
        if not self.once:
            self.once = True
            asyncio.ensure_future(asyncio.gather(
                presence_task(self),
                simple_heartbeat(),
                cog.util.CONF.monitor(),
            ))
            self.deny_commands = False

        print('PVP Bot Ready!')

    async def ignore_message(self, message):
        """
        Determine whether the message should be ignored.

        Ignore messages not directed at bot and any commands that aren't
        from an admin during deny_commands == True.
        Allow DMs.
        """
        # Ignore lines by bot
        if message.author.bot:
            return True

        # Accept only admin commands if denying
        if self.deny_commands and not message.content.startswith(f'{self.prefix}admin'):
            await self.send_message(message.channel, f"__Admin Mode__\n\nOnly `{self.prefix}admin` commands accepted.")
            return True

        return False

    async def on_message_edit(self, before, after):
        """
        Only process commands that were different from before.
        """
        if before.content != after.content and after.content.startswith(self.prefix):
            await self.on_message(after)

    async def handle_dms(self, msg):
        """
        Any hooks to respond to dms.
        """
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            for attach in msg.attachments:
                if attach.size > MAX_FILE_SIZE:
                    await self.send_message(msg.channel, f'Rejecting file {attach.filename}, please upload files < 8MB')
                    continue

                with tempfile.NamedTemporaryFile() as tfile:
                    await attach.save(tfile.name)

                    if cog.util.is_zipfile(tfile):
                        try:
                            with cog.util.extracted_archive(tfile.name) as logs:
                                await pvp.actions.process_logs(
                                    eddb_session, list(logs),
                                    client=self, msg=msg, archive=tfile.name
                                )
                        except zipfile.BadZipfile:
                            await self.send_message(msg.channel, f'Error unzipping {attach.filename}, please check archive.')

                    else:
                        await pvp.actions.process_logs(
                            eddb_session, [pathlib.Path(tfile.name)],
                            client=self, msg=msg, archive=tfile.name, orig_filename=attach.filename
                        )

    async def on_message(self, message):
        """
        Intercepts every message sent to guild!

        Notes:
            message.author - Returns member object
                roles -> List of Role objects. First always @everyone.
                    roles[0].name -> String name of role.
            message.channel - Channel object.
                name -> Name of channel
                guild -> guild of channel
                    members -> Iterable of all members
                    channels -> Iterable of all channels
                    get_member_by_name -> Search for user by nick
            message.content - The text
        """
        content = message.content
        author = message.author
        channel = message.channel

        if await self.ignore_message(message):
            return

        if channel.type == discord.ChannelType.private:
            await self.handle_dms(message)
            return

        if not message.content.startswith(self.prefix):
            await self.message_hooks(message)
            return

        log = logging.getLogger(__name__)
        log.info("guild: '%s' Channel: '%s' User: '%s' | %s",
                 channel.guild, channel.name, author.name, content)

        edit_time = message.edited_at
        try:
            content = re.sub(r'<[#@]\S+>', '', content).strip()  # Strip mentions from text

            with cogdb.session_scope(cogdb.Session) as session,\
                    cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
                # Check permissions before full parsing
                cmd = cog.bot.cmd_from_content(self.prefix, content)
                cogdb.query.check_perms(session, message, cmd)

                args = self.parser.parse_args(re.split(r'\s+', content))
                cls = getattr(pvp.actions, args.cmd)
                await cls(args=args, bot=self, msg=message, session=session, eddb_session=eddb_session).execute()

        except cog.exc.ArgumentParseError as exc:
            log.exception("Failed to parse command. '%s' | %s", author.name, content)
            exc.write_log(log, content=content, author=author, channel=channel)
            if 'invalid choice' in str(exc) or 'argument {' in str(exc):
                # Default show top level commands
                await pvp.actions.Help(args=None, bot=self, msg=message, session=None, eddb_session=None).execute()
            else:
                try:
                    self.parser.parse_args(content.split(' ')[0:1] + ['--help'])
                except cog.exc.ArgumentHelpError:
                    exc.message = 'Invalid command use. Check the command help.'
                    exc.message += f"\n{len(exc.message) * '-'}\n{exc.message}"

            await self.send_ttl_message(channel, str(exc))
            try:
                if edit_time == message.edited_at:
                    await message.delete()
            except discord.DiscordException:
                pass

        except cog.exc.CogException as exc:
            exc.write_log(log, content=content, author=author, channel=channel)
            if isinstance(exc, cog.exc.UserException):
                await self.send_ttl_message(channel, str(exc))
                try:
                    if edit_time == message.edited_at:
                        await message.delete()
                except discord.DiscordException:
                    pass
            else:
                await self.send_message(channel, str(exc))

        except discord.DiscordException as exc:
            if exc.args[0].startswith("BAD REQUEST (status code: 400"):
                resp = "Response would be > 2000 chars, I cannot transmit it to Discord."
                resp += "\n\nIf this useage is valid see Gears."

                await self.send_ttl_message(channel, resp)
                try:
                    if edit_time == message.edited_at:
                        await message.delete()
                except discord.DiscordException:
                    pass
            else:
                gears = self.get_member_by_substr("gearsand").mention
                await self.send_message(channel, f"A critical discord error! {gears}.")
            line = "Discord.py Library raised an exception"
            line += cog.exc.log_format(content=content, author=author, channel=channel)
            log.exception(line)

        except apiclient.errors.Error as exc:
            line = "Google Sheets API raised an exception"
            line += "\n" + str(exc) + "\n\n"
            line += cog.exc.log_format(content=content, author=author, channel=channel)
            log.exception(line)


async def presence_task(bot, delay=180):  # pragma: no cover
    """
    Manage the ultra important task of bot's played game.
    """
    print('Presence task started')
    lines = [
        'Looking for prey',
        'Provide suggestions here',
    ]
    ind = 0
    while True:
        try:
            await bot.change_presence(activity=discord.Game(name=lines[ind]))
        except websockets.exceptions.ConnectionClosed:
            pass

        ind = (ind + 1) % len(lines)
        await asyncio.sleep(delay)


async def simple_heartbeat(delay=30):  # pragma: no cover
    """ Simple heartbeat function to check liveness of main loop. """
    hfile = os.path.join(tempfile.gettempdir(), 'hbeatpvp')
    print(hfile)
    while True:
        async with aiofiles.open(hfile, 'w') as fout:
            await fout.write(f'{delay} {datetime.datetime.utcnow().replace(microsecond=0)}\n')
        await asyncio.sleep(delay)


def main():  # pragma: no cover
    """ Entry here! """
    random.seed(time.time())
    sqlalchemy_log = '--db' in sys.argv
    if sqlalchemy_log:
        print("Enabling SQLAlchemy log.")
    else:
        print("To enable SQLAlchemy log append --db flag.")
    cog.util.init_logging(sqlalchemy_log)

    # Intents for: member info, dms and message.content access
    intents = discord.Intents.default()
    intents.members = True  # pylint: disable=assigning-non-slot
    intents.messages = True  # pylint: disable=assigning-non-slot
    intents.message_content = True  # pylint: disable=assigning-non-slot
    cog.util.BOT = PVPBot("!", scheduler_delay=0, intents=intents)

    token = cog.util.CONF.discord.unwrap.get(os.environ.get('TOKEN', 'pvp'))
    print("Waiting on connection to Discord ...")
    try:
        loop = asyncio.get_event_loop()
        # BLOCKING: N.o. e.s.c.a.p.e.
        loop.run_until_complete(cog.util.BOT.start(token))
    except KeyboardInterrupt:
        loop.run_until_complete(cog.util.BOT.close())
    finally:
        loop.close()


if __name__ == "__main__":  # pragma: no cover
    main()
