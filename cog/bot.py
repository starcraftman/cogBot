"""
This is the main bot. Everything is started upon main() execution. To invoke from root:
    python -m cog.bot

Some useful docs on libraries
-----------------------------
Python 3.5 async tutorial:
    https://snarky.ca/how-the-heck-does-async-await-work-in-python-3-5/

asyncio (builtin package):
    https://docs.python.org/3/library/asyncio.html

discord.py: The main discord library, hooks events.
    https://discordpy.readthedocs.io/en/latest/api.html

aiozmq: Async python bindings for zmq. (depends on pyzmq)
    https://aiozmq.readthedocs.io/en/v0.8.0/

ZeroMQ: Listed mainly as a reference for core concepts.
    http://zguide.zeromq.org/py:all
"""
import asyncio
import datetime
import logging
import os
import pprint
import re
import tempfile

import aiofiles
import apiclient
import discord
import websockets.exceptions
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.get_event_loop().set_debug(True)
except ImportError:
    print("Falling back to default python loop.")
finally:
    print("Default event loop:", asyncio.get_event_loop())

import cog.actions
import cog.exc
import cog.inara
import cog.parse
import cog.scheduler
import cog.sheets
import cog.util
import cogdb
import cogdb.scanners
import cogdb.query


class EmojiResolver():
    """
    Map emoji embeds onto the text required to make them appear.
    """
    def __init__(self):
        # For each guild, store a dict of emojis on that guild
        self.emojis = {}

    def __str__(self):
        """ Just dump the emoji db. """
        return pprint.pformat(self.emojis, indent=2)

    def update(self, guilds):
        """
        Update the emoji dictionary. Call this in on_ready.
        """
        for guild in guilds:
            emoji_names = [emoji.name for emoji in guild.emojis]
            self.emojis[guild.name] = dict(zip(emoji_names, guild.emojis))

    def fix(self, content, guild):
        """
        Expand any emojis for bot before sending, based on guild emojis.

        Embed emojis into the content just like on guild surrounded by ':'. Example:
            Status :Fortifying:
        """
        emojis = self.emojis[guild.name]
        for embed in list(set(re.findall(r':\S+:', content))):
            try:
                emoji = emojis[embed[1:-1]]
                content = content.replace(embed, str(emoji))
            except KeyError:
                logging.getLogger('cog.bot').warning(
                    'EMOJI: Could not find emoji %s for guild %s', embed, guild.name)

        return content


class CogBot(discord.Client):
    """
    The main bot, hooks onto on_message primarily and waits for commands.
    """
    def __init__(self, prefix, **kwargs):
        super().__init__(**kwargs)
        self.prefix = prefix
        self.deny_commands = True
        self.emoji = EmojiResolver()
        # TODO: Instead of global parser, generate based on channel rules.
        self.parser = cog.parse.make_parser(prefix)
        self.sched = cog.scheduler.Scheduler()
        self.start_date = datetime.datetime.utcnow().replace(microsecond=0)

    @property
    def uptime(self):  # pragma: no cover
        """
        Return the uptime since bot was started.
        """
        return str(datetime.datetime.utcnow().replace(microsecond=0) - self.start_date)

    def get_member_by_substr(self, name):
        """
        Given a (substring of a) member name, find the first member that has a similar name.
        Not case sensitive.

        Returns: The discord.Member object or None if nothing found.
        """
        name = name.lower()
        for member in self.get_all_members():
            if name in member.display_name.lower():
                return member

        return None

    def get_channel_by_name(self, name):
        """
        Given channel name, get the Channel object requested.
        There shouldn't be any collisions.

        Returns: The discord.Channel object or None if nothing found.
        """
        return discord.utils.get(self.get_all_channels(), name=name)

    # Events hooked by bot.
    async def on_member_join(self, member):
        """ Called when member joins guild (login). """
        log = logging.getLogger('cog.bot')
        log.info('Member has joined: %s', member.display_name)

    async def on_member_leave(self, member):
        """ Called when member leaves guild (logout). """
        log = logging.getLogger('cog.bot')
        log.info('Member has left: %s', member.display_name)

    async def on_guild_emojis_update(self, *_):
        """ Called when emojis change, just update all emojis. """
        self.emoji.update(self.guilds)

    async def on_ready(self):
        """
        Event triggered when connection established to discord and bot ready.
        """
        log = logging.getLogger('cog.bot')
        log.info('Logged in as: %s', self.user.name)
        log.info('Available on following guilds:')
        for guild in self.guilds:
            log.info('  "%s" with id %s', guild.name, guild.id)

        self.emoji.update(self.guilds)

        # This block is effectively a one time setup.
        if not cog.actions.SCANNERS:
            scanners = await cogdb.scanners.init_scanners()
            cog.actions.SCANNERS = scanners

            self.sched.register('hudson_cattle', scanners['hudson_cattle'],
                                ('Drop', 'Fort', 'User'))
            self.sched.register('hudson_undermine', scanners['hudson_undermine'],
                                ('Hold', 'UM', 'User'))
            self.sched.register('hudson_kos', scanners['hudson_kos'], ('KOS'))

            # separate to force crash if port busy, essential connection for scheduler
            await self.sched.connect_sub()
            await asyncio.sleep(0.2)

            self.sched.schedule_all(delay=1)
            asyncio.ensure_future(asyncio.gather(
                presence_task(self),
                simple_heartbeat(),
            ))

            self.deny_commands = False

        print('GBot Ready!')

    async def ignore_message(self, message):
        """
        Determine whether the message should be ignored.

        Ignore messages not directed at bot and any commands that aren't
        from an admin during deny_commands == True.
        """
        # Ignore lines not directed at bot
        if message.author.bot or not message.content.startswith(self.prefix):
            return True

        # Accept only admin commands if denying
        if self.deny_commands and not message.content.startswith('{}admin'.format(self.prefix)):
            await self.send_message(message.channel, "__Admin Mode__\n\nOnly `!admin` commands accepted.")
            return True

        if isinstance(message.channel, discord.abc.PrivateChannel):
            await self.send_message(message.channel, "Bot will not respond to private commands.")
            return True

        return False

    async def on_message_edit(self, before, after):
        """
        Only process commands that were different from before.
        """
        if before.content != after.content and after.content.startswith(self.prefix):
            before.edited_at = after.edited_at
            await self.on_message(after)

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

        # TODO: Better filtering, use a loop and filter funcs.
        if await self.ignore_message(message):
            return

        log = logging.getLogger('cog.bot')
        log.info("guild: '%s' Channel: '%s' User: '%s' | %s",
                 channel.guild, channel.name, author.name, content)

        try:
            edit_time = message.edited_at
            content = re.sub(r'<[#@]\S+>', '', content).strip()  # Strip mentions from text
            args = self.parser.parse_args(re.split(r'\s+', content))
            await self.dispatch_command(args=args, bot=self, msg=message)

        except cog.exc.ArgumentParseError as exc:
            log.exception("Failed to parse command. '%s' | %s", author.name, content)
            exc.write_log(log, content=content, author=author, channel=channel)
            if 'invalid choice' not in exc.message:
                try:
                    self.parser.parse_args(content.split(' ')[0:1] + ['--help'])
                except cog.exc.ArgumentHelpError as exc2:
                    exc.message = 'Invalid command use. Check the command help.'
                    exc.message += '\n{}\n{}'.format(len(exc.message) * '-', exc2.message)

            await self.send_ttl_message(channel, exc.reply())
            try:
                if edit_time == message.edited_at:
                    await message.delete()
            except discord.DiscordException:
                pass

        except cog.exc.UserException as exc:
            exc.write_log(log, content=content, author=author, channel=channel)

            await self.send_ttl_message(channel, exc.reply())
            try:
                if edit_time == message.edited_at:
                    await message.delete()
            except discord.DiscordException:
                pass

        except cog.exc.InternalException as exc:
            exc.write_log(log, content=content, author=author, channel=channel)
            await self.send_message(channel, exc.reply())

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
                await self.send_message(channel, "A critical discord error! {}.".format(gears))
            line = "Discord.py Library raised an exception"
            line += cog.exc.log_format(content=content, author=author, channel=channel)
            log.exception(line)

        except apiclient.errors.Error as exc:
            line = "Google Sheets API raised an exception"
            line += "\n" + str(exc) + "\n\n"
            line += cog.exc.log_format(content=content, author=author, channel=channel)
            log.exception(line)

    async def dispatch_command(self, **kwargs):
        """
        Simply inspect class and dispatch command. Guaranteed to be valid.
        """
        args, msg = kwargs.get('args'), kwargs.get('msg')

        try:
            await self.sched.wait_for(args.cmd)
            logging.getLogger('cog.bot').info('Command %s aquired lock.', msg.content)
            cogdb.query.check_perms(msg, args)
            cls = getattr(cog.actions, args.cmd)
            await cls(**kwargs).execute()
        finally:
            await self.sched.unwait_for(args.cmd)
            logging.getLogger('cog.bot').info('Command %s released lock.', msg.content)

    async def send_long_message(self, destination, content=None, *, tts=False, embed=None):
        """
        Behaves excactly like Client.send_message except it:

            Splits messages > 2k limit into smaller messages and transmits.
        """
        for part in cog.util.complete_blocks(cog.util.msg_splitter(content)):
            await self.send_message(destination, part, tts=tts, embed=embed)

    # TODO: Signature changed in library, update later.
    async def send_message(self, destination, content=None, *, tts=False, embed=None):
        """
        Behaves excactly like Client.send_message except it:

            Allow several retries before failing, raises on last exception.
            If content is too long, truncate it
        """
        log = logging.getLogger('cog.bot')
        if content and len(content) > cog.util.MSG_LIMIT:
            log.warning('Critical problem, content len close to 2000 limit. Truncating.\
                        \n    Len is %d, starts with: %s', len(content), content[:50])
            content = content[:cog.util.MSG_LIMIT] + '\n**MSG Truncated**'

        attempts = 4
        while attempts:
            try:
                return await destination.send(content, tts=tts, embed=embed)
            except discord.HTTPException:
                # Catching these due to infrequent issues with discord remote.
                await asyncio.sleep(1.5)
                attempts -= 1
                if not attempts:
                    log.exception('SND_MSG Failed to send message to user.')
                    raise

    async def send_ttl_message(self, destination, content, **kwargs):
        """
        Behaves excactly like Client.send_message except:
            After sending message wait 'ttl' seconds then delete message.

        Extra Kwargs:
            ttl: The time message lives before deletion (default 30s)
        """
        try:
            ttl = kwargs.pop('ttl')
        except KeyError:
            ttl = cog.util.get_config('ttl')

        content += '\n\n__This message will be deleted in {} seconds__'.format(ttl)
        message = await self.send_message(destination, content, **kwargs)

        await asyncio.sleep(ttl)
        try:
            await message.delete()
        except discord.NotFound:
            pass

    async def broadcast(self, content, ttl=False, channels=None, **kwargs):
        """
        By default, broadcast a normal message to all channels bot can see.

        args:
            content: The message.
            ttl: If true, send a message that deletes itself.
            channels: A list of channel names (strings) to broadcast to.
         """
        send = self.send_message
        if ttl:
            send = self.send_ttl_message

        if channels:
            channels = [self.get_channel_by_name(name) for name in channels]
        else:
            channels = list(self.get_all_channels())

        messages = []
        for channel in channels:
            if channel.permissions_for(channel.guild.me).send_messages and \
               channel.type == discord.ChannelType.text:
                messages += [send(channel, "**Broadcast**\n\n" + content, **kwargs)]

        await asyncio.gather(*messages)


async def presence_task(bot, delay=180):
    """
    Manage the ultra important task of bot's played game.
    """
    print('Presence task started')
    lines = [
        'Heating the oven',
        'Kneading the dough',
        'Chipping the chocolate',
        'Adding secret ingredients',
        'Cutting the shapes',
        'Putting trays in oven',
        'Setting the timer',
        'Baking cookies',
        'Reading "Cookies Monthly"',
        'Removing trays from oven',
        'Letting cookies cool',
        'Quality control step, yum!',
        'Sealing in freshness',
    ]
    ind = 0
    while True:
        try:
            await bot.change_presence(activity=discord.Game(name=lines[ind]))
        except websockets.exceptions.ConnectionClosed:
            pass

        ind = (ind + 1) % len(lines)
        await asyncio.sleep(delay)


async def simple_heartbeat(delay=30):
    """ Simple heartbeat function to check liveness of main loop. """
    hfile = os.path.join(tempfile.gettempdir(), 'hbeat' + os.environ.get('COG_TOKEN', 'dev'))
    print(hfile)
    while True:
        async with aiofiles.open(hfile, 'w') as fout:
            await fout.write('{} {}\n'.format(delay,
                                              datetime.datetime.utcnow().replace(microsecond=0)))
        await asyncio.sleep(delay)


def main():  # pragma: no cover
    """ Entry here! """
    cog.util.init_logging()
    cog.util.BOT = CogBot("!")

    token = cog.util.get_config('discord', os.environ.get('COG_TOKEN', 'dev'))
    print("Waiting on connection to Discord ...")
    try:
        loop = asyncio.get_event_loop()
        # BLOCKING: N.o. e.s.c.a.p.e.
        loop.run_until_complete(cog.util.BOT.start(token))
    except KeyboardInterrupt:
        loop.run_until_complete(cog.util.BOT.logout())
    finally:
        loop.close()


if __name__ == "__main__":  # pragma: no cover
    main()
