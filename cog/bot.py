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

pyzmq: Python bindings for zmq. (import is named zmq)
    http://pyzmq.readthedocs.io/en/latest/

ZeroMQ: Listed mainly as a reference for core concepts.
    http://zguide.zeromq.org/py:all
"""
from __future__ import absolute_import, print_function
import asyncio
import datetime
import logging
import os
import pprint
import re

import apiclient
import discord
import websockets.exceptions
try:
    # TODO: Cannot use uvloop + pyzmq. Investigate later uvloop + aiozmq
    #       Not a high priority problem, loop rarely bottleneck.
    import zmq.asyncio
    zmq.asyncio.install()
    asyncio.get_event_loop().set_debug(True)
except ImportError:
    print("Missing Core Lib: pyzmq\n    Run: python ./setup.py deps")
    import sys
    sys.exit(1)
finally:
    print("Default event loop:", asyncio.get_event_loop())

import cog.actions
import cog.exc
import cog.inara
import cog.jobs
import cog.parse
import cog.scheduler
import cog.sheets
import cog.util
import cogdb.query


class EmojiResolver(object):
    """
    Map emoji embeds onto the text required to make them appear.
    """
    def __init__(self):
        # For each server, store a dict of emojis on that server
        self.emojis = {}

    def __str__(self):
        """ Just dump the emoji db. """
        return pprint.pformat(self.emojis, indent=2)

    def update(self, servers):
        """
        Update the emoji dictionary. Call this in on_ready.
        """
        for server in servers:
            emoji_names = [emoji.name for emoji in server.emojis]
            self.emojis[server.name] = dict(zip(emoji_names, server.emojis))

    def fix(self, content, server):
        """
        Expand any emojis for bot before sending, based on server emojis.

        Embed emojis into the content just like on server surrounded by ':'. Example:
            Status :Fortifying:
        """
        emojis = self.emojis[server.name]
        for embed in list(set(re.findall(r':\S+:', content))):
            try:
                emoji = emojis[embed[1:-1]]
                content = content.replace(embed, str(emoji))
            except KeyError:
                logging.getLogger('cog.bot').warning(
                    'EMOJI: Could not find emoji %s for server %s', embed, server.name)

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
        """ Called when member joins server (login). """
        log = logging.getLogger('cog.bot')
        log.info('Member has joined: ' + member.display_name)

    async def on_member_leave(self, member):
        """ Called when member leaves server (logout). """
        log = logging.getLogger('cog.bot')
        log.info('Member has left: ' + member.display_name)

    async def on_server_emojis_update(self, *_):
        """ Called when emojis change, just update all emojis. """
        self.emoji.update(self.servers)

    async def on_ready(self):
        """
        Event triggered when connection established to discord and bot ready.
        """
        log = logging.getLogger('cog.bot')
        log.info('Logged in as: %s', self.user.name)
        log.info('Available on following servers:')
        for server in self.servers:
            log.info('  "%s" with id %s', server.name, server.id)

        self.emoji.update(self.servers)

        # This block is effectively a one time setup.
        if not cog.actions.SCANNERS:
            cog.inara.api.bot = self

            # TODO: Parallelize startup with scheduler and jobs.
            for name in cog.util.get_config("scanners"):  # Populate on import
                cog.actions.init_scanner(name)

            cog.scheduler.BOT = self
            self.sched.register('hudson_cattle', cog.actions.get_scanner('hudson_cattle'),
                                ['Drop', 'Fort', 'User'])
            self.sched.register('hudson_undermine', cog.actions.get_scanner('hudson_undermine'),
                                ['Hold', 'UM', 'User'])

            asyncio.ensure_future(asyncio.gather(
                presence_task(self),
                cog.jobs.pool_monitor_task(),
                cog.scheduler.scheduler_task(self, self.sched),
            ))
            await asyncio.sleep(0.5)

            self.deny_commands = False

        print('GBot Ready!')

    def ignore_message(self, message):
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
            return True

        return False

    async def on_message_edit(self, before, after):
        """
        Only process commands that were different from before.
        """
        if before.content != after.content and after.content.startswith(self.prefix):
            await self.on_message(after)

    async def on_message(self, message):
        """
        Intercepts every message sent to server!

        Notes:
            message.author - Returns member object
                roles -> List of Role objects. First always @everyone.
                    roles[0].name -> String name of role.
            message.channel - Channel object.
                name -> Name of channel
                server -> Server of channel
                    members -> Iterable of all members
                    channels -> Iterable of all channels
                    get_member_by_name -> Search for user by nick
            message.content - The text
        """
        content = message.content
        author = message.author
        channel = message.channel

        # TODO: Better filtering, use a loop and filter funcs.
        if self.ignore_message(message):
            return

        log = logging.getLogger('cog.bot')
        log.info("Server: '%s' Channel: '%s' User: '%s' | %s",
                 channel.server, channel.name, author.name, content)

        try:
            content = re.sub(r'<[#@]\w+>', '', content).strip()  # Strip mentions from text
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
                await self.delete_message(message)
            except discord.DiscordException:
                pass

        except cog.exc.UserException as exc:
            exc.write_log(log, content=content, author=author, channel=channel)
            await self.send_ttl_message(channel, exc.reply())
            try:
                await self.delete_message(message)
            except discord.DiscordException:
                pass

        except cog.exc.InternalException as exc:
            exc.write_log(log, content=content, author=author, channel=channel)
            await self.send_message(channel, exc.reply())

        except discord.DiscordException as exc:
            line = "Discord.py Library raised an exception"
            line += cog.exc.log_format(content=content, author=author, channel=channel)
            log.exception(line)

        except apiclient.errors.Error as exc:
            line = "Google Sheets API raised an exception"
            line += cog.exc.log_format(content=content, author=author, channel=channel)
            log.exception(line)

    async def dispatch_command(self, **kwargs):
        """
        Simply inspect class and dispatch command. Guaranteed to be valid.
        """
        args = kwargs.get('args')
        msg = kwargs.get('msg')

        if self.sched.disabled(args.cmd):
            reply = 'Synchronizing sheet changes.\n\n'\
                    'Your command will resume after update has finished.'
            await self.send_message(msg.channel, reply)

            # TODO: Give scheduler a asyncio.Lock to block on.
            while self.sched.disabled(args.cmd):
                await asyncio.sleep(2)

            await self.send_message(msg.channel, '{} Resuming your command: **{}**'.format(
                msg.author.mention, msg.content))

        cogdb.query.check_perms(msg, args)
        cls = getattr(cog.actions, args.cmd)
        await cls(**kwargs).execute()

    async def send_message(self, destination, content=None, *, tts=False, embed=None):
        """
        Behaves excactly like Client.send_message except:

            Allow several retries before failing silently.
            Exceptions raised usually temporary HTTP issues.
        """
        log = logging.getLogger('cog.bot')
        if len(content) > 1975:
            log.warning('Critical problem, content len close to 2000 limit. Truncating.\
                        \n    Len is {}, starts with: {}'.format(len(content), content[:50]))
            content = content[:1975]

        attempts = 4
        while attempts:
            try:
                await super().send_message(destination, content, tts=tts, embed=embed)
                attempts = None
            except discord.DiscordException:
                await asyncio.sleep(1.5)
                attempts -= 1
                if not attempts:
                    log.exception('Failed to send message to user.')

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
            await self.delete_message(message)
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
            if channel.permissions_for(channel.server.me).send_messages and \
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
            await bot.change_presence(game=discord.Game(name=lines[ind]))
        except websockets.exceptions.ConnectionClosed:
            pass

        ind = (ind + 1) % len(lines)
        await asyncio.sleep(delay)


def main():  # pragma: no cover
    """ Entry here! """
    try:
        cog.util.init_logging()
        bot = CogBot("!")
        # BLOCKING: N.o. e.s.c.a.p.e.
        bot.run(cog.util.get_config('discord', os.environ.get('COG_TOKEN', 'dev')))
    finally:
        try:
            bot.logout()
        except UnboundLocalError:
            pass


if __name__ == "__main__":  # pragma: no cover
    main()
