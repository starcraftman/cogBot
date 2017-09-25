#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Discord bot class

API:
    https://discordpy.readthedocs.io/en/latest/api.html

Small Python Async tutorial:
    https://snarky.ca/how-the-heck-does-async-await-work-in-python-3-5/
"""
from __future__ import absolute_import, print_function
import asyncio
import datetime as date
import logging
import logging.handlers
import logging.config
import os
import pprint
import re
import time

import apiclient
import discord
import websockets.exceptions
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.get_event_loop().set_debug(True)
    print('Setting uvloop as asyncio loop, enabling debug')
except ImportError:
    pass

import cog.actions
import cog.exc
import cog.jobs
import cog.parse
import cog.util
import cog.sheets
import cogdb.query


async def bot_ready(bot, msg, wait=120):
    """ Simple busy wait for bot to be ready. """
    if not bot.deny_commands:
        return True

    count = 0
    while count != wait and bot.deny_commands:
        await asyncio.sleep(1)
        count += 1

    if bot.deny_commands:
        await bot.send_message(msg.channel, "Bot could not handle your request at this time. {}".format(msg.author.mention))
        return False

    return True


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
        self.last_cmd = time.time()
        self.start_date = date.datetime.utcnow().replace(microsecond=0)
        self.emoji = EmojiResolver()

    @property
    def uptime(self):  # pragma: no cover
        """
        Return the uptime since bot was started.
        """
        return str(date.datetime.utcnow().replace(microsecond=0) - self.start_date)

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

        if not cog.actions.SCANNERS:
            asyncio.ensure_future(asyncio.gather(
                presence_task(self),
                cog.jobs.pool_starter(self.send_message),
            ))

            # TODO: Parallelize startup with scheduler and jobs.
            for name in cog.util.get_config("scanners"):  # Populate on import
                cog.actions.init_scanner(name)

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
            content = re.sub(r'<@\w+>', '', content).strip()  # Strip mentions from text
            parser = cog.parse.make_parser(self.prefix)
            args = parser.parse_args(re.split(r'\s+', content))
            await self.dispatch_command(args=args, bot=self, msg=message)

        except cog.exc.ArgumentParseError as exc:
            log.exception("Failed to parse command. '%s' | %s", author.name, content)
            exc.write_log(log, content=content, author=author, channel=channel)
            if 'invalid choice' not in exc.message:
                try:
                    parser.parse_args(content.split(' ')[0:1] + ['--help'])
                except cog.exc.ArgumentHelpError as exc2:
                    exc.message = 'Invalid command use. Check the command help.'
                    exc.message += '\n{}\n{}'.format(len(exc.message) * '-', exc2.message)
            await self.send_ttl_message(channel, exc.reply())
            asyncio.ensure_future(self.delete_message(message))

        except cog.exc.UserException as exc:
            exc.write_log(log, content=content, author=author, channel=channel)
            await self.send_ttl_message(channel, exc.reply())
            asyncio.ensure_future(self.delete_message(message))

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

    async def dispatch_command(self, **kwargs):
        """
        Simply inspect class and dispatch command. Guaranteed to be valid.
        """
        args = kwargs.get('args')
        msg = kwargs.get('msg')

        # FIXME: Hack due to users editting sheet.
        outdated = (time.time() - self.last_cmd) > 300.0
        if args.cmd in cog.actions.SHEET_ACTS and outdated:
            self.last_cmd = time.time()

            orig_content = msg.content
            asyncio.ensure_future(self.send_message(
                msg.channel,
                'Bot has been inactive 5+ mins. Command will execute after update.'))
            msg.content = '!admin scan'
            await self.on_message(msg)

            if not await bot_ready(self, msg):
                return

            await self.send_message(msg.channel,
                                    'Now executing: **{}**'.format(orig_content))

            msg.content = orig_content

        cogdb.query.check_perms(msg, args)
        cls = getattr(cog.actions, args.cmd)
        await cls(**kwargs).execute()

    async def broadcast(self, content, ttl=False, **kwargs):
        """
        Broadcast content to ALL channels this bot has send message permissions in.
        """
        send = self.send_message
        if ttl:
            send = self.send_ttl_message

        for server in self.servers:
            for channel in server.channels:
                if channel.permissions_for(server.me).send_messages:
                    asyncio.ensure_future(send(channel, '**BROADCAST** ' + content, **kwargs))


async def presence_task(bot, delay=180):
    """
    Manage the ultra important task of bot's played game.
    """
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
