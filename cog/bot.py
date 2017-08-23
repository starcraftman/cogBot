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
import datetime
import functools
import logging
import logging.handlers
import logging.config
import os
import pprint
import re
import time

import discord
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.get_event_loop().set_debug(True)
    print('Setting uvloop as asyncio loop, enabling debug')
except ImportError:
    pass
import zmq
import zmq.asyncio

import cogdb
import cogdb.schema
import cogdb.query
import cog.actions
import cog.exc
import cog.share
import cog.sheets
import cog.tbl

zmq.asyncio.install()
CTX = zmq.asyncio.Context.instance()


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


# FIXME: Register hook on db object that callsback.
class CogBot(discord.Client):
    """
    The main bot, hooks onto on_message primarily and waits for commands.
    """
    def __init__(self, **kwargs):
        super(CogBot, self).__init__(**kwargs)
        self.prefix = kwargs.get('prefix')
        self.scanner = kwargs.get('scanner')
        self.scanner_um = kwargs.get('scanner_um')
        self.sched = kwargs.get('sched')
        self.deny_commands = True
        self.start_date = datetime.datetime.utcnow().replace(microsecond=0)
        self.emoji = EmojiResolver()

    @property
    def uptime(self):  # pragma: no cover
        """
        Return the uptime since bot was started.
        """
        return str(datetime.datetime.utcnow().replace(microsecond=0) - self.start_date)

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

        print('GBot Ready!')
        self.deny_commands = False

    def ignore_message(self, message):
        """
        Determine whether the message should be ignored.

        Ignore messages not directed at bot and any commands that aren't
        from an admin during deny_commands == True.
        """
        ignore = False

        # Ignore lines not directed at bot
        if message.author.bot or not message.content.startswith(self.prefix):
            ignore = True

        # Accept only admin commands if denying
        if self.deny_commands and not message.content.startswith('{}admin'.format(self.prefix)):
            ignore = True

        return ignore

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
        msg = message.content
        author = message.author
        channel = message.channel
        response = ''

        if self.ignore_message(message):
            return

        log = logging.getLogger('cog.bot')
        log.info("Server: '%s' Channel: '%s' User: '%s' | %s",
                 channel.server, channel.name, author.name, msg)

        try:
            msg = re.sub(r'<@\w+>', '', msg).strip()  # Strip mentions from text
            parser = cog.share.make_parser(self.prefix)
            args = parser.parse_args(msg.split(' '))
            await self.dispatch_command(args=args, bot=self, message=message)

        except (cog.exc.NoMatch, cog.exc.MoreThanOneMatch) as exc:
            log.error("Loose cmd failed to match excatly one. '%s' | %s", author.name, msg)
            log.error(exc)
            response = 'Check command arguments ...\n' + str(exc)
            await self.send_ttl_message(channel, response)
            asyncio.ensure_future(self.delete_message(message))

        except cog.exc.NameCollisionError as exc:
            log.error('Cmdr Name Collision\n' + str(exc))
            asyncio.ensure_future(self.send_message(channel, str(exc)))

        except cog.exc.ArgumentParseError as exc:
            log.exception("Failed to parse command. '%s' | %s", author.name, msg)
            if 'invalid choice' in exc.message:
                response = exc.message
            else:  # Valid subcommand, bad usage. Show subcommand help.
                try:
                    parser.parse_args(msg.split(' ')[0:1] + ['--help'])
                except cog.exc.ArgumentHelpError as exc:
                    response = 'Invalid command/arguments. See help below.'
                    response += '\n{}\n{}'.format(len(response) * '-', exc.message)
            await self.send_ttl_message(channel, response)
            asyncio.ensure_future(self.delete_message(message))

        except cog.exc.ArgumentHelpError as exc:
            log.info("User requested help. '%s' | %s", author.name, msg)
            await self.send_ttl_message(channel, exc.message)
            asyncio.ensure_future(self.delete_message(message))

        except cog.exc.InvalidCommandArgs as exc:
            log.info('Invalid combination of arguments or values. %s | %s', author.name, msg)
            await self.send_ttl_message(channel, str(exc))
            asyncio.ensure_future(self.delete_message(message))

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
            ttl = cog.share.get_config('ttl')

        content += '\n__This message will be deleted in {} seconds__'.format(ttl)
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
        message = kwargs.get('message')

        if self.sched.disabled(args.cmd):
            reply = 'Change detected in spreadsheet, bot will synchronize shortly.\n'\
                    'Execution will resume after update has finished.'
            await self.send_message(message.channel, reply)

            while self.sched.disabled(args.cmd):
                await asyncio.sleep(1)

            await self.send_message(message.channel, 'Resuming command from {}: **{}**'.format(
                message.author.display_name, message.content))


        cls = getattr(cog.actions, args.cmd.capitalize())
        await cls(**kwargs).execute()


async def delay_call(delay, func, *args, **kwargs):
    """ Simply delay then invoke func. """
    log = logging.getLogger('cog.bot')
    await asyncio.sleep(delay)
    func(*args, **kwargs)
    log.info('SCHED - Finished delayed call.')


def done_callback(wrap, _):
    """ Callback called when future done. """
    log = logging.getLogger('cog.bot')
    log.info('SCHED - Finished %s, expected at %s, actual %s',
             wrap.name, wrap.expected, datetime.datetime.utcnow())
    wrap.future = None
    wrap.expected = None


class UpdateScheduler(object):
    """
    Schedule updates for the db and manage permitted commands.
    """
    def __init__(self):
        self.bot = None
        self.delay = 20  # Seconds of timeout before updating
        self.scanners = {}

    def register(self, name, scanner, cmds):
        """
        Register scanner to be updated.
        """
        self.scanners[name] = WrapScanner(name, scanner, cmds)

    def schedule(self, data):
        """ Data is a json payload, format still not determined. """
        wrap = self.scanners.get(data['scanner'])
        wrap.schedule(self.delay)

    def disabled(self, cmd):
        """ Check if a command is disabled due to scheduled update. """
        scheduled = [scanner for scanner in self.scanners.values() if scanner.is_scheduled]
        for scanner in scheduled:
            if cmd in scanner.cmds:
                return True

        return False


class WrapScanner(object):
    """
    Wrap a scanner with info about scheduling. Mainly a data class.
    """
    def __init__(self, name, scanner, cmds):
        self.name = name
        self.cmds = cmds
        self.scanner = scanner
        self.future = None
        self.expected = None

    def __str__(self):
        return 'Wrapper for: {} {}'.format(self.name, self.scanner.__class__.__name__)

    def schedule(self, delay):
        """ Schedule this scanner update after delay seconds. """
        log = logging.getLogger('cog.bot')
        if self.is_scheduled:
            self.future.cancel()
        self.future = asyncio.ensure_future(
            delay_call(delay, self.scanner.scan, cogdb.Session()))
        self.future.add_done_callback(functools.partial(done_callback, self))
        self.expected = datetime.datetime.utcnow() + datetime.timedelta(seconds=delay)
        log.info('SCHED - Update for %s scheduled for %s', self.name, self.expected)

    @property
    def is_scheduled(self):
        """ Simple naming alias, a wrapper is scheduled if the future is set. """
        return self.future


async def bot_updater(client, updater):
    """
    Background task executes while bot alive. On sheet update, receive notification via zmq socket
    do an admin scan for the altered sheet.

    Args:
        client: The bot itself.
        server: Name of a server bot connects to.
        channel: Name of channel on server bot connects to that messages should be sent to.
    """
    await client.wait_until_ready()
    log = logging.getLogger('cog.bot')
    print('Starting update system')
    ctx = zmq.asyncio.Context.instance()
    sock = ctx.socket(zmq.SUB)
    sock.bind('tcp://127.0.0.1:9000')
    sock.subscribe(b'')

    count = 0
    while not client.is_closed:
        data = await sock.recv_json()
        log.debug('POST %d received: %s', count, str(data))

        if data['filter'] == cog.share.get_config('filter'):
            updater.schedule(data)

        count = (count + 1) % 100


def scan_sheet(sheet, cls):
    """
    Run common database initialization.

    - Fetch the current sheet and parse it.
    - Fill database with parsed data.
    """
    session = cogdb.Session()

    paths = cog.share.get_config('paths')
    sheet = cog.sheets.GSheet(sheet,
                              cog.share.rel_to_abs(paths['json']),
                              cog.share.rel_to_abs(paths['token']))

    scanner = cls(sheet)
    scanner.scan(session)

    return scanner


def main():  # pragma: no cover
    """ Entry here!  """
    cog.share.init_logging()
    try:
        cogdb.schema.drop_tables(all=False)  # Only persistent data should be kept on startup
        scanner = scan_sheet(cog.share.get_config('hudson', 'cattle'), cogdb.query.FortScanner)
        scanner_um = scan_sheet(cog.share.get_config('hudson', 'um'), cogdb.query.UMScanner)
        sched = UpdateScheduler()
        sched.register('hudson_cattle', scanner, ['drop', 'fort', 'user'])
        sched.register('hudson_um', scanner_um, ['hold', 'um', 'user'])
        bot = CogBot(prefix='!', sched=sched, scanner=scanner, scanner_um=scanner_um)
        sched.bot = bot
        bot.loop.create_task(bot_updater(bot, sched))

        # BLOCKING: N.o. e.s.c.a.p.e.
        bot.run(cog.share.get_config('discord', os.environ.get('COG_TOKEN', 'dev')))
    finally:
        try:
            bot.logout()
        except UnboundLocalError:
            pass


if __name__ == "__main__":  # pragma: no cover
    main()
