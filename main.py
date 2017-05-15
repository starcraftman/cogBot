#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stub version of bot, doesn't connect to discord.
Instead parses CLI to test commands/outputs locally.
"""
from __future__ import absolute_import, print_function
import logging
import logging.handlers
import os
import sys
import tempfile

import argparse
import discord

import share


# TODO: investigate use of discord.ext.bot || make own bot class
# TODO: Investigate possible database, RethinkDB
# TODO: Secure commands against servers/channels/users
# TODO: Allow management commands to add/remove above
# TODO: Add basic whois support, default lookup in local to channel db
# TODO: Add wider search, across server, inara and other sources
# TODO: Add test cases for all of fort, use test Google Sheet
# TODO: No longer needed? Download csv as background task every 2 minutes

# Simple background coroutine example for later, adapt to refresh csv.
# async def my_task():
    # await client.wait_until_ready()
    # counter = 0
    # while not client.is_closed:
        # counter += 1
        # print('Counter', counter)
        # await asyncio.sleep(5)
# client.loop.create_task(my_task())


def init_logging():
    """
    Initialize project wide logging.
      - 'discord' logger is used by the discord.py framework.
      - 'gbot' logger will be used to log anything in this project.

    Both loggers will:
      - Send all messsages >= WARN to STDERR.
      - Send all messages >= INFO to rotating file log in /tmp.

    IMPORTANT: On every start the logs are rolled over. 5 runs kept max.
    """
    log_folder = os.path.join(tempfile.gettempdir(), 'gbot')
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
    discord_file = os.path.join(log_folder, 'discordpy.log')
    gbot_file = os.path.join(log_folder, 'gbot.log')
    print('discord.py log ' + discord_file)
    print('gbot log: ' + gbot_file)
    fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(msg)s')

    d_logger = logging.getLogger('discord')
    d_logger.setLevel(logging.INFO)
    handler = logging.handlers.RotatingFileHandler(discord_file,
                                                   backupCount=5, encoding='utf-8')
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(fmt)
    handler.doRollover()
    d_logger.addHandler(handler)

    g_logger = logging.getLogger('gbot')
    g_logger.setLevel(logging.INFO)
    handler = logging.handlers.RotatingFileHandler(gbot_file,
                                                   backupCount=5, encoding='utf-8')
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(fmt)
    handler.doRollover()
    g_logger.addHandler(handler)

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.WARNING)
    handler.setFormatter(fmt)
    d_logger.addHandler(handler)
    g_logger.addHandler(handler)


# FIXME: Clean this up, likely make a bot class.
def discord_bot():
    """
    Run the discord bot.
    """
    client = discord.Client()

    @client.event
    async def on_ready():
        """
        Event triggered when connection established to discord and bot ready.
        """
        log = logging.getLogger('gbot')
        log.info('Logged in as: '+ client.user.name)
        log.info('Available on following servers:')
        for server in client.servers:
            log.info('  "%s" with id %s', server.name, server.id)
        print('GBot Ready!')


    @client.event
    async def on_message(message):
        """
        Intercepts every message sent to server!

        Layout of message:
            message.author - Returns member object
                message.author.roles -> List of Role objects. First always @everyone.
                    message.author.roles[0].name -> String name of role.
            message.channel - Channel object.
                message.channel.name -> Name of channel.
                message.channel.server -> server of channel.
            message.content - The text
        """
        cont = message.content
        # Ignore lines not directed at bot
        if message.author.bot or not message.content.startswith('!'):
            return

        log = logging.getLogger('gbot')
        log.info('Server: {}, Channel: {}, User: {} | {}'.format(message.channel.server,
                message.channel.name, message.author.name, message.content))

        if message.content.startswith('!fort') or message.content.starswith('!help'):
            try:
                message.content = message.content[1:]
                parser = share.make_parser()
                args = parser.parse_args(message.content.split(' '))
                msg = args.func(args)
            except share.ArgumentParseError:
                msg = 'Bad command, see !help.'

        elif message.content.startswith('!info'):
            roles = ', '.join([role.name for role in message.author.roles[1:]])
            msg = 'Author: {aut} has Roles: {rol}'.format(aut=message.author.name, rol=roles)
            msg += '\nSent from channel [{ch}] on server [{se}]'.format(ch=message.channel.name,
                                                                    se=message.channel.server)

        else:
            msg = 'Did not understand: {}'.format(message.content)
            msg += '\nGet more info with: !help'

        await client.send_message(message.channel, msg)

    try:
        client.run(share.get_config('secrets', 'discord_token'))
    finally:
        client.close()


def local_bot():
    try:
        parser = share.make_parser()
        while True:
            try:
                line = sys.stdin.readline().rstrip()
                args = parser.parse_args(line.split(' '))
                msg = args.func(args)
                if msg:
                    print(msg.replace('```', ''))
            except share.ArgumentParseError:
                print('Invalid command:', line)
    except KeyboardInterrupt:
        print('\nTerminating loop. Thanks for testing.')


def main():
    init_logging()

    parser = argparse.ArgumentParser(prog='cog', description='a simple discord bot')
    parser.add_argument('-l', '--local', action='store_true', default=False,
                        help='run the bot locally without connecting to discord')
    args = parser.parse_args()

    if args.local:
        local_bot()
    else:
        discord_bot()


if __name__ == "__main__":
    main()
