#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import logging
import logging.handlers
import os
import sys
import tempfile

import discord
import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

import share
import fort
from tbl import wrap_markdown

# TODO: discord.ext.bot -> wrapper
# TODO: Secure to channel/server/users
# TODO: Add basic IFF
# TODO: Generate standard test csv file
# TODO: Download csv as background task every 2 minutes

def init_logging():
    """
    Initialize project wide logging.
      - 'discord' logger is used by the discord.py framework.
      - 'gbot' logger will be used to log anything in this project.

    Both loggers will:
      - Send all messsages >= WARN to STDERR.
      - Send all messages >= INFO to rotating file log in /tmp.

    IMPORTANT: On every start of gbot, the logs are rolled over. 5 runs kept max.
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

    handler = logging.StreamHandler(sys.stderr);
    handler.setLevel(logging.WARNING)
    handler.setFormatter(fmt)
    d_logger.addHandler(handler)
    g_logger.addHandler(handler)

# Simple background coroutine example for later, adapt to refresh csv.
# async def my_task():
    # await client.wait_until_ready()
    # counter = 0
    # while not client.is_closed:
        # counter += 1
        # print('Counter', counter)
        # await asyncio.sleep(5)

client = discord.Client()
# client.loop.create_task(my_task())
init_logging()

@client.event
async def on_ready():
    log = logging.getLogger('gbot')
    log.info('Logged in as: '+ client.user.name)
    log.info('Available on following servers:')
    for server in client.servers:
        log.info('  "{}" with id {}'.format(server.name, server.id))
    print('GBot Ready!')

@client.event
async def on_message(message):
    """
    Notes:
        message.author - Returns member object
            message.author.roles -> List of Role objects. First always @everyone.
                message.author.roles[0].name -> String name of role.
        message.channel - Channel object.
            message.channel.name -> Name of channel.
            message.channel.server -> server of channel.
        message.content - The text
    """
    # Ignore all bots.
    if message.author.bot or not message.content.startswith('!'):
        return

    log = logging.getLogger('gbot')
    log.info('Server: {}, Channel: {}, User: {} | {}'.format(message.channel.server,
            message.channel.name, message.author.name, message.content))

    # FIXME: Move all commands into argsparser.
    if message.content.startswith('!fort'):
        message.content = message.content[1:]
        parser = share.make_parser()
        args = parser.parse_args(message.content.split(' '))
        msg = args.func(args)

    elif message.content.startswith('!info'):
        roles = ', '.join([role.name for role in message.author.roles[1:]])
        msg = 'Author: {aut} has Roles: {rol}'.format(aut=message.author.name, rol=roles)
        msg += '\nSent from channel [{ch}] on server [{se}]'.format(ch=message.channel.name,
                                                                se=message.channel.server)

    elif message.content.startswith('!mirror'):
        msg = '{mention}: {msg}'.format(mention=message.author.mention,
                                        msg=message.content.replace('!mirror ', ''))

    elif message.content.startswith('!help'):
        msg = '\n'.join([
                'Available commands:',
               '!fort - Show current fort targets.',
               '!fort next - Show the next 5 targets.',
               '!fort next long - Show status of the next 5 targets.',
               '!info - Dump user data.',
               '!mirror - Repeat what you write to you.',
               ])
    else:
        msg = 'Did not understand: {}'.format(message.content)
        msg += '\nGet more info with: !help'

    await client.send_message(message.channel, msg)

def main():
    try:
        client.run(share.get_config('token'))
    finally:
        client.close()

if __name__ == "__main__":
    main()
