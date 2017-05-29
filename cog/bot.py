#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Discord bot class
"""
from __future__ import absolute_import, print_function

import discord
import logging

import cog.share


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
        log.info('  "{}" with id {}'.format(server.name, server.id))
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
    author = message.author
    channel = message.channel.name
    server = message.channel.server
    msg = message.content
    cont = message.content
    # Ignore lines not directed at bot
    if author.bot or not msg.startswith('!'):
        return

    log = logging.getLogger('gbot')
    log.info("Server: '{}' Channel: '{}' User: '{}' | {}".format(server,
            channel, author.name, msg))

    if message.content.startswith('!info'):
        roles = ', '.join([role.name for role in message.author.roles[1:]])
        msg = 'Author: {aut} has Roles: {rol}'.format(aut=message.author.name, rol=roles)
        msg += '\nSent from channel [{ch}] on server [{se}]'.format(ch=message.channel.name,
                                                                se=message.channel.server)
    else:
        try:
            message.content = message.content[1:]
            parser = cog.share.make_parser()
            args = parser.parse_args(message.content.split(' '))
            msg = args.func(args)
        except cog.share.ArgumentParseError:
            msg = 'Did not understand: {}'.format(message.content)
            msg += '\nGet more info with: !help'

    await client.send_message(message.channel, msg)


def main():
    cog.share.get_db_session()
    cog.share.init_logging()
    try:
        client.run(cog.share.get_config('secrets', 'discord_token'))
    finally:
        client.close()


if __name__ == "__main__":
    main()
