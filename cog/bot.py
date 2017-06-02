#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Discord bot class
"""
from __future__ import absolute_import, print_function

import logging
import discord

import cogdb.query
import cog.share


client = discord.Client()


@client.event
async def on_ready():
    """
    Event triggered when connection established to discord and bot ready.
    """
    logging.getLogger('cog.bot').info('Logged in as: %s', client.user.name)
    logging.getLogger('cog.bot').info('Available on following servers:')
    for server in client.servers:
        logging.getLogger('cog.bot').info('  "%s" with id %s', server.name, server.id)
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
    msg = ''
    # Ignore lines not directed at bot
    if author.bot or not message.content.startswith('!'):
        return

    logging.getLogger('cog.bot').info("Server: '%s' Channel: '%s' User: '%s' | %s",
                                      server, channel, author.name, msg)

    if message.content.startswith('!info'):
        roles = ', '.join([role.name for role in message.author.roles[1:]])
        msg = 'Author: {aut} has Roles: {rol}'.format(aut=message.author.name, rol=roles)
        msg += '\nSent from channel [{ch}] on server [{se}]'.format(ch=message.channel.name,
                                                                    se=message.channel.server)
    else:
        try:
            message.content = message.content[1:]

            sheet_id = cog.share.get_config('hudson', 'cattle', 'id')
            secrets = cog.share.get_config('secrets', 'sheets')
            sheet = cog.sheets.GSheet(sheet_id, cog.share.rel_to_abs(secrets['json']),
                                      cog.share.rel_to_abs(secrets['token']))
            parser = cog.share.make_parser(cogdb.query.FortTable(sheet))
            args = parser.parse_args(message.content.split(' '))
            msg = args.func(args)
        except cog.share.ArgumentParseError:
            logging.getLogger('cog.bot').error("Command failed from '%s' | %s", author.name, msg)
            msg = 'Did not understand: {}'.format(message.content)
            msg += '\nGet more info with: !help'

    await client.send_message(message.channel, msg)


def main():
    cog.share.init_logging()
    cogdb.query.init_db()
    try:
        client.run(cog.share.get_config('secrets', 'discord_token'))
    finally:
        client.close()


if __name__ == "__main__":
    main()
