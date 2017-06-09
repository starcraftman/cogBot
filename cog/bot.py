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
import logging

import discord

import cog.share


# TODO: investigate use of discord.ext.bot || make own bot class
# TODO: Secure commands against servers/channels/users
# TODO: Allow management commands to add/remove above
# TODO: Add basic whois support, default lookup in local to channel db
# TODO: Add wider search, across server, inara and other sources
# TODO: Possible image export: http://effbot.org/imagingbook/imagefont.htm
# TODO: Perhaps implement a simple LFG system to track people looking to wing.


class CogBot(discord.Client):
    """
    The main bot, hooks onto on_message primarily and waits for commands.
    """
    def __init__(self, prefix, **kwargs):
        self.prefix = prefix
        super(CogBot, self).__init__(**kwargs)

    async def on_member_join(self, member):
        log = logging.getLogger('cog.bot')
        log.info('Member has joined: ' + member.display_name)

    async def on_member_leave(self, member):
        log = logging.getLogger('cog.bot')
        log.info('Member has left: ' + member.display_name)

    async def on_ready(self):
        """
        Event triggered when connection established to discord and bot ready.
        """
        log = logging.getLogger('cog.bot')
        log.info('Logged in as: %s', self.user.name)
        log.info('Available on following servers:')
        for server in self.servers:
            log.info('  "%s" with id %s', server.name, server.id)
        print('GBot Ready!')

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

        # Ignore lines not directed at bot
        if author.bot or not msg.startswith(self.prefix):
            return

        log = logging.getLogger('cog.bot')
        log.info("Server: '%s' Channel: '%s' User: '%s' | %s",
                 channel.server, channel.name, author.name, msg)

        try:
            cog.share.get_or_create_duser(author)
            msg = msg.replace(self.prefix, '')
            parser = cog.share.make_parser()
            args = parser.parse_args(msg.split(' '))
            response = args.func(msg=message, args=args)
        except cog.share.ArgumentParseError:
            log.exception("Failed to parse command. '%s' | %s", author.name, msg)
            response = 'Could not parse command: ' + msg
            response += '\nPlease check how I work: {}help'.format(self.prefix)
        except (cog.exc.NoMatch, cog.exc.MoreThanOneMatch) as exc:
            log.error("Loose cmd failed to match excatly one. '%s' | %s", author.name, msg)
            log.error(exc)
            response = 'Check command arguments ...\n'
            response += str(exc)

        log.info("Responding to %s with %s.", author.name, response)
        await self.send_message(channel, response)


def main():
    cog.share.init_logging()
    cog.share.init_db(cog.share.get_config('hudson', 'cattle', 'id'))
    try:
        bot = CogBot('!')
        bot.run(cog.share.get_config('secrets', 'discord_token'))
    finally:
        bot.close()


if __name__ == "__main__":
    main()
