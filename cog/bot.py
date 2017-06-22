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
import datetime as date
import functools
import logging
import logging.handlers
import logging.config

import discord

import cogdb
import cogdb.schema
import cogdb.query
import cog.exc
import cog.share
import cog.sheets
import cog.tbl


class CogBot(discord.Client):
    """
    The main bot, hooks onto on_message primarily and waits for commands.
    """
    def __init__(self, prefix, **kwargs):
        super(CogBot, self).__init__(**kwargs)
        self.prefix = prefix
        self.callback_add_fort = None
        self.callback_add_user = None

    def set_callbacks(self, callbacks):
        self.callback_add_user, self.callback_add_fort = callbacks

    # Events hooked by bot.
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
            cogdb.query.get_or_create_duser(author)
            parser = cog.share.make_parser(self.prefix)
            args = parser.parse_args(msg.split(' '))
            await self.dispatch_command(message=message, args=args, session=cogdb.Session())
        except (cog.exc.NoMatch, cog.exc.MoreThanOneMatch) as exc:
            log.error("Loose cmd failed to match excatly one. '%s' | %s", author.name, msg)
            log.error(exc)
            response = 'Check command arguments ...\n' + str(exc)
            await self.send_message(channel, response)
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
            await self.send_message(channel, response)
        except cog.exc.ArgumentHelpError as exc:
            log.info("User requested help. '%s' | %s", author.name, msg)
            await self.send_message(channel, exc.message)

    # Commands beyond here
    async def dispatch_command(self, **kwargs):
        """
        Simply inspect class and dispatch command. Guaranteed to be valid.
        """
        await getattr(self, 'command_' + kwargs.get('args').cmd)(**kwargs)

    async def command_help(self, **kwargs):
        """
        Provide an overview of help.
        """
        over = 'Here is an overview of my commands.\nFor more information do: ![Command] -h\n'
        lines = [
            ['Command', 'Effect'],
            ['!drop', 'Drop forts into the fort sheet.'],
            ['!dump', 'Dump the database to the server console. For admins.'],
            ['!fort', 'Get information about our fort systems.'],
            ['!info', 'Display information on a user.'],
            ['!scan', 'Rebuild the database by fetching and parsing latest data.'],
            ['!time', 'Show game time and time to ticks.'],
            ['!user', 'UNUSED ATM. Manage users.'],
            ['!help', 'This help message.'],
        ]

        response = over + cog.tbl.wrap_markdown(cog.tbl.format_table(lines, header=True))
        message = kwargs.get('message')
        await self.send_message(message.channel, response)

    async def command_drop(self, **kwargs):
        """
        Drop forts at the fortification target.
        """
        log = logging.getLogger('cog.bot')
        args = kwargs.get('args')
        msg = kwargs.get('message')
        session = kwargs.get('session')

        if args.user:
            args.user = ' '.join(args.user)
            import mock
            duser = mock.Mock()
            duser.suser = cogdb.query.get_sheet_user_by_name(session, args.user)
            duser.sheet_name = duser.suser.sheet_name
            duser.display_name = duser.sheet_name
        else:
            duser = cogdb.query.get_discord_user_by_id(session, msg.author.id)
            cogdb.query.get_or_create_sheet_user(session, duser)
        log.info('DROP - Matched duser %s with id %s.',
                 args.user if args.user else msg.author.display_name, duser.display_name)

        if args.system:
            args.system = ' '.join(args.system)
            system = cogdb.query.get_system_by_name(session, args.system)
        else:
            current = cogdb.query.find_current_target(session)
            system = cogdb.query.get_fort_targets(session, current)[0]
        log.info('DROP - Matched system %s based on args: %s.',
                 system.name, args.system)

        fort = cogdb.query.add_fort(session, self.callback_add_fort,
                                    system=system, user=duser.suser,
                                    amount=args.amount)
        log.info('DROP - Sucessfully dropped %d at %s for %s.',
                 args.amount, system.name, duser.display_name)

        message = kwargs.get('message')
        await self.send_message(message.channel, fort.system.short_display())

    async def command_dump(self, **kwargs):
        """
        For debugging, able to dump the database quickly to console.
        """
        cogdb.query.dump_db()
        message = kwargs.get('message')
        await self.send_message(message.channel, 'Db has been dumped to server console.')

    async def command_fort(self, **kwargs):
        """
        Provide information on and manage the fort sheet.
        """
        args = kwargs.get('args')
        session = kwargs.get('session')
        systems = []

        if args.systems:
            args.long = True
            for system in args.systems:
                try:
                    systems.append(cogdb.query.get_system_by_name(session,
                                                                  system, search_all=True))
                except (cog.exc.NoMatch, cog.exc.MoreThanOneMatch):
                    pass

        elif args.next:
            cur_index = cogdb.query.find_current_target(session)
            systems = cogdb.query.get_next_fort_targets(session,
                                                        cur_index, count=args.next)
        else:
            cur_index = cogdb.query.find_current_target(session)
            systems = cogdb.query.get_fort_targets(session, cur_index)

        if args.status:
            states = cogdb.query.get_all_systems_by_state(session)
            total = len(cogdb.query.get_all_systems(session))

            keys = ['cancelled', 'fortified', 'undermined', 'skipped', 'left']
            lines = [
                [key.capitalize() for key in keys],
                ['{}/{}'.format(len(states[key]), total) for key in keys],
            ]
            response = cog.tbl.wrap_markdown(cog.tbl.format_table(lines, sep='|', header=True))
        elif args.long:
            lines = [systems[0].__class__.header] + [system.table_row for system in systems]
            response = cog.tbl.wrap_markdown(cog.tbl.format_table(lines, sep='|', header=True))
        else:
            lines = [system.short_display() for system in systems]
            response = '\n'.join(lines)

        message = kwargs.get('message')
        await self.send_message(message.channel, response)

    async def command_info(self, **kwargs):
        """
        Provide information about the discord server.
        """
        args = kwargs.get('args')
        message = kwargs.get('message')

        if args.user:
            members = message.channel.server.members
            user = cogdb.query.fuzzy_find(args.user, members, obj_attr='display_name')
        else:
            user = message.author

        lines = [
            '**' + user.display_name + '**',
            '-' * (len(user.display_name) + 6),
            'Username: {}#{}'.format(user.name, user.discriminator),
            'ID: ' + user.id,
            'Status: ' + str(user.status),
            'Join Date: ' + str(user.joined_at),
            'Roles: ' + str([str(role) for role in user.roles[1:]]),
            'Highest Role: ' + str(user.top_role).replace('@', '@ '),
        ]

        await self.send_message(message.channel, '\n'.join(lines))

    async def command_scan(self, **kwargs):
        """
        Allow reindexing the sheets when out of date with new edits.
        """
        cogdb.schema.drop_scanned_tables()
        init_db(cog.share.get_config('hudson', 'cattle'))
        message = kwargs.get('message')
        await self.send_messsage(message.channel, 'The database has been updated.')

    async def command_time(self, **kwargs):
        """
        Provide the time command.

        Shows the time ...
        - In game
        - To daily BGS tick
        - To weekly tick
        """
        now = date.datetime.utcnow().replace(microsecond=0)
        today = now.replace(hour=0, minute=0, second=0)

        daily_tick = today + date.timedelta(hours=16)
        if daily_tick < now:
            daily_tick = daily_tick + date.timedelta(days=1)

        weekly_tick = today + date.timedelta(hours=7)
        while weekly_tick.strftime('%A') != 'Thursday':
            weekly_tick += date.timedelta(days=1)

        lines = [
            'Game Time: **{}**'.format(now.strftime('%H:%M:%S')),
            'Time to BGS Tick: **{}** ({})'.format(daily_tick - now, daily_tick),
            'Time to Cycle Tick: **{}** ({})'.format(weekly_tick - now, weekly_tick),
            'All Times UTC',
        ]

        message = kwargs.get('message')
        await self.send_message(message.channel, '\n'.join(lines))

    async def command_user(self, **kwargs):
        message = kwargs.get('message')
        await self.send_message(message.channel, 'Reserved for future use.')


def init_db(sheet_id):
    """
    Run common database initialization.

    - Fetch the current sheet and parse it.
    - Fill database with parsed data.
    - Settatr this module callbacks for GSheets.
    """
    session = cogdb.Session()

    if not session.query(cogdb.schema.System).all():
        paths = cog.share.get_config('paths')
        sheet = cog.sheets.GSheet(sheet_id,
                                  cog.share.rel_to_abs(paths['json']),
                                  cog.share.rel_to_abs(paths['token']))

        cells = sheet.whole_sheet()
        system_col = cogdb.query.first_system_column(sheet.get_with_formatting('!A10:J10'))
        user_col, user_row = cogdb.query.first_user_row(cells)
        scanner = cogdb.query.SheetScanner(cells, system_col, user_col, user_row)
        scanner.scan(session)

        # Return callbacks
        return (
            functools.partial(cog.sheets.callback_add_user, sheet, user_col),
            functools.partial(cog.sheets.callback_add_fort, sheet),
        )


def main():
    cog.share.init_logging()
    try:
        bot = CogBot('!')
        bot.set_callbacks(init_db(cog.share.get_config('hudson', 'cattle')))
        bot.run(cog.share.get_config('discord_token'))
    finally:
        bot.close()


if __name__ == "__main__":
    main()
