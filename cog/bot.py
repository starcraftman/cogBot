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
    def __init__(self, **kwargs):
        super(CogBot, self).__init__(**kwargs)
        self.prefix = kwargs.get('prefix')
        self.scanner = kwargs.get('scanner')

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
            session = cogdb.Session()
            cogdb.query.check_discord_user(session, author)
            parser = cog.share.make_parser(self.prefix)
            args = parser.parse_args(msg.split(' '))
            await self.dispatch_command(message=message, args=args, session=session)
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
            ['!hold', 'Declare held merits or redeem them.'],
            ['!info', 'Display information on a user.'],
            ['!scan', 'Rebuild the database by fetching and parsing latest data.'],
            ['!time', 'Show game time and time to ticks.'],
            ['!um', 'Get information about undermining targets.'],
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
        message = kwargs.get('message')
        session = kwargs.get('session')

        if args.user:
            args.user = ' '.join(args.user)
            duser = cogdb.query.get_sheet_user_by_name(session, args.user).duser
        else:
            duser = cogdb.query.get_discord_user_by_id(session, message.author.id)
            cogdb.query.check_sheet_user(
                session, duser,
                lambda x: asyncio.ensure_future(self.scanner.update_user(x))
            )
        log.info('DROP - Matched duser %s with id %s.',
                 args.user if args.user else message.author.display_name, duser.id)

        if args.system:
            args.system = ' '.join(args.system)
            system = cogdb.query.fort_find_system(session, args.system)
        else:
            current = cogdb.query.fort_find_current_index(session)
            system = cogdb.query.fort_get_targets(session, current)[0]
        log.info('DROP - Matched system %s based on args: %s.',
                 system.name, args.system)

        drop = cogdb.query.fort_add_drop(session, system=system,
                                         suser=duser.suser, amount=args.amount)
        if args.set:
            system.set_status(args.set)
            session.commit()
        asyncio.ensure_future(self.scanner.update_drop(drop))
        asyncio.ensure_future(self.scanner.update_system(drop.system))

        log.info('DROP - Sucessfully dropped %d at %s for %s.',
                 args.amount, system.name, duser.display_name)

        await self.send_message(message.channel, drop.system.short_display())

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
        cur_index = cogdb.query.fort_find_current_index(session)

        if args.system:
            systems.append(cogdb.query.fort_find_system(session, args.system,
                                                        search_all=True))
        elif args.next:
            systems = cogdb.query.fort_get_next_targets(session,
                                                        cur_index, count=args.next)
        else:
            systems = cogdb.query.fort_get_targets(session, cur_index)

        if args.summary:
            states = cogdb.query.fort_get_systems_by_state(session)
            total = len(cogdb.query.fort_get_systems(session))

            keys = ['cancelled', 'fortified', 'undermined', 'skipped', 'left']
            lines = [
                [key.capitalize() for key in keys],
                ['{}/{}'.format(len(states[key]), total) for key in keys],
            ]
            response = cog.tbl.wrap_markdown(cog.tbl.format_table(lines, sep='|', header=True))
        elif args.set:
            system = systems[0]
            system.set_status(args.set)
            session.commit()
            asyncio.ensure_future(self.scanner.update_system(system, max_fort=False))
            response = system.short_display(missing=False) + ', um: {}'.format(system.um_status)
        elif args.long:
            lines = [systems[0].__class__.header] + [system.table_row for system in systems]
            response = cog.tbl.wrap_markdown(cog.tbl.format_table(lines, sep='|', header=True))
        else:
            lines = [system.short_display() for system in systems]
            response = '\n'.join(lines)

        message = kwargs.get('message')
        await self.send_message(message.channel, response)

    async def command_hold(self, **kwargs):
        """
        Update a user's held merits.
        """
        args = kwargs.get('args')
        message = kwargs.get('message')

        if args.died:
            response = 'You died. All merits reset.'
        elif args.redeem:
            response = 'You have redeemed {} new merits, total this cycle {}.'.format(0, 0)
        elif args.held and args.system:
            response = 'Holding {} merits in {}.'.format(args.held, args.system)
        else:
            response = 'Holding these merits:'

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
        self.scanner.scan(kwargs.get('session'))
        message = kwargs.get('message')
        await self.send_message(message.channel, 'The database has been updated.')

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

    async def command_um(self, **kwargs):
        """
        Command to show um systems and update status.
        """
        args = kwargs.get('args')
        message = kwargs.get('message')

        if args.system:
            response = 'Show the system requested.'
            if args.set:
                response = 'Show system post update.'
        else:
            response = 'Show all incomplete systems.'

        await self.send_message(message.channel, response)

    async def command_user(self, **kwargs):
        """
        Allow a user to manage his/herself in the sheets.
        """
        args = kwargs.get('args')
        message = kwargs.get('message')
        session = kwargs.get('session')

        duser = cogdb.query.get_discord_user_by_id(session, message.author.id)

        if not duser.suser:
            cogdb.query.check_sheet_user(session, duser)

        if args.name:
            duser.pref_name = ' '.join(args.name)
            duser.suser.name = duser.pref_name

        if args.cry:
            duser.suser.cry = ' '.join(args.cry)
        if args.name or args.cry:
            asyncio.ensure_future(self.scanner.update_sheet_user(duser.suser))

        lines = [
            '**{}**'.format(message.author.display_name),
            'Cattle',
            '  Name: {}'.format(duser.suser.name),
            '  Cry: {}'.format(duser.suser.cry),
            '  Merits: {}'.format(duser.suser.merits),
        ]

        await self.send_message(message.channel, '\n'.join(lines))


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

        scanner = cogdb.query.SheetScanner(sheet)
        scanner.scan(session)

        return scanner


def main():
    cog.share.init_logging()
    try:
        scanner = init_db(cog.share.get_config('hudson', 'cattle'))
        bot = CogBot(prefix='!', scanner=scanner)
        bot.run(cog.share.get_config('discord_token'))
    finally:
        try:
            bot.close()
        except UnboundLocalError:
            pass


if __name__ == "__main__":
    main()
