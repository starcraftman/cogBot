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
import re

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
        self.scanner_um = kwargs.get('scanner_um')

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
            cogdb.query.ensure_duser(session, author)
            parser = cog.share.make_parser(self.prefix)
            msg = re.sub(r'<@\w+>', '', msg).strip()
            args = parser.parse_args(msg.split(' '))
            await self.dispatch_command(message=message, args=args, session=session)
        except (cog.exc.NoMatch, cog.exc.MoreThanOneMatch) as exc:
            log.error("Loose cmd failed to match excatly one. '%s' | %s", author.name, msg)
            log.error(exc)
            response = 'Check command arguments ...\n' + str(exc)
            await self.send_ttl_message(channel, response)
            asyncio.ensure_future(self.delete_message(message))
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

    async def send_ttl_message(self, channel, content, *, time=30):
        """
        Send a message to channel and delete it after time seconds.
        Any messages passed in as extra list will also be deleted.

        Args:
            channel: A valid server channel.
            content: The message to send.
            time: The TTL before deletion.
            extra: Additional messages to delete at same time.
        """
        content += '\n\nThis message will be deleted in {} seconds.'.format(time)
        message = await self.send_message(channel, content)

        await asyncio.sleep(time)

        asyncio.ensure_future(self.delete_message(message))

    # Commands beyond here
    async def dispatch_command(self, **kwargs):
        """
        Simply inspect class and dispatch command. Guaranteed to be valid.
        """
        await getattr(self, 'command_' + kwargs.get('args').cmd)(**kwargs)
        # asyncio.ensure_future(self.delete_message(kwargs.get('message')))

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
            ['!scan', 'Rebuild the database with latest sheet data.'],
            ['!time', 'Show game time and time to ticks.'],
            ['!um', 'Get information about undermining targets.'],
            ['!user', 'Manage your user, set sheet name and tag.'],
            ['!help', 'This help message.'],
        ]

        response = over + cog.tbl.wrap_markdown(cog.tbl.format_table(lines, header=True))
        message = kwargs.get('message')
        await self.send_ttl_message(message.channel, response)
        asyncio.ensure_future(self.delete_message(message))

    async def command_drop(self, **kwargs):
        """
        Drop forts at the fortification target.
        """
        log = logging.getLogger('cog.bot')
        args = kwargs.get('args')
        message = kwargs.get('message')
        session = kwargs.get('session')

        # If a user mentions another, assume drop for mentioned user
        author = message.author
        if message.mentions:
            author = message.mentions[0]
        duser = cogdb.query.get_duser(session, author.id)

        if not duser.cattle:
            cogdb.query.add_sheet(session, duser.pref_name,
                                  type=cogdb.schema.ESheetType.cattle)
            asyncio.ensure_future(self.scanner.update_user(duser.cattle))
            notice = 'Automatically added {} to cattle sheet. See !user command to change.'
            asyncio.ensure_future(self.send_message(message.channel,
                                                    notice.format(duser.pref_name)))

        log.info('DROP - Matched duser %s with id %s.', duser.display_name, duser.id[:6])

        if args.system:
            args.system = ' '.join(args.system)
            system = cogdb.query.fort_find_system(session, args.system)
        else:
            current = cogdb.query.fort_find_current_index(session)
            system = cogdb.query.fort_get_targets(session, current)[0]
        log.info('DROP - Matched system %s based on args: %s.',
                 system.name, args.system)

        drop = cogdb.query.fort_add_drop(session, system=system,
                                         user=duser.cattle, amount=args.amount)
        if args.set:
            system.set_status(args.set)
            session.commit()
        asyncio.ensure_future(self.scanner.update_drop(drop))
        asyncio.ensure_future(self.scanner.update_system(drop.system))

        log.info('DROP - Sucessfully dropped %d at %s for %s.',
                 args.amount, system.name, duser.display_name)

        # if drop.system.is_fortified():
            # message.content = self.prefix + 'fort'
            # asyncio.ensure_future(self.on_message(message))
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
        log = logging.getLogger('cog.bot')
        args = kwargs.get('args')
        message = kwargs.get('message')
        session = kwargs.get('session')

        # If a user mentions another, assume working for mentioned user
        author = message.author
        if message.mentions:
            author = message.mentions[0]
        duser = cogdb.query.get_duser(session, author.id)

        if not duser.undermine:
            cogdb.query.add_sheet(session, duser.pref_name,
                                  type=cogdb.schema.ESheetType.um)
            asyncio.ensure_future(self.scanner_um.update_sheet_user(duser.undermine))
            notice = 'Automatically added {} to undermine sheet. See !user command to change.'
            asyncio.ensure_future(self.send_message(message.channel,
                                                    notice.format(duser.pref_name)))

        log.info('HOLD - Matched duser %s with id %s.', duser.display_name, duser.id[:6])

        if args.died:
            cogdb.query.um_reset_held(session, duser.undermine)
            log.info('HOLD - User died %s.', duser.display_name)
            await self.send_message(message.channel, 'Sorry you died :(. Held merits reset.')
            return
        elif args.redeem:
            redeemed = cogdb.query.um_redeem_merits(session, duser.undermine)
            log.info('HOLD - User %s redeemed %d merits.', duser.display_name, redeemed)
            response = 'You have redeemed {} merits.\n{}'.format(redeemed, duser.undermine.merits)
            await self.send_message(message.channel, response)
            return

        # Default case, add a hold.
        search = ''.join(args.system)
        print(search)

        system = cogdb.query.um_find_system(session, search)
        log.info('HOLD - Matched system %s based on args: %s.', system.name, args.system)
        hold = cogdb.query.um_add_hold(session, system=system,
                                       user=duser.undermine, held=args.held)
        if args.set:
            system.set_status(args.set)
            session.commit()
        asyncio.ensure_future(self.scanner_um.update_hold(hold))
        # asyncio.ensure_future(self.scanner_um.update_system(hold.system))

        log.info('Hold - Sucessfully dropped %d at %s for %s.',
                 args.amount, system.name, duser.display_name)

        response = str(hold.system)
        if hold.system.is_undermined():
            response += '\n\nPlaceholder for other UM targets.'
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
        cogdb.schema.drop_tables(all=False)
        self.scanner.scan(kwargs.get('session'))
        self.scanner_um.scan(kwargs.get('session'))
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
        session = kwargs.get('session')

        if args.system:
            system = cogdb.query.um_find_system(session, ' '.join(args.system))
            response = str(system)
            if args.set:
                response = 'Show system post update.'
        else:
            systems = cogdb.query.um_get_systems(session)
            response = '\n'.join([str(system) for system in systems])

        await self.send_message(message.channel, response)

    async def command_user(self, **kwargs):
        """
        Allow a user to manage his/herself in the sheets.
        """
        args = kwargs.get('args')
        message = kwargs.get('message')
        session = kwargs.get('session')

        duser = cogdb.query.get_duser(session, message.author.id)

        if args.name:
            new_name = ' '.join(args.name)
            for sheet in duser.sheets:
                sheet.name = new_name
            duser.pref_name = new_name

        if args.cry:
            for sheet in duser.sheets:
                sheet.cry = args.cry

        if args.name or args.cry:
            asyncio.ensure_future(self.scanner.update_sheet_user(duser.cattle))

        lines = [
            '**{}**'.format(message.author.display_name),
            'Fort Capacity: {}'.format(duser.capacity)
        ]
        for sheet in duser.sheets:
            lines += [
                '{} {}'.format(sheet.faction.capitalize(), sheet.type.replace('Sheet', '')),
                '  Name: {}'.format(sheet.name),
                '  Cry: {}'.format(sheet.cry),
                '  Merits: {}'.format(sheet.merits),
            ]

        await self.send_message(message.channel, '\n'.join(lines))


def scan_sheet(sheet_id, cls):
    """
    Run common database initialization.

    - Fetch the current sheet and parse it.
    - Fill database with parsed data.
    """
    session = cogdb.Session()

    paths = cog.share.get_config('paths')
    sheet = cog.sheets.GSheet(sheet_id,
                              cog.share.rel_to_abs(paths['json']),
                              cog.share.rel_to_abs(paths['token']))

    scanner = cls(sheet)
    scanner.scan(session)

    return scanner


def main():
    cog.share.init_logging()
    try:
        scanner = scan_sheet(cog.share.get_config('hudson', 'cattle'), cogdb.query.FortScanner)
        scanner_um = scan_sheet(cog.share.get_config('hudson', 'um'), cogdb.query.UMScanner)
        bot = CogBot(prefix='!', scanner=scanner, scanner_um=scanner_um)
        # bot = CogBot(prefix='!', scanner=scanner, scanner_um=None)
        bot.run(cog.share.get_config('discord_token'))
    finally:
        try:
            bot.close()
        except UnboundLocalError:
            pass


if __name__ == "__main__":
    main()
