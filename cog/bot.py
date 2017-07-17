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
import re
import sys

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
        self.deny_commands = True

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
        self.deny_commands = False

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

        # Ignore lines not directed at bot or when denying commands
        if author.bot or self.deny_commands or not msg.startswith(self.prefix):
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
        except cog.exc.InvalidCommandArgs as exc:
            log.info('Invalid combination of arguments. %s | %s', author.name, msg)
            await self.send_ttl_message(channel, str(exc))
            asyncio.ensure_future(self.delete_message(message))

    async def bot_shutdown(self):
        """
        Shutdown the bot. Not ideal, I should reconsider later.
        """
        await asyncio.sleep(1)
        await self.logout()
        sys.exit(0)

    def fix_emoji(self, content):
        """
        Expand any emojis for bot before sending.

        Embed emojis into text surrounded by ':', example:
            Status update :Fortifying:
        """
        embeds = cog.share.extract_emoji(content)
        all_emoji = list(self.get_all_emojis())
        all_emoji_names = [emoji.name for emoji in all_emoji]
        emoji_dict = dict(zip(all_emoji_names, all_emoji))
        for embed in embeds:
            emoji = emoji_dict.get(embed[1:-1])
            if emoji:
                content = content.replace(embed, str(emoji))
            else:
                logging.getLogger('cog.bot').warning('FIX_EMOJI: Could not find emoji for: %s',
                                                     str(embed))

        return content

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
        content += '\nThis message will be deleted in {} seconds.'.format(time)
        message = await self.send_message(channel, content)

        await asyncio.sleep(time)

        asyncio.ensure_future(self.delete_message(message))

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
        message = kwargs.get('message')
        over = 'Here is an overview of my commands.\nFor more information do: ![Command] -h\n'
        lines = [
            ['Command', 'Effect'],
            ['{prefix}admin', 'Admin commands'],
            ['{prefix}drop', 'Drop forts into the fort sheet.'],
            ['{prefix}fort', 'Get information about our fort systems.'],
            ['{prefix}hold', 'Declare held merits or redeem them.'],
            ['{prefix}time', 'Show game time and time to ticks.'],
            ['{prefix}um', 'Get information about undermining targets.'],
            ['{prefix}user', 'Manage your user, set sheet name and tag.'],
            ['{prefix}help', 'This help message.'],
        ]
        lines = [[line[0].format(prefix=self.prefix), line[1]] for line in lines]

        response = over + cog.tbl.wrap_markdown(cog.tbl.format_table(lines, header=True))
        await self.send_ttl_message(message.channel, response)
        asyncio.ensure_future(self.delete_message(message))

    async def command_admin(self, **kwargs):
        """
        Admin command console. For knowledgeable users only.
        """
        args = kwargs.get('args')
        message = kwargs.get('message')
        session = kwargs.get('session')

        # TODO: In real solution, check perms on dispatch or make decorator.
        if message.author.id != '250266447794667520':
            response = "I'm sorry, {}. I'm afraid I can't do that.".format(
                message.author.display_name)
            logging.getLogger('cog.bot').error('Unauthorized Access to !admin: %s %s',
                                               message.author.id, message.author.display_name)
            await self.send_message(message.channel, response)
            return

        if args.subcmd == 'dump':
            cogdb.query.dump_db()
            response = 'Db has been dumped to server console.'

        if args.subcmd == 'halt':
            self.deny_commands = True
            asyncio.ensure_future(self.send_message(message.channel,
                                                    'Shutdown in 40s. No more commands accepted.'))
            await asyncio.sleep(40)
            asyncio.ensure_future(self.bot_shutdown())
            response = 'Goodbye!'

        elif args.subcmd == 'scan':
            await self.send_message(message.channel, 'Wait until scan done.')
            cogdb.schema.drop_tables(all=False)
            self.scanner.scan(session)
            self.scanner_um.scan(session)
            response = 'Scan finished. The database is current.'

        elif args.subcmd == 'info':
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
            response = '\n'.join(lines)

        await self.send_message(message.channel, response)

    async def command_drop(self, **kwargs):
        """
        Drop forts at the fortification target.
        """
        log = logging.getLogger('cog.bot')
        args = kwargs.get('args')
        message = kwargs.get('message')
        session = kwargs.get('session')

        # If a user mentions another, assume drop for mentioned user
        if message.mentions:
            if len(message.mentions) != 1:
                raise cog.exc.InvalidCommandArgs('Mention only 1 member per command.')

            log.info('DROP %s - Substituting author -> %s.',
                     message.author, message.mentions[0])
            message.author = message.mentions[0]
            message.mentions = []
            asyncio.ensure_future(self.on_message(message))
            return

        duser = cogdb.query.get_duser(session, message.author.id)
        if not duser.cattle:
            cogdb.query.add_sheet(session, duser.pref_name, cry=duser.pref_cry,
                                  type=cogdb.schema.ESheetType.cattle)
            asyncio.ensure_future(self.scanner.update_sheet_user(duser.cattle))
            notice = 'Automatically added {} to cattle sheet. See !user command to change.'
            asyncio.ensure_future(self.send_message(message.channel,
                                                    notice.format(duser.pref_name)))

        log.info('DROP %s - Matched duser with id %s.', duser.display_name, duser.id[:6])

        if args.system:
            args.system = ' '.join(args.system)
            system = cogdb.query.fort_find_system(session, args.system)
            log.info('DROP %s - Matched system %s from: %s.',
                     duser.display_name, system.name, args.system)
        else:
            system = cogdb.query.fort_get_targets(session)[0]
            log.info('DROP %s - Matched current target: %s.',
                     duser.display_name, system.name)

        drop = cogdb.query.fort_add_drop(session, system=system,
                                         user=duser.cattle, amount=args.amount)
        if args.set:
            system.set_status(args.set)
        asyncio.ensure_future(self.scanner.update_drop(drop))
        asyncio.ensure_future(self.scanner.update_system(drop.system))

        log.info('DROP %s - Sucessfully dropped %d at %s.',
                 duser.display_name, args.amount, system.name)

        session.commit()
        response = drop.system.short_display()
        if drop.system.is_fortified:
            new_target = cogdb.query.fort_get_targets(session)[0]
            response += '\n\nNext Target: ' + new_target.short_display()
        await self.send_message(message.channel, drop.system.short_display())

    async def command_fort(self, **kwargs):
        """
        Provide information on and manage the fort sheet.
        """
        args = kwargs.get('args')
        session = kwargs.get('session')
        systems = []

        if args.next:
            args.nextn = 1

        if args.system:
            systems.append(cogdb.query.fort_find_system(session, ' '.join(args.system),
                                                        search_all=True))
        elif args.nextn:
            systems = cogdb.query.fort_get_next_targets(session, count=args.nextn)
        else:
            systems = cogdb.query.fort_get_targets(session)

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
            response = '__{} Fort Targets__\n\n'.format('Next' if args.nextn else 'Current')
            lines = [system.short_display() for system in systems]
            response += '\n'.join(lines)

        message = kwargs.get('message')
        await self.send_message(message.channel, self.fix_emoji(response))

    async def command_hold(self, **kwargs):
        """
        Update a user's held merits.
        """
        log = logging.getLogger('cog.bot')
        args = kwargs.get('args')
        message = kwargs.get('message')
        session = kwargs.get('session')

        # If a user mentions another, assume drop for mentioned user
        if message.mentions:
            if len(message.mentions) != 1:
                raise cog.exc.InvalidCommandArgs('Mention only 1 member per command.')

            log.info('HOLD - Substituting author: %s ->  %s in message.',
                     message.author, message.mentions[0])
            message.author = message.mentions[0]
            message.mentions = []
            asyncio.ensure_future(self.on_message(message))
            return

        duser = cogdb.query.get_duser(session, message.author.id)
        if not duser.undermine:
            cogdb.query.add_sheet(session, duser.pref_name, cry=duser.pref_cry,
                                  type=cogdb.schema.ESheetType.um)
            asyncio.ensure_future(self.scanner_um.update_sheet_user(duser.undermine))
            notice = 'Automatically added {} to undermine sheet. See !user command to change.'
            asyncio.ensure_future(self.send_message(message.channel,
                                                    notice.format(duser.pref_name)))

        log.info('HOLD %s - Matched duser with id %s.', duser.display_name, duser.id[:6])

        if args.died:
            holds = cogdb.query.um_reset_held(session, duser.undermine)
            log.info('HOLD %s - User reset merits.', duser.display_name)
            response = 'Sorry you died :(. Held merits reset.'

        elif args.redeem:
            holds, redeemed = cogdb.query.um_redeem_merits(session, duser.undermine)
            log.info('HOLD %s - Redeemed %d merits.', duser.display_name, redeemed)
            response = 'You redeemed {} new merits.\n{}'.format(redeemed, duser.undermine.merits)

        else:  # Default case, update the hold for a system
            system = cogdb.query.um_find_system(session, ' '.join(args.system))
            log.info('HOLD %s - Matched system %s from: %s.',
                     duser.display_name, system.name, args.system)
            hold = cogdb.query.um_add_hold(session, system=system,
                                           user=duser.undermine, held=args.amount)
            holds = [hold]

            if args.set:
                system.set_status(args.set)
                asyncio.ensure_future(self.scanner_um.update_system(hold.system))

            log.info('Hold %s - Update hold of  %d at %s.',
                     duser.display_name, args.amount, system.name)

            response = str(hold.system)
            if hold.system.is_undermined:
                response += '\n\nPlaceholder for other UM targets.'

        session.commit()
        for hold in holds:
            asyncio.ensure_future(self.scanner_um.update_hold(hold))
        await self.send_message(message.channel, response)

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

        # Sanity check
        if (args.set or args.offset) and not args.system:
            response = 'Forgot to specify system to update'

        elif args.system:
            system = cogdb.query.um_find_system(session, ' '.join(args.system))

            if args.offset:
                system.map_offset = args.offset
            if args.set:
                system.set_status(args.set)
            if args.set or args.offset:
                session.commit()
                asyncio.ensure_future(self.scanner_um.update_system(system))

            response = str(system)

        else:
            systems = cogdb.query.um_get_systems(session)
            response = '__Current UM Targets__\n\n' + '\n'.join([str(system) for system in systems])

        await self.send_message(message.channel, response)

    async def command_user(self, **kwargs):
        """
        Allow a user to manage his/herself in the sheets.
        """
        args = kwargs.get('args')
        message = kwargs.get('message')
        session = kwargs.get('session')
        log = logging.getLogger('cog.bot')

        duser = cogdb.query.get_duser(session, message.author.id)
        if args.name:
            args.name = ' '.join(args.name)
            log.info('USER %s - DUser.pref_name from %s -> %s.',
                     duser.display_name, duser.pref_name, args.name)
            for sheet in duser.sheets:
                sheet.name = args.name
            duser.pref_name = args.name

        if args.cry:
            args.cry = ' '.join(args.cry)
            log.info('USER %s - DUser.pref_cry from %s -> %s.',
                     duser.display_name, duser.pref_cry, args.cry)
            for sheet in duser.sheets:
                sheet.cry = args.cry
            duser.pref_cry = args.cry

        if args.hudson:
            log.info('USER %s - Duser.faction -> hudson.', duser.display_name)

        if args.winters:
            log.info('USER %s - Duser.faction -> winters.', duser.display_name)

        lines = [
            '**{}**'.format(message.author.display_name),
        ]
        for sheet in duser.sheets:
            lines += [
                '{} {}'.format(sheet.faction.capitalize(), sheet.type.replace('Sheet', '')),
                '    Name: {}'.format(sheet.name),
                '    Cry: {}'.format(sheet.cry),
                '    Merits: {}'.format(sheet.merits),
            ]

        if args.name or args.cry:
            asyncio.ensure_future(self.scanner.update_sheet_user(duser.cattle))
            asyncio.ensure_future(self.scanner_um.update_sheet_user(duser.undermine))
        session.commit()
        await self.send_message(message.channel, '\n'.join(lines))


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
        scanner = scan_sheet(cog.share.get_config('hudson', 'cattle'), cogdb.query.FortScanner)
        scanner_um = scan_sheet(cog.share.get_config('hudson', 'um'), cogdb.query.UMScanner)
        bot = CogBot(prefix='!', scanner=scanner, scanner_um=scanner_um)
        # BLOCKING: N.o. e.s.c.a.p.e.
        bot.run(cog.share.get_config('discord', os.environ.get('COG_TOKEN', 'dev')))
    finally:
        try:
            bot.logout()
        except UnboundLocalError:
            pass


if __name__ == "__main__":  # pragma: no cover
    main()
