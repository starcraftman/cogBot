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
import pickle
import re
import sys
import tempfile
import time

import discord
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print('Setting uvloop as asyncio default event loop.')
except ImportError:
    pass

import cogdb
import cogdb.schema
import cogdb.query
import cog.exc
import cog.share
import cog.sheets
import cog.tbl


def user_info(user):  # pragma: no cover
    """
    Trivial message formatter based on user information.
    """
    lines = [
        ['Username', '{}#{}'.format(user.name, user.discriminator)],
        ['ID', user.id],
        ['Status', str(user.status)],
        ['Join Date', str(user.joined_at)],
        ['All Roles:', str([str(role) for role in user.roles[1:]])],
        ['Top Role:', str(user.top_role).replace('@', '@ ')],
    ]
    return '**' + user.display_name + '**\n' + cog.tbl.wrap_markdown(cog.tbl.format_table(lines))


def write_start_time(fname):  # pragma: no cover
    """
    On startup write current UTC datetime.
    """
    with open(fname, 'wb') as fout:
        pickle.dump(date.datetime.utcnow().replace(microsecond=0), fout, pickle.HIGHEST_PROTOCOL)


def get_uptime(fname):  # pragma: no cover
    """
    Compare now to when started.
    """
    with open(fname, 'rb') as fin:
        old_now = pickle.load(fin)

    now = date.datetime.utcnow().replace(microsecond=0)
    return str(now - old_now)


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
        self.uptime_file = tempfile.NamedTemporaryFile()
        self.last_cmd = time.time()
        write_start_time(self.uptime_file.name)

    # Events hooked by bot.
    async def on_member_join(self, member):
        """ Called when member joins server (login). """
        log = logging.getLogger('cog.bot')
        log.info('Member has joined: ' + member.display_name)

    async def on_member_leave(self, member):
        """ Called when member leaves server (logout). """
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
            log.info('Invalid combination of arguments or values. %s | %s', author.name, msg)
            await self.send_ttl_message(channel, str(exc))
            asyncio.ensure_future(self.delete_message(message))

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

        content += '\nThis message will be deleted in {} seconds.'.format(ttl)
        message = await self.send_message(destination, content, **kwargs)

        await asyncio.sleep(ttl)
        asyncio.ensure_future(self.delete_message(message))

    # Commands beyond here
    async def dispatch_command(self, **kwargs):
        """
        Simply inspect class and dispatch command. Guaranteed to be valid.
        """
        message = kwargs.get('message')
        # FIXME: Hack due to users editting sheet.
        last_cmd = self.last_cmd
        self.last_cmd = time.time()
        if (time.time() - last_cmd) > 60.0 * 5:
            orig_content = message.content
            asyncio.ensure_future(self.send_message(
                message.channel,
                'Bot has been inactive 5+ mins. Command will execute after update.'))
            message.content = '!admin scan'
            await self.on_message(message)
            await self.send_message(message.channel,
                                    'Now executing: ' + orig_content)
        await getattr(self, 'command_' + kwargs.get('args').cmd)(**kwargs)

    async def command_help(self, **kwargs):
        """
        Provide an overview of help.
        """
        message = kwargs.get('message')
        over = [
            'Here is an overview of my commands.',
            '',
            'For more information do: `{}Command -h`'.format(self.prefix),
            '       Example: `{}drop -h`'.format(self.prefix),
            '',
        ]
        lines = [
            ['Command', 'Effect'],
            ['{prefix}admin', 'Admin commands.'],
            ['{prefix}drop', 'Drop forts into the fort sheet.'],
            ['{prefix}feedback', 'Give feedback or report a bug.'],
            ['{prefix}fort', 'Get information about our fort systems.'],
            ['{prefix}hold', 'Declare held merits or redeem them.'],
            ['{prefix}status', 'Info about this bot.'],
            ['{prefix}time', 'Show game time and time to ticks.'],
            ['{prefix}um', 'Get information about undermining targets.'],
            ['{prefix}user', 'Manage your user, set sheet name and tag.'],
            ['{prefix}help', 'This help message.'],
        ]
        lines = [[line[0].format(prefix=self.prefix), line[1]] for line in lines]

        response = '\n'.join(over) + cog.tbl.wrap_markdown(cog.tbl.format_table(lines, header=True))
        await self.send_ttl_message(message.channel, response)
        asyncio.ensure_future(self.delete_message(message))

    async def bot_shutdown(self):
        """
        Shutdown the bot. Not ideal, I should reconsider later.
        """
        await asyncio.sleep(1)
        await self.logout()
        sys.exit(0)

    async def command_admin(self, **kwargs):
        """
        Admin command console. For knowledgeable users only.
        """
        args = kwargs.get('args')
        message = kwargs.get('message')
        session = kwargs.get('session')
        response = ''

        # TODO: In real solution, check perms on dispatch or make decorator.
        # if message.author.id != '250266447794667520':
            # response = "I'm sorry, {}. I'm afraid I can't do that.".format(
                # message.author.display_name)
            # logging.getLogger('cog.bot').error('Unauthorized Access to !admin: %s %s',
                                               # message.author.id, message.author.display_name)
            # await self.send_message(message.channel, response)
            # return

        if args.subcmd == 'deny':
            self.deny_commands = not self.deny_commands
            response = 'Commands: **{}abled**'.format('Dis' if self.deny_commands else 'En')

        elif args.subcmd == 'dump':
            cogdb.query.dump_db()
            response = 'Db has been dumped to server console.'

        elif args.subcmd == 'halt':
            self.deny_commands = True
            asyncio.ensure_future(self.send_message(message.channel,
                                                    'Shutdown in 40s. Commands: **Disabled**'))
            await asyncio.sleep(40)
            asyncio.ensure_future(self.bot_shutdown())
            response = 'Goodbye!'

        elif args.subcmd == 'scan':
            self.deny_commands = True
            asyncio.ensure_future(self.send_message(message.channel,
                                                    'Updating database. Commands: **Disabled**'))
            await asyncio.sleep(2)

            cogdb.schema.drop_tables(all=False)
            self.scanner.scan(session)
            self.scanner_um.scan(session)

            self.deny_commands = False
            response = 'Update finished. Commands: **Enabled**'

        elif args.subcmd == 'info':
            if message.mentions:
                response = ''
                for user in message.mentions:
                    response += user_info(user) + '\n'
            else:
                response = user_info(message.author)
            message.channel = message.author  # Not for public

        if response:
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
            log.info('DROP %s - Adding to cattle as %s.',
                     duser.display_name, duser.pref_name)
            cogdb.query.add_sheet(session, duser.pref_name, cry=duser.pref_cry,
                                  type=cogdb.schema.ESheetType.cattle)
            asyncio.ensure_future(self.scanner.update_sheet_user(duser.cattle))
            notice = 'Automatically added {} to cattle sheet. See !user command to change.'
            asyncio.ensure_future(self.send_message(message.channel,
                                                    notice.format(duser.pref_name)))

        log.info('DROP %s - Matched duser with id %s and sheet name %s.',
                 duser.display_name, duser.id[:6], duser.cattle.name)

        if args.system:
            args.system = ' '.join(args.system)
            system = cogdb.query.fort_find_system(session, args.system)
            log.info('DROP %s - Matched system %s from: \n%s.',
                     duser.display_name, system.name, system)
        else:
            system = cogdb.query.fort_get_targets(session)[0]
            log.info('DROP %s - Matched current target: \n%s.',
                     duser.display_name, system)

        drop = cogdb.query.fort_add_drop(session, system=system,
                                         user=duser.cattle, amount=args.amount)
        log.info('DROP %s - After drop, Drop: %s\nSystem: %s.',
                 duser.display_name, drop, system)

        if args.set:
            system.set_status(args.set)
        session.commit()
        asyncio.ensure_future(self.scanner.update_drop(drop))
        asyncio.ensure_future(self.scanner.update_system(drop.system))

        log.info('DROP %s - Sucessfully dropped %d at %s.',
                 duser.display_name, args.amount, system.name)

        response = drop.system.display()
        if drop.system.is_fortified:
            new_target = cogdb.query.fort_get_targets(session)[0]
            response += '\n\n__Next Fort Target__:\n' + new_target.display()
        await self.send_message(message.channel, self.fix_emoji(response))

    async def command_feedback(self, **kwargs):
        """
        Send bug reports to Gears' Hideout -> reports channel.
        """
        args = kwargs.get('args')
        message = kwargs.get('message')
        lines = [
            ['Server', message.server.name],
            ['Channel', message.channel.name],
            ['Author', message.author.name],
            ['Time (UTC)', date.datetime.utcnow()],
        ]
        response = cog.tbl.wrap_markdown(cog.tbl.format_table(lines)) + '\n\n'
        response += '__Bug Report Follows__\n\n' + ' '.join(args.content)

        server = discord.utils.get(self.servers, name="Gears' Hideout")
        channel = discord.utils.get(server.channels, name="feedback")
        await self.send_message(channel, response)

    async def command_fort(self, **kwargs):
        """
        Provide information on and manage the fort sheet.
        """
        log = logging.getLogger('cog.bot')
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
            # FIXME: Excessive to fix
            log.info("Fort Summary - Start")
            for key in states:
                log.info("Fort Summary - %s %s", key,
                         str([system.name for system in states[key]]))
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
            asyncio.ensure_future(self.scanner.update_system(system))
            response = system.display(missing=False)
        elif args.long:
            lines = [systems[0].__class__.header] + [system.table_row for system in systems]
            response = cog.tbl.wrap_markdown(cog.tbl.format_table(lines, sep='|', header=True))
        else:
            response = '__{} Fort Targets__\n\n'.format('Next' if args.nextn else 'Current')
            lines = [system.display() for system in systems]
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
            log.info('HOLD %s - Adding to undermining as %s.',
                     duser.display_name, duser.pref_name)
            cogdb.query.add_sheet(session, duser.pref_name, cry=duser.pref_cry,
                                  type=cogdb.schema.ESheetType.um)
            asyncio.ensure_future(self.scanner_um.update_sheet_user(duser.undermine))
            notice = 'Automatically added {} to undermine sheet. See !user command to change.'
            asyncio.ensure_future(self.send_message(message.channel,
                                                    notice.format(duser.pref_name)))

        log.info('HOLD %s - Matched duser with id %s and sheet name %s.',
                 duser.display_name, duser.id[:6], duser.undermine.name)

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
            log.info('HOLD %s - Matched system name %s: \n%s.',
                     duser.display_name, args.system, system)
            hold = cogdb.query.um_add_hold(session, system=system,
                                           user=duser.undermine, held=args.amount)
            holds = [hold]

            if args.set:
                system.set_status(args.set)
                asyncio.ensure_future(self.scanner_um.update_system(hold.system))

            log.info('Hold %s - After update, hold: %s\nSystem: %s.',
                     duser.display_name, hold, system)

            response = hold.system.display()
            if hold.system.is_undermined:
                response += '\n\nSystem is finished with held merits. Type `!um` for more targets.'

        session.commit()
        for hold in holds:
            asyncio.ensure_future(self.scanner_um.update_hold(hold))
        await self.send_message(message.channel, response)

    async def command_status(self, **kwargs):
        """
        Simple bot info command.
        """
        lines = [
            ['Created By', 'GearsandCogs'],
            ['Uptime', get_uptime(self.uptime_file.name)],
            ['Version', '{}'.format(cog.__version__)],
        ]
        response = cog.tbl.wrap_markdown(cog.tbl.format_table(lines))
        await self.send_message(kwargs.get('message').channel, response)

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

            response = system.display()

        else:
            systems = cogdb.query.um_get_systems(session)
            response = '__Current UM Targets__\n\n' + '\n'.join(
                [system.display() for system in systems])

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
        session.commit()

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
        cogdb.schema.drop_tables(all=False)  # Only persistent data should be kept on startup
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
