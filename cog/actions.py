"""
To facilitate complex actions based on commands create a
hierarchy of actions that can be recombined in any order.
All actions have async execute methods.
"""
from __future__ import absolute_import, print_function
import asyncio
import datetime
import logging
import sys

import decorator
import discord
from sqlalchemy.sql import text as sql_text

import cogdb
import cog.tbl


HOME = "Gears' Hideout"
FUC = "Federal United Command"


async def bot_shutdown(bot):  # pragma: no cover
    """
    Shutdown the bot. Not ideal, I should reconsider later.
    """
    await asyncio.sleep(1)
    await bot.logout()
    sys.exit(0)


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


@decorator.decorator
async def check_mentions(coro, *args, **kwargs):
    """ If a single member mentioned, resubmit message on their behalf. """
    self = args[0]
    message = self.message

    if message.mentions:
        if len(message.mentions) != 1:
            raise cog.exc.InvalidCommandArgs('Mention only 1 member per command.')

        self.log.info('DROP %s - Substituting author -> %s.',
                      message.author, message.mentions[0])
        message.author = message.mentions[0]
        message.mentions = []
        asyncio.ensure_future(self.bot.on_message(message))

    else:
        await coro(*args, **kwargs)


class Action(object):
    """
    Top level action, contains shared logic.
    """
    def __init__(self, **kwargs):
        self.args = kwargs['args']
        self.bot = kwargs['bot']
        self.message = kwargs['message']
        self.log = logging.getLogger('cog.actions')
        self.session = cogdb.Session()
        self.__duser = None

    @property
    def mauthor(self):
        """ Author of the message. """
        return self.message.author

    @property
    def mchannel(self):
        """ Channel message came from. """
        return self.message.channel

    @property
    def mserver(self):
        """ Server message came from. """
        return self.message.server

    @property
    def duser(self):
        """ DUser associated with message author. """
        if not self.__duser:
            self.__duser = cogdb.query.ensure_duser(self.session, self.mauthor)

        return self.__duser

    def get_channel(self, server, channel_name):
        """ Given a server and channel_name, get the channel object requested. """
        if not isinstance(server, discord.Server):
            server = discord.utils.get(self.bot.servers, name=server)
        return discord.utils.get(server.channels, name=channel_name)

    async def execute(self):
        """
        Take steps to accomplish requested action, including possibly
        invoking and scheduling other actions.
        """
        raise NotImplementedError


class Help(Action):
    """
    Provide an overview of help.
    """
    async def execute(self):
        prefix = self.bot.prefix
        over = [
            'Here is an overview of my commands.',
            '',
            'For more information do: `{}Command -h`'.format(prefix),
            '       Example: `{}drop -h`'.format(prefix),
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
        lines = [[line[0].format(prefix=prefix), line[1]] for line in lines]

        response = '\n'.join(over) + cog.tbl.wrap_markdown(cog.tbl.format_table(lines, header=True))
        await self.bot.send_ttl_message(self.mchannel, response)
        asyncio.ensure_future(self.bot.delete_message(self.message))


class Feedback(Action):
    """
    Send bug reports to Gears' Hideout reporting channel.
    """
    async def execute(self):
        lines = [
            ['Server', self.message.server.name],
            ['Channel', self.message.channel.name],
            ['Author', self.message.author.name],
            ['Date (UTC)', datetime.datetime.utcnow()],
        ]
        response = cog.tbl.wrap_markdown(cog.tbl.format_table(lines)) + '\n\n'
        response += '__Bug Report Follows__\n\n' + ' '.join(self.args.content)

        self.log.info('FEEDBACK %s - Left a bug report.', self.mauthor.name)
        await self.bot.send_message(self.get_channel(HOME, 'feedback'), response)


class Status(Action):
    """
    Display the status of this bot.
    """
    async def execute(self):
        lines = [
            ['Created By', 'GearsandCogs'],
            ['Uptime', self.bot.uptime],
            ['Version', '{}'.format(cog.__version__)],
        ]

        await self.bot.send_message(self.mchannel,
                                    cog.tbl.wrap_markdown(cog.tbl.format_table(lines)))


class Bgs(Action):
    """
    Provide bgs related commands.
    """
    async def execute(self):
        remote = cogdb.SideSession()

        system = cogdb.query.fort_find_system(self.session, ' '.join(self.args.system),
                                              search_all=True)
        self.log.info('BGS - Looking for: %s', system.name)

        query = sql_text("SELECT * FROM v_age WHERE control=:name ORDER BY age desc")
        query = query.bindparams(name=system.name)

        result = list(remote.execute(query))
        self.log.info('BGS - Received from query: %s', str(result))

        lines = [['Control', 'System', 'Age']] + result
        response = cog.tbl.wrap_markdown(cog.tbl.format_table(lines, header=True))
        await self.bot.send_message(self.mchannel, response)


class Time(Action):
    """
    Provide in game time and time to import in game ticks.

    Shows the time ...
    - In game
    - To daily BGS tick
    - To weekly tick
    """
    async def execute(self):
        now = datetime.datetime.utcnow().replace(microsecond=0)
        today = now.replace(hour=0, minute=0, second=0)

        remote = cogdb.SideSession()
        query = sql_text('select tick from bgs_tick where tick > :date order by tick asc limit 1')
        query = query.bindparams(date=str(now))
        bgs_tick = remote.execute(query).fetchone()[0]
        self.log.info('BGS_TICK - %s -> %s', str(now), bgs_tick)

        if bgs_tick < now:
            bgs_tick = bgs_tick + datetime.timedelta(days=1)

        weekly_tick = today + datetime.timedelta(hours=7)
        while weekly_tick < now or weekly_tick.strftime('%A') != 'Thursday':
            weekly_tick += datetime.timedelta(days=1)

        lines = [
            'Game Time: **{}**'.format(now.strftime('%H:%M:%S')),
            'BGS Tick in **{}**    (Expected {})'.format(bgs_tick - now, bgs_tick),
            'Cycle Ends in **{}**'.format(weekly_tick - now),
            'All Times UTC',
        ]

        await self.bot.send_message(self.mchannel, '\n'.join(lines))


class User(Action):
    """
    Manage your user settings.
    """
    async def execute(self):
        args = self.args
        if args.name:
            self.update_name()

        if args.cry:
            self.update_cry()

        if args.hudson:
            self.log.info('USER %s - Duser.faction -> hudson.', self.mauthor.display_name)
            asyncio.ensure_future(
                self.bot.send_message(self.mchannel, "Not available at this time."))

        if args.winters:
            self.log.info('USER %s - Duser.faction -> winters.', self.mauthor.display_name)
            asyncio.ensure_future(
                self.bot.send_message(self.mchannel, "Not available at this time."))

        self.session.commit()
        if args.name or args.cry:
            if self.duser.cattle:
                asyncio.ensure_future(self.bot.scanner.update_sheet_user(self.duser.cattle))
            if self.duser.undermine:
                asyncio.ensure_future(
                    self.bot.scanner_um.update_sheet_user(self.duser.undermine))

        lines = [
            '**{}**'.format(self.mauthor.display_name),
        ]
        for sheet in self.duser.sheets:
            lines += [
                '{} {}'.format(sheet.faction.capitalize(), sheet.type.replace('Sheet', '')),
                '    Merits: {}'.format(sheet.merits),
            ]

        await self.bot.send_message(self.mchannel, '\n'.join(lines))

    def update_name(self):
        """ Update the user's cmdr name in the sheets. """
        new_name = ' '.join(self.args.name)
        self.log.info('USER %s - DUser.pref_name from %s -> %s.',
                      self.duser.display_name, self.duser.pref_name, new_name)

        for sheet in self.duser.sheets:
            sheet.name = new_name
        self.duser.pref_name = new_name

    def update_cry(self):
        """ Update the user's cry in the sheets. """
        new_cry = ' '.join(self.args.cry)
        self.log.info('USER %s - DUser.pref_cry from %s -> %s.',
                      self.duser.display_name, self.duser.pref_cry, new_cry)

        for sheet in self.duser.sheets:
            sheet.cry = new_cry
        self.duser.pref_cry = new_cry


class Drop(Action):
    """
    Handle the logic of dropping a fort at a target.
    """
    def check_sheet(self):
        """ Check if user present in sheet. """
        if self.duser.cattle:
            return

        self.log.info('DROP %s - Adding to cattle as %s.',
                      self.duser.display_name, self.duser.pref_name)
        cogdb.query.add_sheet(self.session, self.duser.pref_name, cry=self.duser.pref_cry,
                              type=cogdb.schema.ESheetType.cattle)
        asyncio.ensure_future(self.bot.scanner.update_sheet_user(self.duser.cattle))
        notice = 'Automatically added {} to cattle sheet. See !user command to change.'.format(
            self.duser.pref_name)
        asyncio.ensure_future(self.bot.send_message(self.mchannel, notice))

    @check_mentions
    async def execute(self):
        """
        Drop forts at the fortification target.
        """
        self.check_sheet()
        self.log.info('DROP %s - Matched duser with id %s and sheet name %s.',
                      self.duser.display_name, self.duser.id[:6], self.duser.cattle)

        system = cogdb.query.fort_find_system(self.session, ' '.join(self.args.system))
        self.log.info('DROP %s - Matched system %s from: \n%s.',
                      self.duser.display_name, system.name, system)

        drop = cogdb.query.fort_add_drop(self.session, system=system,
                                         user=self.duser.cattle, amount=self.args.amount)
        self.log.info('DROP %s - After drop, Drop: %s\nSystem: %s.',
                      self.duser.display_name, drop, system)

        if self.args.set:
            system.set_status(self.args.set)
        self.session.commit()

        asyncio.ensure_future(self.bot.scanner.update_drop(drop))
        asyncio.ensure_future(self.bot.scanner.update_system(drop.system))

        self.log.info('DROP %s - Sucessfully dropped %d at %s.',
                      self.duser.display_name, self.args.amount, system.name)

        response = drop.system.display()
        if drop.system.is_fortified:
            new_target = cogdb.query.fort_get_targets(self.session)[0]
            response += '\n\n__Next Fort Target__:\n' + new_target.display()
        await self.bot.send_message(self.mchannel, self.bot.emoji.fix(response, self.mserver))


class Fort(Action):
    """
    Provide information on and manage the fort sheet.
    """
    async def execute(self):
        if self.args.summary:
            states = cogdb.query.fort_get_systems_by_state(self.session)

            # FIXME: Excessive to fix
            self.log.info("Fort Summary - Start")
            for key in states:
                self.log.info("Fort Summary - %s %s", key,
                              str([system.name for system in states[key]]))
            total = len(cogdb.query.fort_get_systems(self.session))

            keys = ['cancelled', 'fortified', 'undermined', 'skipped', 'left']
            lines = [
                [key.capitalize() for key in keys],
                ['{}/{}'.format(len(states[key]), total) for key in keys],
            ]
            response = cog.tbl.wrap_markdown(cog.tbl.format_table(lines, sep='|', header=True))

        elif self.args.set:
            system_name = ' '.join(self.args.system)
            if ',' in system_name:
                raise cog.exc.InvalidCommandArgs('One system at a time with --set flag')

            system = cogdb.query.fort_find_system(self.session, system_name, search_all=True)
            system.set_status(self.args.set)
            self.session.commit()
            asyncio.ensure_future(self.bot.scanner.update_system(system))
            response = system.display()

        # elif self.args.long:
            # lines = [systems[0].__class__.header] + [system.table_row for system in systems]
            # response = cog.tbl.wrap_markdown(cog.tbl.format_table(lines, sep='|', header=True))

        elif self.args.miss:
            lines = ['__Systems Missing {} Supplies__\n'.format(self.args.miss)]

            for system in cogdb.query.fort_get_systems(self.session):
                if not system.is_fortified and not system.skip and system.missing <= self.args.miss:
                    lines.append(system.display(force_miss=True))

            response = '\n'.join(lines)

        elif self.args.system:
            lines = ['__Search Results__\n']
            system_names = ' '.join(self.args.system).split(',')
            for name in system_names:
                lines.append(cogdb.query.fort_find_system(self.session,
                                                          name.strip(), search_all=True).display())
            response = '\n'.join(lines)

        else:
            lines = ['__Active Targets__']
            lines += [system.display() for system in cogdb.query.fort_get_targets(self.session)]
            lines += ['\n__Next Targets__']
            lines += [system.display() for system in
                      cogdb.query.fort_get_next_targets(self.session, count=self.args.next)]

            defers = cogdb.query.fort_get_deferred_targets(self.session)
            if defers:
                lines += ['\n__Almost Done__'] + [system.display() for system in defers]

            response = '\n'.join(lines)

        await self.bot.send_message(self.mchannel, self.bot.emoji.fix(response, self.mserver))


class Admin(Action):
    """
    Admin command console. For knowledgeable users only.
    """
    async def execute(self):
        args = self.args
        message = self.message
        session = self.session
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
            self.bot.deny_commands = not self.bot.deny_commands
            response = 'Commands: **{}abled**'.format('Dis' if self.bot.deny_commands else 'En')

        elif args.subcmd == 'dump':
            cogdb.query.dump_db()
            response = 'Db has been dumped to server console.'

        elif args.subcmd == 'halt':
            self.bot.deny_commands = True
            asyncio.ensure_future(self.bot.send_message(message.channel,
                                                        'Shutdown in 40s. Commands: **Disabled**'))
            await asyncio.sleep(40)
            asyncio.ensure_future(bot_shutdown(self.bot))
            response = 'Goodbye!'

        elif args.subcmd == 'scan':
            self.bot.deny_commands = True
            asyncio.ensure_future(self.bot.send_message(message.channel,
                                                        'Updating db. Commands: **Disabled**'))
            await asyncio.sleep(2)

            # TODO: Blocks here, problematic for async. Use thread for scanners?
            cogdb.schema.drop_tables(all=False)
            self.bot.scanner.scan(session)
            self.bot.scanner_um.scan(session)

            # Commands only accepted if no critical errors during update
            self.bot.deny_commands = False
            await self.bot.send_message(message.channel,
                                        'Update finished. Commands: **Enabled**')

        elif args.subcmd == 'info':
            if message.mentions:
                response = ''
                for user in message.mentions:
                    response += user_info(user) + '\n'
            else:
                response = user_info(message.author)
            message.channel = message.author  # Not for public

        if response:
            await self.bot.send_message(message.channel, response)


class Hold(Action):
    """
    Update a user's held merits.
    """
    def check_sheet(self):
        """ Check if user present in sheet. """
        if self.duser.undermine:
            return

        self.log.info('DROP %s - Adding to cattle as %s.',
                      self.duser.display_name, self.duser.pref_name)
        cogdb.query.add_sheet(self.session, self.duser.pref_name, cry=self.duser.pref_cry,
                              type=cogdb.schema.ESheetType.um)
        asyncio.ensure_future(self.bot.scanner_um.update_sheet_user(self.duser.undermine))
        notice = 'Automatically added {} to undermine sheet. See !user command to change.'.format(
            self.duser.pref_name)
        asyncio.ensure_future(self.bot.send_message(self.mchannel, notice))

    def set_hold(self):
        """ Set the hold on a system. """
        system = cogdb.query.um_find_system(self.session, ' '.join(self.args.system))
        self.log.info('HOLD %s - Matched system name %s: \n%s.',
                      self.duser.display_name, self.args.system, system)
        hold = cogdb.query.um_add_hold(self.session, system=system,
                                       user=self.duser.undermine, held=self.args.amount)

        if self.args.set:
            system.set_status(self.args.set)
            asyncio.ensure_future(self.bot.scanner_um.update_system(hold.system))

        self.log.info('Hold %s - After update, hold: %s\nSystem: %s.',
                      self.duser.display_name, hold, system)

        response = hold.system.display()
        if hold.system.is_undermined:
            response += '\n\nSystem is finished with held merits. Type `!um` for more targets.'

        return ([hold], response)

    @check_mentions
    async def execute(self):
        self.check_sheet()
        self.log.info('HOLD %s - Matched self.duser with id %s and sheet name %s.',
                      self.duser.display_name, self.duser.id[:6], self.duser.undermine)

        if self.args.died:
            holds = cogdb.query.um_reset_held(self.session, self.duser.undermine)
            self.log.info('HOLD %s - User reset merits.', self.duser.display_name)
            response = 'Sorry you died :(. Held merits reset.'

        elif self.args.redeem:
            holds, redeemed = cogdb.query.um_redeem_merits(self.session, self.duser.undermine)
            self.log.info('HOLD %s - Redeemed %d merits.', self.duser.display_name, redeemed)
            response = 'You redeemed {} new merits.\n{}'.format(redeemed,
                                                                self.duser.undermine.merits)

        else:  # Default case, update the hold for a system
            holds, response = self.set_hold()

        self.session.commit()
        for hold in holds:
            asyncio.ensure_future(self.bot.scanner_um.update_hold(hold))
        await self.bot.send_message(self.mchannel, response)


class Um(Action):
    """
    Command to show um systems and update status.
    """
    async def execute(self):
        # Sanity check
        if (self.args.set or self.args.offset) and not self.args.system:
            response = 'Forgot to specify system to update'

        elif self.args.system:
            system = cogdb.query.um_find_system(self.session, ' '.join(self.args.system))

            if self.args.offset:
                system.map_offset = self.args.offset
            if self.args.set:
                system.set_status(self.args.set)
            if self.args.set or self.args.offset:
                self.session.commit()
                asyncio.ensure_future(self.bot.scanner_um.update_system(system))

            response = system.display()

        else:
            systems = cogdb.query.um_get_systems(self.session)
            response = '__Current UM Targets__\n\n' + '\n'.join(
                [system.display() for system in systems])

        await self.bot.send_message(self.mchannel, response)
