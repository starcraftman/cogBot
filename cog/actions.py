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
from functools import partial

import decorator
import discord

import cogdb
import cogdb.query
import cogdb.side
import cog.jobs
import cog.tbl
import cog.util


HOME = "Gears' Hideout"
FUC = "Federal United Command"
SHEET_ACTS = ['Drop', 'Hold', 'Fort', 'UM', 'User']
SCANNERS = {}


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

    if self.msg.mentions:
        if len(self.msg.mentions) != 1:
            raise cog.exc.InvalidCommandArgs('Mention only 1 member per command.')

        self.log.info('DROP %s - Substituting author -> %s.',
                      self.msg.author, self.msg.mentions[0])
        self.msg.author = self.msg.mentions[0]
        self.msg.mentions = []
        asyncio.ensure_future(self.bot.on_message(self.msg))

    else:
        await coro(*args, **kwargs)


def check_sheet(scanner_name, stype):
    """ Check if user present in sheet. """
    @decorator.decorator
    async def inner(coro, *args, **kwargs):
        """ The actual decorator. """
        self = args[0]
        sheet = getattr(self, stype)
        if not sheet:
            self.log.info('DROP %s - Adding to %s as %s.',
                          self.duser.display_name, stype, self.duser.pref_name)
            sheet = cogdb.query.add_sheet(self.session, self.duser.pref_name,
                                          cry=self.duser.pref_cry,
                                          type=getattr(cogdb.schema.ESheetType, stype),
                                          start_row=get_scanner(scanner_name).user_row)

            sync_func = partial(get_scanner(scanner_name).update_sheet_user,
                                sheet.row, sheet.cry, sheet.name)
            cog.jobs.QUE.put_nowait(cog.jobs.Job(sync_func, self.msg))

            notice = 'Automatically added {} to {} sheet. See !user command to change.'.format(
                self.duser.pref_name, stype)
            asyncio.ensure_future(self.bot.send_message(self.msg.channel, notice))

        await coro(*args, **kwargs)

    return inner


class Action(object):
    """
    Top level action, contains shared logic.
    """
    def __init__(self, **kwargs):
        self.args = kwargs['args']
        self.bot = kwargs['bot']
        self.msg = kwargs['msg']
        self.log = logging.getLogger('cog.actions')
        self.session = cogdb.Session()
        self.__duser = None

    @property
    def duser(self):
        """ DUser associated with message author. """
        if not self.__duser:
            self.__duser = cogdb.query.ensure_duser(self.session, self.msg.author)
            self.log.info('DUSER - ' + str(self.__duser))

        return self.__duser

    @property
    def cattle(self):
        """ User's current cattle sheet. """
        return self.duser.cattle(self.session)

    @property
    def undermine(self):
        """ User's current undermining sheet. """
        return self.duser.undermine(self.session)

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


class Admin(Action):
    """
    Admin command console. For knowledgeable users only.
    """
    async def execute(self):
        args = self.args
        response = ''
        admin = cogdb.query.get_admin(self.session, self.duser)

        if args.subcmd == "add":
            for member in self.msg.mentions:
                admin.add(self.session, member)
            response = "Admins added."

        elif args.subcmd == "remove":
            for member in self.msg.mentions:
                admin.remove(self.session, cogdb.query.get_admin(self.session, member))
            response = "Admins removed."

        elif args.subcmd == 'cast':
            asyncio.ensure_future(self.bot.broadcast(' '.join(self.args.content)))
            response = 'Broadcast scheduled.'

        elif args.subcmd == 'deny':
            self.bot.deny_commands = not self.bot.deny_commands
            response = 'Commands: **{}abled**'.format('Dis' if self.bot.deny_commands else 'En')

        elif args.subcmd == 'dump':
            cogdb.query.dump_db()
            response = 'Db has been dumped to server console.'

        elif args.subcmd == 'halt':
            self.bot.deny_commands = True
            asyncio.ensure_future(self.bot.send_message(self.msg.channel,
                                                        'Shutdown in 40s. Commands: **Disabled**'))
            await asyncio.sleep(40)
            asyncio.ensure_future(bot_shutdown(self.bot))
            response = 'Goodbye!'

        elif args.subcmd == 'scan':
            self.bot.deny_commands = True
            asyncio.ensure_future(self.bot.send_message(self.msg.channel,
                                                        'Updating db. Commands: **Disabled**'))

            await asyncio.sleep(2)

            update_db(self.bot, self.msg)

        elif args.subcmd == 'info':
            if self.msg.mentions:
                response = ''
                for user in self.msg.mentions:
                    response += user_info(user) + '\n'
            else:
                response = user_info(self.msg.author)
            self.msg.channel = self.msg.author  # Not for public

        if response:
            await self.bot.send_message(self.msg.channel, response)


class BGS(Action):
    """
    Provide bgs related commands.
    """
    async def execute(self):
        system = cogdb.query.fort_find_system(self.session, ' '.join(self.args.system),
                                              search_all=True)
        self.log.info('BGS - Looking for: %s', system.name)

        lines = [['Control', 'System', 'Age']]
        systems = cogdb.side.exploited_systems_by_age(cogdb.SideSession(), system.name)
        if isinstance(systems, type([])):
            lines += [[system.control, system.system, system.age] for system in systems]
            response = cog.tbl.wrap_markdown(cog.tbl.format_table(lines, header=True))
        else:
            response = systems
        await self.bot.send_message(self.msg.channel, response)


class Drop(Action):
    """
    Handle the logic of dropping a fort at a target.
    """
    @check_mentions
    @check_sheet('hudson_cattle', 'cattle')
    async def execute(self):
        """
        Drop forts at the fortification target.
        """
        # self.check_sheet()
        self.log.info('DROP %s - Matched duser with id %s and sheet name %s.',
                      self.duser.display_name, self.duser.id[:6], self.cattle)

        system = cogdb.query.fort_find_system(self.session, ' '.join(self.args.system),
                                              search_all=True)
        self.log.info('DROP %s - Matched system %s from: \n%s.',
                      self.duser.display_name, system.name, system)

        drop = cogdb.query.fort_add_drop(self.session, system=system,
                                         user=self.cattle, amount=self.args.amount)
        self.log.info('DROP %s - After drop, Drop: %s\nSystem: %s.',
                      self.duser.display_name, drop, system)

        if self.args.set:
            system.set_status(self.args.set)
        self.session.commit()

        sync_func = partial(sync_drop, [drop.system.sheet_col, drop.user.row, drop.amount],
                            [drop.system.sheet_col, drop.system.fort_status, drop.system.um_status])
        cog.jobs.QUE.put_nowait(cog.jobs.Job(sync_func, self.msg))

        self.log.info('DROP %s - Sucessfully dropped %d at %s.',
                      self.duser.display_name, self.args.amount, system.name)

        response = drop.system.display()
        if drop.system.is_fortified:
            try:
                new_target = cogdb.query.fort_get_targets(self.session)[0]
                response += '\n\n__Next Fort Target__:\n' + new_target.display()
            except cog.exc.NoMoreTargets:
                response += '\n\n Could not determine next fort target.'
        await self.bot.send_message(self.msg.channel,
                                    self.bot.emoji.fix(response, self.msg.server))


class Fort(Action):
    """
    Provide information on and manage the fort sheet.
    """
    def find_missing(self, left):
        """ Show systems with 'left' remaining. """
        lines = ['__Systems Missing {} Supplies__'.format(left)]

        for system in cogdb.query.fort_get_systems(self.session):
            if not system.is_fortified and not system.skip and system.missing <= left:
                lines.append(system.display(miss=True))

        return '\n'.join(lines)

    def system_summary(self):
        """ Provide a quick summary of systems. """
        states = cogdb.query.fort_get_systems_by_state(self.session)

        total = len(cogdb.query.fort_get_systems(self.session))
        keys = ['cancelled', 'fortified', 'undermined', 'skipped', 'left']
        lines = [
            [key.capitalize() for key in keys],
            ['{}/{}'.format(len(states[key]), total) for key in keys],
        ]

        return cog.tbl.wrap_markdown(cog.tbl.format_table(lines, sep='|', header=True))

    async def execute(self):
        if self.args.summary:
            response = self.system_summary()

        elif self.args.set:
            system_name = ' '.join(self.args.system)
            if ',' in system_name:
                raise cog.exc.InvalidCommandArgs('One system at a time with --set flag')

            system = cogdb.query.fort_find_system(self.session, system_name, search_all=True)
            system.set_status(self.args.set)
            self.session.commit()


            sync_func = partial(get_scanner("hudson_cattle").update_system,
                                system.sheet_col, system.fort_status, system.um_status)
            cog.jobs.QUE.put_nowait(cog.jobs.Job(sync_func, self.msg))
            response = system.display()

        elif self.args.miss:
            response = self.find_missing(self.args.miss)

        elif self.args.system:
            lines = ['__Search Results__']
            system_names = ' '.join(self.args.system).split(',')
            for name in system_names:
                lines.append(cogdb.query.fort_find_system(self.session,
                                                          name.strip(), search_all=True).display())
            response = '\n'.join(lines)

        elif self.args.next:
            lines = ['__Next Targets__']
            lines += [system.display() for system in
                      cogdb.query.fort_get_next_targets(self.session, count=self.args.next)]
            response = '\n'.join(lines)

        else:
            lines = ['__Active Targets__']
            lines += [system.display() for system in cogdb.query.fort_get_targets(self.session)]

            lines += ['\n__Next Targets__']
            next_count = self.args.next if self.args.next else 3
            lines += [system.display() for system in
                      cogdb.query.fort_get_next_targets(self.session, count=next_count)]

            defers = cogdb.query.fort_get_deferred_targets(self.session)
            if defers:
                lines += ['\n__Almost Done__'] + [system.display() for system in defers]
            response = '\n'.join(lines)

        await self.bot.send_message(self.msg.channel,
                                    self.bot.emoji.fix(response, self.msg.server))


class Feedback(Action):
    """
    Send bug reports to Gears' Hideout reporting channel.
    """
    async def execute(self):
        lines = [
            ['Server', self.msg.server.name],
            ['Channel', self.msg.channel.name],
            ['Author', self.msg.author.name],
            ['Date (UTC)', datetime.datetime.utcnow()],
        ]
        response = cog.tbl.wrap_markdown(cog.tbl.format_table(lines)) + '\n\n'
        response += '__Bug Report Follows__\n\n' + ' '.join(self.args.content)

        self.log.info('FEEDBACK %s - Left a bug report.', self.msg.author.name)
        await self.bot.send_message(self.get_channel(HOME, 'feedback'), response)


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
        await self.bot.send_ttl_message(self.msg.channel, response)
        asyncio.ensure_future(self.bot.delete_message(self.msg))


class Hold(Action):
    """
    Update a user's held merits.
    """
    def set_hold(self):
        """ Set the hold on a system. """
        if not self.args.system:
            raise cog.exc.InvalidCommandArgs("You forgot to specify a system to update.")

        system = cogdb.query.um_find_system(self.session, ' '.join(self.args.system))
        self.log.info('HOLD %s - Matched system name %s: \n%s.',
                      self.duser.display_name, self.args.system, system)
        hold = cogdb.query.um_add_hold(self.session, system=system,
                                       user=self.undermine, held=self.args.amount)

        if self.args.set:
            system.set_status(self.args.set)
            sync_func = partial(get_scanner("hudson_undermine").update_system,
                                system.sheet_col, system.progress_us, system.progress_them, system.map_offset)
            cog.jobs.QUE.put_nowait(cog.jobs.Job(sync_func, self.msg))

        self.log.info('Hold %s - After update, hold: %s\nSystem: %s.',
                      self.duser.display_name, hold, system)

        response = hold.system.display()
        if hold.system.is_undermined:
            response += '\n\nSystem is finished with held merits. Type `!um` for more targets.'

        return ([hold], response)

    @check_mentions
    @check_sheet('hudson_undermine', 'undermine')
    async def execute(self):
        # self.check_sheet()
        self.log.info('HOLD %s - Matched self.duser with id %s and sheet name %s.',
                      self.duser.display_name, self.duser.id[:6], self.undermine)

        if self.args.died:
            holds = cogdb.query.um_reset_held(self.session, self.undermine)
            self.log.info('HOLD %s - User reset merits.', self.duser.display_name)
            response = 'Sorry you died :(. Held merits reset.'

        elif self.args.redeem:
            holds, redeemed = cogdb.query.um_redeem_merits(self.session, self.undermine)
            self.log.info('HOLD %s - Redeemed %d merits.', self.duser.display_name, redeemed)
            response = 'You redeemed {} new merits.\n{}'.format(redeemed,
                                                                self.undermine.merit_summary())

        else:  # Default case, update the hold for a system
            holds, response = self.set_hold()

        self.session.commit()
        holds = [[hold.system.sheet_col, hold.user.row, hold.held, hold.redeemed] for hold in holds]
        cog.jobs.QUE.put_nowait(cog.jobs.Job(partial(sync_holds, holds), self.msg))

        await self.bot.send_message(self.msg.channel, response)


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

        await self.bot.send_message(self.msg.channel,
                                    cog.tbl.wrap_markdown(cog.tbl.format_table(lines)))


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
        today = now.replace(hour=0, minute=0, second=0)  # pylint: disable=unexpected-keyword-arg

        weekly_tick = today + datetime.timedelta(hours=7)
        while weekly_tick < now or weekly_tick.strftime('%A') != 'Thursday':
            weekly_tick += datetime.timedelta(days=1)

        lines = [
            'Game Time: **{}**'.format(now.strftime('%H:%M:%S')),
            cogdb.side.next_bgs_tick(cogdb.SideSession(), now),
            'Cycle Ends in **{}**'.format(weekly_tick - now),
            'All Times UTC',
        ]

        await self.bot.send_message(self.msg.channel, '\n'.join(lines))


class UM(Action):
    """
    Command to show um systems and update status.
    """
    async def execute(self):
        # Sanity check
        if (self.args.set or self.args.offset) and not self.args.system:
            raise cog.exc.InvalidCommandArgs("You forgot to specify a system to update.")

        elif self.args.system:
            system = cogdb.query.um_find_system(self.session, ' '.join(self.args.system))

            if self.args.offset:
                system.map_offset = self.args.offset
            if self.args.set:
                system.set_status(self.args.set)
            if self.args.set or self.args.offset:
                self.session.commit()
                sync_func = partial(get_scanner("hudson_undermine").update_system,
                                    system.sheet_col, system.progress_us, system.progress_them, system.map_offset)
                cog.jobs.QUE.put_nowait(cog.jobs.Job(sync_func, self.msg))

            response = system.display()

        else:
            systems = cogdb.query.um_get_systems(self.session)
            response = '__Current UM Targets__\n\n' + '\n'.join(
                [system.display() for system in systems])

        await self.bot.send_message(self.msg.channel, response)


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
            self.log.info('USER %s - Duser.faction -> hudson.', self.msg.author.display_name)
            asyncio.ensure_future(
                self.bot.send_message(self.msg.channel, "Not available at this time."))
            return

        if args.winters:
            self.log.info('USER %s - Duser.faction -> winters.', self.msg.author.display_name)
            asyncio.ensure_future(
                self.bot.send_message(self.msg.channel, "Not available at this time."))
            return

        self.session.commit()
        if args.name or args.cry:
            if self.cattle:
                sheet = self.cattle
                sync_func = partial(get_scanner("hudson_cattle").update_sheet_user,
                                    sheet.row, sheet.cry, sheet.name)
                cog.jobs.QUE.put_nowait(cog.jobs.Job(sync_func, self.msg))
            if self.undermine:
                sheet = self.undermine
                sync_func = partial(get_scanner("hudson_undermine").update_sheet_user,
                                    sheet.row, sheet.cry, sheet.name)
                cog.jobs.QUE.put_nowait(cog.jobs.Job(sync_func, self.msg))

        lines = [
            '__{}__'.format(self.msg.author.display_name),
            'Sheet Name: ' + self.duser.pref_name,
            'Default Cry:{}'.format(' ' + self.duser.pref_cry if self.duser.pref_cry else ''),
            '',
        ]
        if self.cattle:
            lines += [
                '__{} {}__'.format(self.cattle.faction.capitalize(),
                                   self.cattle.type.replace('Sheet', '')),
                '    Cry: {}'.format(self.cattle.cry),
                '    Total: {}'.format(self.cattle.merit_summary()),
            ]
            mlines = [['System', 'Amount']]
            mlines += [[merit.system.name, merit.amount] for merit in self.cattle.merits
                       if merit.amount > 0]
            lines += cog.tbl.wrap_markdown(cog.tbl.format_table(mlines, header=True)).split('\n')
        if self.undermine:
            lines += [
                '__{} {}__'.format(self.undermine.faction.capitalize(),
                                   self.undermine.type.replace('Sheet', '')),
                '    Cry: {}'.format(self.undermine.cry),
                '    Total: {}'.format(self.undermine.merit_summary()),
            ]
            mlines = [['System', 'Hold', 'Redeemed']]
            mlines += [[merit.system.name, merit.held, merit.redeemed] for merit
                       in self.undermine.merits if merit.held + merit.redeemed > 0]
            lines += cog.tbl.wrap_markdown(cog.tbl.format_table(mlines, header=True)).split('\n')

        await self.bot.send_message(self.msg.channel, '\n'.join(lines))

    def update_name(self):
        """ Update the user's cmdr name in the sheets. """
        new_name = ' '.join(self.args.name)
        self.log.info('USER %s - DUser.pref_name from %s -> %s',
                      self.duser.display_name, self.duser.pref_name, new_name)
        cogdb.query.check_pref_name(self.session, self.duser, new_name)

        for sheet in self.duser.sheets(self.session):
            sheet.name = new_name
        self.duser.pref_name = new_name

    def update_cry(self):
        """ Update the user's cry in the sheets. """
        new_cry = ' '.join(self.args.cry)
        self.log.info('USER %s - DUser.pref_cry from %s -> %s',
                      self.duser.display_name, self.duser.pref_cry, new_cry)

        for sheet in self.duser.sheets(self.session):
            sheet.cry = new_cry
        self.duser.pref_cry = new_cry


def update_db(bot, msg):
    """ Simple hook to use from bot. """
    bot.deny_commands = True
    job = cog.jobs.Job(scan_all_sheets, msg, attempts=5, timeout=12, step=3)
    job.register(partial(scan_all_sheets_cb, bot, msg))
    cog.jobs.QUE.put_nowait(job)


def scan_all_sheets():
    """ Executes in another process. """
    for scanner in SCANNERS.values():
        scanner.scan()


def scan_all_sheets_cb(bot, msg):
    """ When scanning suceeds, enable commands. """
    # Commands only accepted if no critical errors during update
    bot.deny_commands = False
    asyncio.ensure_future(
        bot.send_message(msg.channel, 'Update finished. Commands: **Enabled**'))


def sync_drop(drop_args, system_args):
    """ Executes in another process. """
    scanner = get_scanner("hudson_cattle")
    scanner.update_drop(*drop_args)
    scanner.update_system(*system_args)


def sync_holds(holds):
    """ Executes in another process. """
    # TODO: Expand the holds to a continuous rectangle and one update.
    scanner = get_scanner("hudson_undermine")
    for hold in holds:
        scanner.update_hold(*hold)


def init_scanner(name):
    """
    Initialize a scanner based on configuration.
    """
    print("Intializing scanner -> ", name)
    logging.getLogger('cog.actions').info("Initializing the %s scanner.", name)
    sheet = cog.util.get_config("scanners", name)
    cls = getattr(cogdb.query, sheet.pop("cls"))
    scanner = cls(sheet)
    scanner.scan()
    SCANNERS[name] = scanner


def get_scanner(name):
    """
    Store scanners in this module for shared use.
    """
    return SCANNERS[name]
